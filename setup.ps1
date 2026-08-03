<#
    setup.ps1 - bring the whole CDC lab up: Oracle prerequisites, the
    Debezium connector, and the RisingWave tables/views.

    Usage:
        .\setup.ps1            # start containers, then apply everything below
        .\setup.ps1 -SqlOnly   # containers already running, just (re)apply config
        .\setup.ps1 -Down      # tear everything down, volumes included

    Everything this script applies is idempotent, so -SqlOnly (or a plain
    re-run) is always safe.

    NOT covered here - separate tools with their own setup:
        oracle-connector.json is applied automatically, but flatten/ (the
        Python XML-flattening consumer) is not - it has its own dependencies
        (pip install -r flatten/requirements.txt) and runs as a standalone,
        long-running process. See README.md Step 5.
#>
[CmdletBinding()]
param(
    [switch]$SqlOnly,
    [switch]$Down
)

# Deliberately NOT 'Stop': docker compose writes its progress to stderr, and
# under -ErrorActionPreference Stop Windows PowerShell 5.1 turns every native
# stderr line into a terminating NativeCommandError. Failures are detected via
# $LASTEXITCODE below instead.
$ErrorActionPreference = 'Continue'
$ProjectDir     = $PSScriptRoot
$Compose        = Join-Path $ProjectDir 'docker-compose.yml'
$OraclePass     = 'oracle'
$ConnectorName  = 't24-account-cdc'   # must match "name" in oracle-connector.json
$RisingWaveNet  = 'cdc-oracle_default'

function Write-Step($text) {
    Write-Host ''
    Write-Host "=== $text " -ForegroundColor Cyan
}

# Runs a script from ./ (mounted read-only at /scripts) through sqlplus as SYSDBA.
function Invoke-SqlFile {
    param(
        [Parameter(Mandatory)][string]$File,
        [Parameter(Mandatory)][string]$Service   # XE (CDB root) or XEPDB1 (PDB)
    )
    $conn = "sys/$OraclePass@//localhost:1521/$Service as sysdba"
    docker exec cdc-oracle sqlplus -S -L $conn "@/scripts/$File"
    if ($LASTEXITCODE -ne 0) {
        throw "sqlplus failed on $File (service $Service, exit $LASTEXITCODE)"
    }
}

if ($Down) {
    Write-Step 'Tearing down (containers + volumes)'
    docker compose -f $Compose down -v
    if ($LASTEXITCODE -ne 0) { throw 'docker compose down failed' }
    return
}

if (-not $SqlOnly) {
    Write-Step 'Starting oracle, kafka, connect, risingwave'
    docker compose -f $Compose up -d
    if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed' }

    # The health check can flip to "healthy" while the startdb.d hook is still
    # bouncing the database to turn on ARCHIVELOG, so wait for the entrypoint's
    # own banner instead - it is printed after all startup scripts have run.
    Write-Step 'Waiting for Oracle (first boot also enables ARCHIVELOG, which restarts the DB)'
    $ready    = $false
    $deadline = (Get-Date).AddMinutes(10)
    while ((Get-Date) -lt $deadline) {
        $logs  = docker logs cdc-oracle 2>&1 | Out-String
        $state = docker inspect -f '{{.State.Health.Status}}' cdc-oracle 2>$null
        if ($state -eq 'healthy' -and $logs -match 'DATABASE IS READY TO USE!') {
            $ready = $true
            break
        }
        Write-Host "  oracle: $state"
        Start-Sleep -Seconds 10
    }
    if (-not $ready) { throw "Oracle did not become ready in time (last health state: $state)" }
    Write-Host '  oracle: ready' -ForegroundColor Green
}

Write-Step 'Step 1a - instance prerequisites (CDB$ROOT)'
Invoke-SqlFile -File 'oracle-prereqs.sql' -Service 'XE'

Write-Step 'Step 1b - application schema + table supplemental logging (XEPDB1)'
Invoke-SqlFile -File 'oracle-setup.sql' -Service 'XEPDB1'

Write-Step 'Kafka Connect status'
# Connect needs ~30-60s to join its group and serve the REST API.
$plugins  = $null
$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    try {
        $plugins = Invoke-RestMethod 'http://localhost:8083/connector-plugins' -TimeoutSec 10
        break
    } catch {
        Write-Host '  waiting for Connect REST API on :8083 ...'
        Start-Sleep -Seconds 10
    }
}

$connectReady = $false
if (-not $plugins) {
    Write-Warning '  Connect REST API never came up - check: docker logs cdc-connect'
} else {
    $connectReady = $true
    $oracle = $plugins | Where-Object { $_.class -like '*OracleConnector' }
    if ($oracle) {
        Write-Host "  Oracle connector plugin available: $($oracle.class) v$($oracle.version)" -ForegroundColor Green
    } else {
        Write-Warning '  Connect is up but no Oracle connector plugin was found.'
    }
}

Write-Step 'Deploying the Debezium connector'
if (-not $connectReady) {
    Write-Warning '  Skipped - Connect REST API is not up.'
} else {
    # POST on an already-existing connector name returns 409 - check first so
    # this step is safe to re-run, same as everything else in this script.
    $exists = $false
    try {
        Invoke-RestMethod "http://localhost:8083/connectors/$ConnectorName" -TimeoutSec 10 | Out-Null
        $exists = $true
    } catch { }

    if ($exists) {
        Write-Host "  Connector '$ConnectorName' already exists, leaving it as-is." -ForegroundColor Green
    } else {
        $connectorJson = Get-Content (Join-Path $ProjectDir 'oracle-connector.json') -Raw
        try {
            Invoke-RestMethod -Method Post -Uri 'http://localhost:8083/connectors' `
                -ContentType 'application/json' -Body $connectorJson | Out-Null
            Write-Host "  Connector '$ConnectorName' created." -ForegroundColor Green
        } catch {
            Write-Warning "  Failed to create connector: $($_.Exception.Message)"
        }
    }

    $deadline = (Get-Date).AddMinutes(2)
    $state    = $null
    while ((Get-Date) -lt $deadline) {
        try {
            $state = (Invoke-RestMethod "http://localhost:8083/connectors/$ConnectorName/status" -TimeoutSec 10).connector.state
            if ($state -eq 'RUNNING') { break }
        } catch { }
        Start-Sleep -Seconds 5
    }
    if ($state -eq 'RUNNING') {
        Write-Host "  connector state: RUNNING" -ForegroundColor Green
    } else {
        Write-Warning "  connector state: $state - check: docker logs cdc-connect"
    }
}

Write-Step 'Waiting for the Kafka topic (RisingWave sources need it to already exist)'
# The connector reporting RUNNING only means the task started - it still has
# to run the initial snapshot before anything actually reaches Kafka.
# Confirmed by testing: without this wait, risingwave-setup.sql's CREATE
# TABLE ... WITH (connector='kafka', topic='t24.T24.ACCOUNT') fails with
# "topic t24.T24.ACCOUNT not found" on a fresh stack.
$topicReady = $false
$deadline   = (Get-Date).AddMinutes(2)
while ((Get-Date) -lt $deadline) {
    $topics = docker exec cdc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null
    if ($topics -contains 't24.T24.ACCOUNT') { $topicReady = $true; break }
    Write-Host '  waiting for topic t24.T24.ACCOUNT ...'
    Start-Sleep -Seconds 5
}
if ($topicReady) {
    Write-Host '  topic t24.T24.ACCOUNT exists.' -ForegroundColor Green
} else {
    Write-Warning '  Topic never appeared - RisingWave setup below will likely fail. Check: docker logs cdc-connect'
}

Write-Step 'Applying RisingWave tables/views'
# risingwave-setup.sql uses IF NOT EXISTS throughout, so piping it in on every
# run is safe. No local psql needed - a throwaway container on the compose
# network runs it instead (same pattern used everywhere in README.md).
#
# -v ON_ERROR_STOP=1 matters: psql's default is to print an error and keep
# going, which made an earlier version of this script report success even
# when every statement in the file had failed. This makes psql itself exit
# non-zero on the first real error, so $LASTEXITCODE is actually meaningful.
$rwSql = Get-Content (Join-Path $ProjectDir 'risingwave-setup.sql') -Raw
$rwSql | docker run --rm -i --network $RisingWaveNet postgres:16-alpine `
    psql -h risingwave -p 4566 -d dev -U root -v ON_ERROR_STOP=1
if ($LASTEXITCODE -ne 0) {
    Write-Warning '  RisingWave setup failed - check: docker logs cdc-risingwave'
} else {
    Write-Host '  t24_account, t24_account_events, t24_account_audit ready.' -ForegroundColor Green
}

Write-Host ''
Write-Host 'CDC pipeline is up: Oracle -> Debezium -> Kafka -> RisingWave.' -ForegroundColor Green
Write-Host 'Optional next step (separate tool, not automated here):'
Write-Host '  pip install -r flatten\requirements.txt'
Write-Host '  python flatten\generate_schema.py'
Write-Host '  python flatten\consume_to_risingwave.py --from-beginning'
