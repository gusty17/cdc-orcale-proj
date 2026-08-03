<#
    setup.ps1 - bring the CDC lab up and apply the Oracle prerequisites.

    Usage:
        .\setup.ps1            # start containers, then run step 1 SQL
        .\setup.ps1 -SqlOnly   # containers already running, just (re)apply SQL
        .\setup.ps1 -Down      # tear everything down, volumes included

    Both SQL scripts are idempotent, so -SqlOnly can be re-run at will.
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
$ProjectDir = $PSScriptRoot
$Compose    = Join-Path $ProjectDir 'docker-compose.yml'
$OraclePass = 'oracle'

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
    Write-Step 'Starting oracle, kafka, connect'
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

if (-not $plugins) {
    Write-Warning '  Connect REST API never came up - check: docker logs cdc-connect'
} else {
    $oracle = $plugins | Where-Object { $_.class -like '*OracleConnector' }
    if ($oracle) {
        Write-Host "  Oracle connector plugin available: $($oracle.class) v$($oracle.version)" -ForegroundColor Green
    } else {
        Write-Warning '  Connect is up but no Oracle connector plugin was found.'
    }
}

Write-Host ''
Write-Host 'Oracle is ready for CDC. Next: define the connector in oracle-connector.json' -ForegroundColor Green
Write-Host 'and POST it to http://localhost:8083/connectors.'
