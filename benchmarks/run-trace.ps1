<#
.SYNOPSIS
    Runs the trace poller and a load generator together, in the right
    order, and reports on THIS RUN only.

.DESCRIPTION
    The poller must run BEFORE the first change lands, or timings come
    back NULL - this script waits for the poller to finish seeding
    before starting the load, so that ordering can't be gotten wrong.

    t24_cdc_trace accumulates across runs, so this scopes its report to
    just this run via the Kafka offset recorded before the load starts.

    Deliberately not a docker-compose service - polling RisingWave
    ~20x/sec permanently would load the very instance being measured.

.PARAMETER Load
    benchmark  - 20 inserts + 10 updates + 10 deletes (benchmarks/03-oracle-load.py)
    continuous - 1 row/sec until -Seconds elapses (seed/seed_xml.py)
    update     - one update, on the most recently inserted row (tests/test-update-cdc.py)
    delete     - one delete, on the most recently inserted row (tests/test-delete-cdc.py)
    none       - poller only; make changes yourself in another terminal

.PARAMETER Seconds
    How long to run the continuous loader. Ignored for other modes.

.PARAMETER All
    Report on everything in the trace, not just this run.

.EXAMPLE
    .\benchmarks\run-trace.ps1 -Load benchmark

.EXAMPLE
    .\benchmarks\run-trace.ps1 -Load continuous -Seconds 30
#>
param(
    [ValidateSet('benchmark', 'continuous', 'update', 'delete', 'none')]
    [string]$Load = 'benchmark',

    [int]$Seconds = 30,

    [switch]$All
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$psqlArgs = @('-h', 'cdc-risingwave', '-p', '4566', '-d', 'dev', '-U', 'root')

function Invoke-Psql([string]$sql) {
    # Errors swallowed on purpose - called in a retry loop while waiting
    # for RisingWave's barrier; a transient failure shouldn't spam the same error.
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'SilentlyContinue'
    $out = docker exec cdc-superset-db psql @psqlArgs -t -A -c $sql 2>$null
    $ErrorActionPreference = $prev
    return $out
}

# ---------------------------------------------------------------- poller
Write-Host "starting poller..." -ForegroundColor Cyan
$poller = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    Get-Content benchmarks\02-trace-poller.py | docker exec -i cdc-superset python -
} -ArgumentList $root

# Wait for the seed to finish first - otherwise the load's first rows
# get silently skipped as "already existing" by the poller's snapshot.
$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline) {
    if ((Receive-Job $poller -Keep) -match 'watching for new changes') { break }
    Start-Sleep -Milliseconds 250
}
Receive-Job $poller -Keep | Select-Object -Last 1 | Write-Host

# Everything this run produces lands at this offset or later - recorded
# after the poller is ready, before any change, so the window is exact.
$startOffset = 0
$offsets = docker exec cdc-kafka /opt/kafka/bin/kafka-get-offsets.sh `
    --bootstrap-server localhost:9092 --topic t24.T24.ACCOUNT
if ($offsets -match ':(\d+)$') { $startOffset = [int]$Matches[1] }
Write-Host "trace window: kafka offset >= $startOffset"

# ------------------------------------------------------------------ load
Write-Host "running load: $Load" -ForegroundColor Cyan
switch ($Load) {
    'benchmark' {
        # Runs directly on the host, not in a container - same as update/delete below.
        python benchmarks\03-oracle-load.py
    }
    'continuous' {
        # Started detached, stopped on a timer - the loader never exits on its own.
        # DELAY_SECONDS pinned to 1 - seed_xml.py's own default is 0.5s, which
        # would silently change this benchmark's pacing vs. what's documented above.
        $env:DELAY_SECONDS = '1'
        $loader = Start-Process -NoNewWindow -PassThru -FilePath 'python' `
                                -ArgumentList 'seed\seed_xml.py'
        Write-Host "  running for ${Seconds}s..."
        Start-Sleep -Seconds $Seconds
        Stop-Process -Id $loader.Id -Force
        Remove-Item Env:\DELAY_SECONDS -ErrorAction SilentlyContinue
        Write-Host "  loader stopped"
    }
    'update' {
        # Runs directly on the host, not in a container - test-update-cdc.py
        # connects to Oracle via localhost:1521, same as seed/*.py.
        python tests\test-update-cdc.py
    }
    'delete' {
        python tests\test-delete-cdc.py
    }
    'none' {
        Write-Host "  make your changes now - poller stops 20s after they end"
    }
}

# ---------------------------------------------------------------- settle
Write-Host "waiting for poller to settle (stops 20s after the last change)..." -ForegroundColor Cyan
Wait-Job $poller -Timeout 900 | Out-Null
Receive-Job $poller | Write-Host
Remove-Job $poller -Force

$scope = if ($All) { "1=1" } else { "kafka_offset >= $startOffset" }

# The poller's writes aren't visible to another session until the next
# barrier lands - querying right after it exits can show "(0 rows)" for
# a run that worked. FLUSH plus a confirm-loop covers that race.
Write-Host "waiting for results to become visible..." -ForegroundColor Cyan
docker exec cdc-superset-db psql @psqlArgs -c "FLUSH;" | Out-Null

$deadline = (Get-Date).AddSeconds(30)
$timed = 0
while ((Get-Date) -lt $deadline) {
    $timed = [int](Invoke-Psql "SELECT count(*) FROM t24_cdc_trace WHERE $scope AND total_ms IS NOT NULL;")
    if ($timed -gt 0) { break }
    Start-Sleep -Milliseconds 500
}

if ($timed -eq 0) {
    Write-Host "`nno timed changes in this run's window (offset >= $startOffset)." -ForegroundColor Yellow
    Write-Host "the load may have produced nothing, or the poller missed it. Try -All to see the whole trace."
    return
}

# ---------------------------------------------------------------- report
Write-Host "`n--- trace ($(if ($All) {'all runs'} else {'this run'})) ---" -ForegroundColor Cyan
docker exec cdc-superset-db psql @psqlArgs -c @"
SELECT op,
       count(*)                                     AS changes,
       round(avg(oracle_to_kafka_ms)::numeric, 0)   AS ora2kafka,
       round(avg(kafka_to_rw_ms)::numeric, 0)       AS kafka2rw,
       round(avg(rw_to_parsed_ms)::numeric, 0)      AS rw2parsed,
       round(avg(total_ms)::numeric, 0)             AS total_avg,
       min(total_ms)                                AS total_min,
       max(total_ms)                                AS total_max
  FROM t24_cdc_trace
 WHERE $scope AND total_ms IS NOT NULL
 GROUP BY op ORDER BY op;
"@

Write-Host "per-row detail:" -ForegroundColor DarkGray
Write-Host "  docker exec cdc-superset-db psql -h cdc-risingwave -p 4566 -d dev -U root -c ""SELECT * FROM t24_cdc_trace WHERE $scope ORDER BY kafka_offset;"""
