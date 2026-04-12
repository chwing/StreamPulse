param(
    [int]$DurationSeconds = 300,
    [int]$EventsPerSecond = 10000
)

Write-Host "StreamPulse Stress Test" -ForegroundColor Cyan
Write-Host "=======================" -ForegroundColor Cyan
Write-Host "Duration:   $DurationSeconds seconds"
Write-Host "Throughput: $EventsPerSecond events/sec"
Write-Host ""

$VenvPython = "$PSScriptRoot\..\producer\.venv\Scripts\python.exe"
$ProducerScript = "$PSScriptRoot\..\producer\main.py"

if (-not (Test-Path $VenvPython)) {
    Write-Host "ERROR: Virtual environment not found at $VenvPython" -ForegroundColor Red
    Write-Host "Run this first inside the producer/ folder:" -ForegroundColor Yellow
    Write-Host "  python -m venv .venv" -ForegroundColor Yellow
    Write-Host "  .venv\Scripts\activate" -ForegroundColor Yellow
    Write-Host "  pip install -r requirements.txt" -ForegroundColor Yellow
    exit 1
}

$env:EVENTS_PER_SECOND = $EventsPerSecond.ToString()

Write-Host "Using Python: $VenvPython" -ForegroundColor Green
Write-Host "Starting high-throughput producer..." -ForegroundColor Yellow

$producer = Start-Process `
    -FilePath $VenvPython `
    -ArgumentList $ProducerScript `
    -WorkingDirectory "$PSScriptRoot\..\producer" `
    -PassThru -NoNewWindow

Write-Host "Producer PID: $($producer.Id)"
Write-Host "Running for $DurationSeconds seconds."
Write-Host "Watch lag at http://localhost:3000"
Write-Host ""

$start = Get-Date

while ((Get-Date) - $start -lt [TimeSpan]::FromSeconds($DurationSeconds)) {
    Start-Sleep -Seconds 10

    $elapsed = [int]((Get-Date) - $start).TotalSeconds

    Write-Host "[$elapsed s] Consumer lag snapshot:" -ForegroundColor Cyan

    $lagOutput = docker exec streampulse-kafka kafka-consumer-groups `
        --bootstrap-server localhost:9092 `
        --describe `
        --group streampulse-spark 2>$null

    if ($lagOutput) {
        Write-Host $lagOutput
    } else {
        Write-Host "  No lag data yet. Spark consumer may still be starting." -ForegroundColor Yellow
    }

    Write-Host ""
}

if (-not $producer.HasExited) {
    Stop-Process -Id $producer.Id -Force
    Write-Host "Producer stopped." -ForegroundColor Green
}

Write-Host ""
Write-Host "Stress test complete." -ForegroundColor Green
Write-Host "Full lag timeline: http://localhost:3000"
Write-Host ""
Write-Host "Kafka topic offsets:" -ForegroundColor Cyan

docker exec streampulse-kafka kafka-run-class kafka.tools.GetOffsetShell `
    --broker-list localhost:9092 `
    --topic clickstream --time -1

$env:EVENTS_PER_SECOND = "100"
Write-Host "EVENTS_PER_SECOND reset to 100." -ForegroundColor Green