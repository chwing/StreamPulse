# StreamPulse Health Check

Write-Host "StreamPulse Health Check" -ForegroundColor Cyan
Write-Host "========================" -ForegroundColor Cyan

$allHealthy = $true

function Check($name, $url) {
    try {
        $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
        if ($r.StatusCode -eq 200) {
            Write-Host "  [OK]  $name" -ForegroundColor Green
        } else {
            Write-Host "  [FAIL] $name - HTTP $($r.StatusCode)" -ForegroundColor Red
            $script:allHealthy = $false
        }
    } catch {
        Write-Host "  [FAIL] $name - unreachable" -ForegroundColor Red
        $script:allHealthy = $false
    }
}

Write-Host ""
Write-Host "Checking services..." -ForegroundColor Yellow
Check "Kafka UI"        "http://localhost:8080/api/clusters"
Check "Schema Registry" "http://localhost:8081/subjects"
Check "MinIO"           "http://localhost:9000/minio/health/live"
Check "Spark Master"    "http://localhost:8090"
Check "Trino"           "http://localhost:8085/v1/info"
Check "Grafana"         "http://localhost:3000/api/health"
Check "Prometheus"      "http://localhost:9090/-/healthy"

Write-Host ""
Write-Host "Checking Kafka topics..." -ForegroundColor Yellow
$topics = docker exec streampulse-kafka kafka-topics --list --bootstrap-server localhost:9092 2>$null
foreach ($t in @("clickstream", "orders", "sessions", "dlq")) {
    if ($topics -match $t) {
        Write-Host "  [OK]  Topic: $t" -ForegroundColor Green
    } else {
        Write-Host "  [FAIL] Topic: $t not found" -ForegroundColor Red
        $allHealthy = $false
    }
}

Write-Host ""
Write-Host "Checking MinIO buckets..." -ForegroundColor Yellow
foreach ($b in @("streampulse-bronze", "streampulse-silver", "streampulse-gold")) {
    $result = docker exec streampulse-minio mc ls local/$b 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK]  Bucket: $b" -ForegroundColor Green
    } else {
        Write-Host "  [FAIL] Bucket: $b not found" -ForegroundColor Red
        $allHealthy = $false
    }
}

Write-Host ""
Write-Host "Checking consumer lag..." -ForegroundColor Yellow
$groups = & docker @(
    "exec",
    "streampulse-kafka",
    "kafka-consumer-groups",
    "--bootstrap-server",
    "localhost:9092",
    "--list"
) 2>&1

if ($groups -match "^streampulse-spark$") {
    $lag = & docker @(
        "exec",
        "streampulse-kafka",
        "kafka-consumer-groups",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        "streampulse-spark"
    ) 2>&1

    Write-Host "  [OK]  Consumer group streampulse-spark active" -ForegroundColor Green
    Write-Host $lag
} else {
    Write-Host "  [WARN] Consumer group not yet active - no committed offsets yet" -ForegroundColor Yellow
}

Write-Host ""
if ($allHealthy) {
    Write-Host "All checks passed. StreamPulse is healthy." -ForegroundColor Green
} else {
    Write-Host "Some checks failed. See above for details." -ForegroundColor Red
    exit 1
}
