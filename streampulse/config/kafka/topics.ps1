$KAFKA_CONTAINER = "streampulse-kafka"

Write-Host "Creating StreamPulse Kafka topics..." -ForegroundColor Cyan

# clickstream
docker exec $KAFKA_CONTAINER `
  kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic clickstream `
  --partitions 3 `
  --replication-factor 1 `
  --config retention.ms=604800000 `
  --if-not-exists

# orders
docker exec $KAFKA_CONTAINER `
  kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic orders `
  --partitions 3 `
  --replication-factor 1 `
  --config retention.ms=604800000 `
  --if-not-exists

# sessions
docker exec $KAFKA_CONTAINER `
  kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic sessions `
  --partitions 3 `
  --replication-factor 1 `
  --config retention.ms=604800000 `
  --if-not-exists

# dead-letter queue
docker exec $KAFKA_CONTAINER `
  kafka-topics --create `
  --bootstrap-server localhost:9092 `
  --topic dlq `
  --partitions 1 `
  --replication-factor 1 `
  --if-not-exists

Write-Host "Topics created. Listing all topics:" -ForegroundColor Green
docker exec $KAFKA_CONTAINER `
  kafka-topics --list --bootstrap-server localhost:9092