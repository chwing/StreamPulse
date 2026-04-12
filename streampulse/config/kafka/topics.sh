#!/bin/bash

KAFKA_CONTAINER="streampulse-kafka"

echo "Creating StreamPulse Kafka topics..."

# clickstream — user behavior events (page views, clicks, add-to-cart)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic clickstream \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

# orders — purchase completion events
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

# sessions — session start/end lifecycle events
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic sessions \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --if-not-exists

# dead-letter queue — for malformed or failed events (Phase 6)
docker exec $KAFKA_CONTAINER \
  kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic dlq \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Topics created. Listing all topics:"
docker exec $KAFKA_CONTAINER \
  kafka-topics --list --bootstrap-server localhost:9092