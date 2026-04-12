# Copilot Instructions for StreamPulse

## Build, run, and verification commands

Run these from `streampulse\`:

```powershell
# Start platform services (Kafka, Schema Registry, MinIO, Spark, Trino, Grafana, Prometheus)
docker compose up -d --build

# Create Kafka topics (auto-create is disabled in docker-compose)
.\config\kafka\topics.ps1

# Run producer locally
cd producer
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Validation commands that exist in this repo:

```powershell
# Full stack smoke check
.\scripts\health_check.ps1

# Stress run (high throughput producer + lag snapshots)
.\scripts\stress_test.ps1 -DurationSeconds 300 -EventsPerSecond 10000
```

There is currently **no configured unit-test framework** (no repo `tests/`, `pytest.ini`, or test runner command) and **no configured linter command**. Do not invent `pytest`/`ruff`/`flake8` steps unless those tools are added to the repo.

## High-level architecture

StreamPulse is a medallion streaming pipeline:

1. `producer\` generates clickstream/order/session events, serializes them with Confluent Avro wire format, and publishes to Kafka topics `clickstream`, `orders`, `sessions` (with `dlq` for failures).
2. `spark\jobs\bronze_writer.py` consumes all Kafka topics and writes one Bronze Delta table (`s3a://streampulse-bronze/events`) with raw `event_json` plus Kafka metadata.
3. `spark\jobs\silver_transformer.py` reads Bronze as a stream, parses typed records per topic, and upserts to Silver Delta tables:
   - `s3a://streampulse-silver/events/clickstream`
   - `s3a://streampulse-silver/events/orders`
   - `s3a://streampulse-silver/events/sessions`
4. `spark\jobs\gold_aggregator.py` reads Silver clickstream/orders streams and appends Gold aggregates:
   - `revenue_by_5min`
   - `top_products`
   - `conversion_funnel`
   - `active_users`
5. Trino (`trino\catalog\delta.properties`) exposes Gold Delta tables as catalog `delta.default.*`.
6. Grafana (`grafana\provisioning\`) is pre-provisioned to query Trino (`delta.default.*`) and Prometheus.

## Key repository-specific conventions

- **Schema-first event contract:** producer startup calls `AvroSerializer.register_all_schemas()`; Spark decodes via schema ID from Schema Registry (`utils\schema_fetcher.py`).
- **Confluent wire format is mandatory:** `[magic byte 0][4-byte schema id][avro payload]` in both producer serializer and Spark deserializer paths.
- **Topic names are centralized and fixed** in `producer\config.py` (`clickstream`, `orders`, `sessions`, `dlq`) and reused across components.
- **Kafka topic auto-creation is intentionally off** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`), so topic scripts in `config\kafka\` are part of normal setup.
- **Layer responsibilities are strict:**
  - Bronze = append-only raw decoded JSON + Kafka metadata.
  - Silver = typed, deduplicated rows via Delta merge (`upsert_to_delta`) keyed by `event_id` / `order_id` / `session_id`.
  - Gold = append-only windowed aggregates for dashboard reads.
- **State/checkpoints live in MinIO S3A**, not local disk (`CHECKPOINT_*` in `spark\config.py`), so streaming restarts continue from persisted offsets.
- **Streaming query boot order matters:** `spark\main.py` starts Bronze, waits, then Silver, waits, then Gold to avoid downstream startup races.
- **Windows-first operational scripts:** setup/ops scripts are primarily PowerShell (`topics.ps1`, `health_check.ps1`, `stress_test.ps1`); keep command examples PowerShell-friendly.
