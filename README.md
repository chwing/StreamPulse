# StreamPulse — Real-Time E-Commerce Analytics Pipeline

A production-grade streaming data pipeline that processes thousands
of e-commerce events per second and delivers live analytics through
a Grafana dashboard. Built to demonstrate end-to-end modern data
engineering skills.

---

## Architecture

```
Python Producer (Faker)
    │ Avro / Schema Registry
    ▼
Apache Kafka (3 topics × 3 partitions)
    │ Spark Structured Streaming
    ▼
Delta Lake on MinIO
    Bronze (raw) → Silver (cleaned) → Gold (aggregated)
    │ Trino SQL
    ▼
Grafana Dashboard (live, 30s refresh)
    + Prometheus metrics + consumer lag alerting
```

## Stack

| Layer | Technology | Version |
|---|---|---|
| Event streaming | Apache Kafka | 7.5.0 |
| Schema enforcement | Confluent Schema Registry | 7.5.0 |
| Stream processing | Apache Spark Structured Streaming | 3.5.0 |
| Storage format | Delta Lake | 3.0.0 |
| Object storage | MinIO (S3-compatible) | latest |
| SQL engine | Trino | 435 |
| Visualization | Grafana | 10.2.0 |
| Monitoring | Prometheus + JMX Exporter | 2.48.0 |
| Serialization | Apache Avro | — |
| Containerization | Docker Compose | — |
| Language | Python | 3.11 |

## Key design decisions

**Why Avro over JSON?**
Avro provides binary serialization (60–70% smaller messages),
schema evolution support, and compile-time contract enforcement
via the Schema Registry. A malformed event is rejected before
it enters Kafka — not after it corrupts a downstream table.

**Why Delta Lake over plain Parquet?**
Delta provides ACID transactions for concurrent Spark writers,
time travel for debugging (query any historical table state),
and the merge() operation for deduplication in the Silver layer.
Plain Parquet has none of these guarantees.

**Why the medallion architecture?**
Bronze = audit log (replayable). Silver = clean, deduplicated,
typed data (reliable). Gold = pre-aggregated metrics (fast reads).
Each layer serves a different consumer with a different SLA.

**Why Trino in front of Delta Lake?**
Grafana needs a JDBC endpoint that speaks ANSI SQL. Trino provides
that without requiring a Spark cluster to be permanently running.
It reads Delta Parquet files directly from MinIO with partition
pruning for fast dashboard queries.

**Why `acks=all` + idempotent producer?**
`acks=all` guarantees no message is acknowledged until all in-sync
replicas have persisted it. Idempotence ensures retried messages
never produce duplicates. Together they give exactly-once producer
semantics at the cost of minimal latency — acceptable for analytics.

**Why foreachBatch instead of a native Delta sink?**
foreachBatch gives complete control over each micro-batch as a
static DataFrame, enabling Delta merge (for Silver deduplication),
multiple output paths per batch, and arbitrary Python logic.
Native streaming sinks lack these capabilities.

## Quick start

**Prerequisites:** Docker Desktop, Python 3.11+, 8GB RAM minimum.

```powershell
# 1. Clone and enter the project
git clone https://github.com/yourusername/streampulse.git
cd streampulse

# 2. Configure credentials
copy .env.example .env
# Edit .env if you want custom passwords (defaults work out of the box)

# 3. Start the full stack
docker compose up -d --build

# 4. Create Kafka topics (wait ~60s for services to be healthy first)
.\config\kafka\topics.ps1

# 5. Start the event producer
cd producer
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py

# 6. Open the dashboard
start http://localhost:3000   # Grafana — admin/streampulse
start http://localhost:8080   # Kafka UI
start http://localhost:9001   # MinIO console
start http://localhost:8090   # Spark Master UI
start http://localhost:9090   # Prometheus
```

## Verifying the pipeline

```powershell
# Full stack health check
.\scripts\health_check.ps1

# Check consumer lag
docker exec streampulse-kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe --group streampulse-spark

# Run the stress test (10k events/sec for 5 minutes)
.\scripts\stress_test.ps1 -DurationSeconds 300 -EventsPerSecond 10000
```

## Dashboard panels

| Panel | Source table | Refresh |
|---|---|---|
| Revenue per 5 min | gold.revenue_by_5min | 30s |
| Orders per 5 min | gold.revenue_by_5min | 30s |
| Average order value | gold.revenue_by_5min | 30s |
| Total revenue (1h) | gold.revenue_by_5min | 30s |
| Total orders (1h) | gold.revenue_by_5min | 30s |
| Conversion funnel | gold.conversion_funnel | 30s |
| Cart & conv rate | gold.conversion_funnel | 30s |
| Active unique users | gold.active_users | 30s |
| Top sessions by revenue | gold.top_products | 30s |
| Consumer lag | Prometheus | 15s |
| Messages in/sec | Prometheus | 15s |
| Bytes in/sec | Prometheus | 15s |
| Request latency | Prometheus | 15s |

## Project structure

```
streampulse/
├── producer/          ← Python Kafka producer (Faker + Avro)
│   └── schemas/       ← Avro schema definitions (.avsc)
├── spark/             ← PySpark Structured Streaming jobs
│   ├── jobs/          ← Bronze / Silver / Gold writers
│   └── utils/         ← SparkSession, schema fetcher, Delta helpers
├── trino/             ← Trino SQL engine config + Delta catalog
├── grafana/           ← Dashboard JSON + datasource provisioning
├── monitoring/        ← Prometheus scrape config + JMX Exporter rules
├── config/kafka/      ← Topic creation scripts (PowerShell)
├── scripts/           ← Health check + stress test (PowerShell)
└── docker-compose.yml
```

---

## From simulation to production — integrating real website data

StreamPulse is built on a **fully production-grade pipeline**. The Kafka topics, Spark Structured Streaming jobs, Delta Lake layers, and Grafana dashboard are identical to what you would deploy for a live e-commerce platform. The only component that changes when moving to production is the data source — replacing the `generator.py` simulation with real user events from your website.

Everything from Kafka onward requires **zero modification**.

---

### What is simulated vs what is real

| Component | In StreamPulse (simulation) | In production (real website) |
|---|---|---|
| `generator.py` | Faker library — invented users, random events | **Replaced** by JS snippet + server webhook |
| `producer.py` | Unchanged | Unchanged |
| Schema Registry | Unchanged | Unchanged |
| Kafka topics | Unchanged | Unchanged |
| Spark Streaming | Unchanged | Unchanged |
| Delta Lake | Unchanged | Unchanged |
| Grafana dashboard | Unchanged | Unchanged |

The simulation exists for one reason: to let you build, test, and demonstrate the entire pipeline without needing a live website with real traffic. Once you have real traffic, you swap out one file.

---

### The three integration points

Connecting a real website to StreamPulse requires adding three components. None of them touch the existing pipeline.

#### 1. A collector API

A lightweight FastAPI service that acts as the bridge between your website and Kafka. It receives HTTP POST requests from the browser and from your server, validates the event payload against the Avro schema, and publishes it to the correct Kafka topic.

```
streampulse/
└── collector/
    ├── Dockerfile
    ├── requirements.txt   ← fastapi, uvicorn, confluent-kafka, pydantic
    └── main.py            ← POST /events, POST /events/batch, GET /health
```

Add it to `docker-compose.yml` as a new service on port 8000, connected to `streampulse-net` so it can reach the Kafka broker at `kafka:29092`.

#### 2. A JavaScript snippet on your website

A small script added before the closing `</body>` tag on every page. It assigns a persistent `user_id` and `session_id` in `localStorage`, listens for click and visibility events, and sends them to your Collector API using `navigator.sendBeacon()` — a non-blocking browser API that does not slow down page loads.

```html
<script>
(function() {
  var SESSION_ID = localStorage.getItem('sp_session') || crypto.randomUUID();
  var USER_ID    = localStorage.getItem('sp_user')    || crypto.randomUUID();
  localStorage.setItem('sp_session', SESSION_ID);
  localStorage.setItem('sp_user',    USER_ID);

  var API = 'https://your-collector.yourdomain.com/events';

  function send(type, extra) {
    navigator.sendBeacon(API, JSON.stringify(Object.assign({
      event_id:        crypto.randomUUID(),
      user_id:         USER_ID,
      session_id:      SESSION_ID,
      event_type:      type,
      page_url:        window.location.href,
      referrer:        document.referrer || null,
      device_type:     /Mobi|Android/i.test(navigator.userAgent) ? 'mobile' : 'desktop',
      event_timestamp: Date.now()
    }, extra || {})));
  }

  // Automatic events
  send('page_view');
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'hidden') send('session_end');
  });

  // Add data-track="add_to_cart" data-product-id="xyz" to your buttons
  document.addEventListener('click', function(e) {
    var btn = e.target.closest('[data-track]');
    if (btn) send(btn.dataset.track, { product_id: btn.dataset.productId });
  });
})();
</script>
```

This works with **any website** — plain HTML, WordPress, Shopify, React, Vue, Next.js, or any other framework. No SDK, no npm package, no build step required.

To track specific elements, add `data-track` attributes to your HTML:

```html
<button data-track="add_to_cart" data-product-id="prod-123">Add to cart</button>
<button data-track="product_click" data-product-id="prod-123">View details</button>
```

#### 3. A server-side webhook for orders

Client-side tracking is sufficient for behavioural events (page views, clicks), but purchase events must come from the server — browsers can close mid-checkout, ad blockers can fire, and connections can drop. Your payment processor (Stripe, PayPal, or any other) can be configured to POST a webhook to your Collector API immediately after a successful payment.

```python
# In collector/main.py — add this endpoint
@app.post("/webhook/order-completed")
async def order_completed(request: Request):
    data = await request.json()

    event = {
        "order_id":        data["order_id"],
        "user_id":         data["customer_id"],
        "session_id":      data.get("session_id", str(uuid.uuid4())),
        "items":           data["line_items"],
        "total_amount":    data["amount_total"] / 100,  # Stripe sends cents
        "currency":        data["currency"].upper(),
        "payment_method":  data["payment_method_type"],
        "order_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }

    producer.produce(topic="orders", value=json.dumps(event).encode(),
                     key=data["customer_id"].encode())
    producer.flush()
    return {"status": "ok"}
```

Register the endpoint URL in your payment processor dashboard under Webhooks. No other changes needed.

---

### Full integration architecture

```
Your website (any stack)
│
├── Every page  →  JS snippet  →  POST /events
│                                      │
└── Checkout    →  Payment webhook  →  POST /webhook/order-completed
                                            │
                                   Collector API (FastAPI)
                                   validates · sanitizes · routes
                                            │
                              ┌─────────────┴──────────────┐
                         Kafka topic                   Kafka DLQ
                      clickstream / orders           (bad events)
                              │
                   ─────── unchanged ────────
                   Spark · Delta · Grafana
```

---

### Deployment requirements for a live website

To expose the Collector API to the internet you need:

**A VPS or cloud server** (DigitalOcean Droplet, Hetzner Cloud, AWS EC2) running your Docker Compose stack. A 4 GB RAM instance is sufficient for development-scale traffic (a few thousand users per day).

**A domain and HTTPS certificate.** `navigator.sendBeacon()` requires HTTPS — browsers silently ignore it on plain HTTP. Run Nginx as a reverse proxy in front of the Collector API and provision a free TLS certificate with Let's Encrypt / Certbot.

**CORS configured** to your website's domain. The Collector API's `allow_origins` must match your website's exact origin (`https://yoursite.com`). Without this, browsers block the POST requests.

For **high-traffic production** (tens of thousands of daily users), replace the single local Kafka broker with a managed service — Confluent Cloud or AWS MSK both use the same `confluent-kafka` Python client. The only change is the `bootstrap.servers` value in your `.env` file.

---

### What does not change at all

The following components require **zero code changes** when switching from simulated to real data:

- All three Kafka topics and their Avro schemas
- The Spark Structured Streaming jobs (`bronze_writer.py`, `silver_transformer.py`, `gold_aggregator.py`)
- The Bronze → Silver → Gold Delta Lake medallion architecture
- The Trino SQL engine and its catalog configuration
- The Grafana dashboard and all 13 panels
- The Prometheus monitoring and consumer lag alerting

This is the core value of the medallion architecture and the schema-first design: the pipeline is completely decoupled from its data source. It processes whatever arrives in the Kafka topics, regardless of whether that data came from a Faker loop or ten thousand real users browsing your store.

---

## What I learned building this

- Confluent Avro wire format and Schema Registry integration
- Spark Structured Streaming windowing, watermarking,
  and the foreachBatch pattern for Delta Lake writes
- Delta Lake ACID guarantees, time travel, and the merge()
  operation for exactly-once Silver layer semantics
- MinIO S3A configuration for local cloud-native storage
- Trino Delta Lake connector and partition-pruning queries
- Kafka JMX metrics and consumer group lag monitoring
- Docker Compose networking — dual Kafka listeners,
  service health dependencies, and container restart policies
- Designing a schema-first, source-agnostic pipeline
  that can serve both simulated and real production traffic

---