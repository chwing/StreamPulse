"""
Dead-Letter Queue consumer.

Reads events that failed schema validation or processing from the
'dlq' Kafka topic and writes them to a log file for investigation.

In production this would:
  - Write to an alerting system (PagerDuty, Slack)
  - Store in a separate Delta table for manual review
  - Trigger a reprocessing job after the root cause is fixed

Run standalone:
  python dlq_consumer.py
"""

import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
from config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streampulse.dlq")


def main():
    consumer_conf = {
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id":          "streampulse-dlq-monitor",
        "auto.offset.reset": "earliest",
        # earliest: read all DLQ messages from the beginning
        # so no failed event is ever missed even if this
        # consumer was offline for a while.
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([config.TOPIC_DLQ])

    logger.info("DLQ consumer started — watching for failed events...")

    dlq_log_path = "dlq_events.jsonl"

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached the end of the partition — not an error,
                    # just means we've caught up. Keep polling.
                    continue
                raise KafkaException(msg.error())

            # Log the failed event to a JSONL file
            failed_event = {
                "received_at":     datetime.utcnow().isoformat(),
                "kafka_topic":     msg.topic(),
                "kafka_partition": msg.partition(),
                "kafka_offset":    msg.offset(),
                "key":             msg.key().decode("utf-8") if msg.key() else None,
                # Value may not be valid Avro (that's why it failed),
                # so store the raw bytes as a hex string for inspection.
                "value_hex":       msg.value().hex() if msg.value() else None,
            }

            logger.warning(
                f"DLQ event received | "
                f"partition={msg.partition()} offset={msg.offset()} "
                f"key={failed_event['key']}"
            )

            # Append to JSONL file — one JSON object per line
            with open(dlq_log_path, "a") as f:
                f.write(json.dumps(failed_event) + "\n")

    except KeyboardInterrupt:
        logger.info("DLQ consumer stopping...")
    finally:
        consumer.close()
        logger.info(f"DLQ events written to {dlq_log_path}")


if __name__ == "__main__":
    main()