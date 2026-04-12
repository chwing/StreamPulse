import time
import logging
import signal
import sys
from config import config
from serializer import AvroSerializer
from producer import StreamPulseProducer
from generator import next_events

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("streampulse.main")


# ── Graceful shutdown flag ────────────────────────────────────────────────────
_running = True

def _handle_signal(signum, frame):
    global _running
    logger.info(f"Received signal {signum} — shutting down gracefully...")
    _running = False

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ── Main entry point ──────────────────────────────────────────────────────────
def main():
    logger.info("StreamPulse producer starting...")
    logger.info(
        f"Config: {config.EVENTS_PER_SECOND} events/sec | "
        f"{config.NUM_USERS} users | {config.NUM_PRODUCTS} products"
    )

    # Step 1: register Avro schemas with the Schema Registry
    serializer = AvroSerializer()
    serializer.register_all_schemas()

    # Step 2: build the Kafka producer
    producer = StreamPulseProducer(serializer)

    # Step 3: run the event loop
    event_count  = 0
    start_time   = time.time()
    stats_interval = 10   # print throughput stats every 10 seconds

    logger.info("Event loop started. Press Ctrl+C to stop.")

    while _running:
        loop_start = time.perf_counter()

        # Generate 1–3 events for this simulation step
        events = next_events()

        for topic, key, record in events:
            producer.send(topic, key, record)
            event_count += 1

        # Print stats every N seconds
        elapsed = time.time() - start_time
        if elapsed >= stats_interval and elapsed > 0:
            actual_rate = event_count / elapsed
            logger.info(
                f"Throughput: {actual_rate:.0f} events/sec | "
                f"Total: {event_count:,} | "
                f"Errors: {producer.error_count}"
            )
            # Reset counters for the next interval
            event_count = 0
            start_time  = time.time()

        # Pace the loop to match EVENTS_PER_SECOND
        elapsed_loop = time.perf_counter() - loop_start
        sleep_time   = config.sleep_interval - elapsed_loop
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Step 4: graceful shutdown
    logger.info("Flushing producer buffer before exit...")
    producer.flush()
    logger.info(
        f"Shutdown complete. Total errors: {producer.error_count}"
    )


if __name__ == "__main__":
    main()