import logging
from confluent_kafka import Producer, KafkaException
from config import config
from serializer import AvroSerializer

logger = logging.getLogger(__name__)


class StreamPulseProducer:

    def __init__(self, serializer: AvroSerializer):
        self._serializer = serializer
        self._producer   = self._build_producer()
        self._error_count = 0

    def _build_producer(self) -> Producer:
        """
        Construct the confluent-kafka Producer with all
        tuned configuration values.
        """
        conf = {
            "bootstrap.servers":  config.BOOTSTRAP_SERVERS,
            "acks":               config.ACKS,
            "compression.type":   config.COMPRESSION,
            "linger.ms":          config.LINGER_MS,
            "batch.size":         config.BATCH_SIZE,
            # Retry up to 5 times with exponential backoff
            # before declaring a message failed
            "retries":            5,
            "retry.backoff.ms":   200,
            # Enable idempotence: the broker deduplicates
            # retried messages so you never get duplicates
            # even after a network hiccup
            "enable.idempotence": True,
        }
        return Producer(conf)

    def _delivery_callback(self, err, msg):
        """
        Called asynchronously by the client once the broker
        acknowledges (or fails) each message.

        err  : None on success, KafkaError on failure
        msg  : the original Message object
        """
        if err is None:
            logger.debug(
                f"Delivered → topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )
        else:
            self._error_count += 1
            logger.error(
                f"Delivery FAILED → topic={msg.topic()} "
                f"key={msg.key()} error={err}"
            )
            # Route the failed raw message to the DLQ
            try:
                self._producer.produce(
                    topic=config.TOPIC_DLQ,
                    value=msg.value(),
                    key=msg.key(),
                    # No callback on DLQ — avoid infinite loop
                )
            except KafkaException as dlq_err:
                logger.critical(f"Could not write to DLQ: {dlq_err}")

    def send(self, topic: str, key: str, record: dict):
        """
        Serialize and produce one event. Non-blocking —
        returns immediately after placing the message
        in the internal buffer.
        """
        try:
            value_bytes = self._serializer.serialize(topic, record)
            self._producer.produce(
                topic=topic,
                value=value_bytes,
                key=key.encode("utf-8"),
                on_delivery=self._delivery_callback,
            )
            # poll(0) = non-blocking: give the client a chance
            # to fire any pending delivery callbacks without
            # waiting for new ones to arrive
            self._producer.poll(0)

        except ValueError as e:
            logger.error(f"Serialization error for topic '{topic}': {e}")
        except KafkaException as e:
            logger.error(f"Kafka error producing to '{topic}': {e}")

    def flush(self):
        """
        Block until all buffered messages are delivered.
        Call this before shutdown to guarantee zero message loss.
        """
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(
                f"Flush timed out — {remaining} messages may be lost"
            )

    @property
    def error_count(self) -> int:
        return self._error_count