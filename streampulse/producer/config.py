import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Kafka
    BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ACKS: str              = os.getenv("KAFKA_ACKS", "all")
    COMPRESSION: str       = os.getenv("KAFKA_COMPRESSION", "snappy")
    LINGER_MS: int         = int(os.getenv("KAFKA_LINGER_MS", "5"))
    BATCH_SIZE: int        = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))

    # Schema Registry
    SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    # Topics
    TOPIC_CLICKSTREAM: str = "clickstream"
    TOPIC_ORDERS: str      = "orders"
    TOPIC_SESSIONS: str    = "sessions"
    TOPIC_DLQ: str         = "dlq"

    # Producer behavior
    EVENTS_PER_SECOND: int = int(os.getenv("EVENTS_PER_SECOND", "100"))
    NUM_USERS: int         = int(os.getenv("NUM_USERS", "500"))
    NUM_PRODUCTS: int      = int(os.getenv("NUM_PRODUCTS", "200"))

    # Derived: sleep time between events
    @property
    def sleep_interval(self) -> float:
        return 1.0 / self.EVENTS_PER_SECOND


config = Config()