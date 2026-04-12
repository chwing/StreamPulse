import os
from dotenv import load_dotenv

load_dotenv()


class SparkConfig:
    # Spark
    SPARK_MASTER: str     = os.getenv("SPARK_MASTER", "local[*]")
    APP_NAME: str         = "StreamPulse-Streaming"

    # Kafka
    BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    TOPICS: str            = "clickstream,orders,sessions"

    # Schema Registry
    SCHEMA_REGISTRY_URL: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )

    # MinIO / S3A
    MINIO_ENDPOINT:   str = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "streampulse")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "streampulse123")

    # Delta Lake paths (S3A URIs)
    BRONZE_PATH:   str = "s3a://streampulse-bronze/events"
    SILVER_PATH:   str = "s3a://streampulse-silver/events"
    # Trino file metastore expects tables under schema folders.
    # Keep Gold tables under /default/ to map to delta.default.*
    GOLD_REVENUE:  str = "s3a://streampulse-gold/default/revenue_by_5min"
    GOLD_PRODUCTS: str = "s3a://streampulse-gold/default/top_products"
    GOLD_FUNNEL:   str = "s3a://streampulse-gold/default/conversion_funnel"
    GOLD_USERS:    str = "s3a://streampulse-gold/default/active_users"

    # Checkpoints (persisted to MinIO so restarts resume from last offset)
    CHECKPOINT_BASE:   str = os.getenv(
        "CHECKPOINT_BASE", "s3a://streampulse-bronze/checkpoints"
    )
    CHECKPOINT_BRONZE: str = f"{os.getenv('CHECKPOINT_BASE', 's3a://streampulse-bronze/checkpoints')}/bronze"
    CHECKPOINT_SILVER: str = f"{os.getenv('CHECKPOINT_BASE', 's3a://streampulse-bronze/checkpoints')}/silver"
    CHECKPOINT_GOLD:   str = f"{os.getenv('CHECKPOINT_BASE', 's3a://streampulse-bronze/checkpoints')}/gold"

    # Streaming behavior
    TRIGGER_INTERVAL:  str = "30 seconds"
    WATERMARK_DELAY:   str = "10 minutes"
    WINDOW_DURATION:   str = "5 minutes"
    STARTING_OFFSETS:  str = "latest"
    # "latest" = only process new messages after job starts
    # "earliest" = replay everything from beginning (use for backfills)

    # Kafka consumer group
    CONSUMER_GROUP: str = "streampulse-spark"


config = SparkConfig()
