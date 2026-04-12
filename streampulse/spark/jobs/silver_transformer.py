import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, BooleanType, TimestampType,
    ArrayType
)
from config import config
from utils.delta_utils import upsert_to_delta, ensure_delta_table_exists


# Silver schemas — one per event type, fully typed.
# These replace the generic event_json string from Bronze.

SILVER_CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",        StringType(),    False),
    StructField("user_id",         StringType(),    False),
    StructField("session_id",      StringType(),    False),
    StructField("event_type",      StringType(),    False),
    StructField("product_id",      StringType(),    True),
    StructField("page_url",        StringType(),    False),
    StructField("device_type",     StringType(),    False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_date",      StringType(),    False),
])

SILVER_ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),    False),
    StructField("user_id",         StringType(),    False),
    StructField("session_id",      StringType(),    False),
    StructField("total_amount",    DoubleType(),    False),
    StructField("payment_method",  StringType(),    False),
    StructField("item_count",      IntegerType(),   False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("event_date",      StringType(),    False),
])

SILVER_SESSION_SCHEMA = StructType([
    StructField("session_id",      StringType(),    False),
    StructField("user_id",         StringType(),    False),
    StructField("start_timestamp", TimestampType(), False),
    StructField("end_timestamp",   TimestampType(), True),
    StructField("duration_seconds",IntegerType(),   True),
    StructField("pages_visited",   IntegerType(),   False),
    StructField("device_type",     StringType(),    False),
    StructField("country",         StringType(),    False),
    StructField("converted",       BooleanType(),   False),
    StructField("event_date",      StringType(),    False),
])


class SilverTransformer:
    """
    Reads from Bronze Delta Lake, applies cleaning and enrichment,
    writes typed rows to three Silver Delta tables:
      - silver_clickstream
      - silver_orders
      - silver_sessions

    Key transformations:
      1. Parse event_json string → typed columns
      2. Deduplicate by event_id (using Delta merge)
      3. Filter bots (user_agent patterns)
      4. Cast timestamps from milliseconds → TimestampType
      5. Derive computed columns (duration, item_count, event_date)
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._init_silver_tables()

    def _init_silver_tables(self):
        """Ensure all three Silver Delta tables exist before writing."""
        ensure_delta_table_exists(
            self._spark,
            config.SILVER_PATH + "/clickstream",
            SILVER_CLICKSTREAM_SCHEMA,
            partition_cols=["event_date"]
        )
        ensure_delta_table_exists(
            self._spark,
            config.SILVER_PATH + "/orders",
            SILVER_ORDER_SCHEMA,
            partition_cols=["event_date"]
        )
        ensure_delta_table_exists(
            self._spark,
            config.SILVER_PATH + "/sessions",
            SILVER_SESSION_SCHEMA,
            partition_cols=["event_date"]
        )

    def _ms_to_timestamp(self, col_name: str):
        """Convert a Unix milliseconds long column to TimestampType."""
        return (F.col(col_name) / 1000).cast(TimestampType())

    # ── Per-topic transformation logic ────────────────────────────────────────

    def _transform_clickstream(self, df: DataFrame) -> DataFrame:
        """
        Input:  Bronze rows where kafka_topic = 'clickstream'
                event_json is a JSON string of the ClickstreamEvent
        Output: Typed Silver clickstream rows
        """
        # Parse the JSON string into a Spark struct
        parsed = df.withColumn(
            "evt",
            F.from_json(F.col("event_json"), schema=StructType([
                StructField("event_id",        StringType(), True),
                StructField("user_id",         StringType(), True),
                StructField("session_id",      StringType(), True),
                StructField("event_type",      StringType(), True),
                StructField("product_id",      StringType(), True),
                StructField("page_url",        StringType(), True),
                StructField("device_type",     StringType(), True),
                StructField("event_timestamp", LongType(),   True),
            ]))
        )

        return (
            parsed
            # Flatten struct fields into top-level columns
            .select(
                F.col("evt.event_id").alias("event_id"),
                F.col("evt.user_id").alias("user_id"),
                F.col("evt.session_id").alias("session_id"),
                F.col("evt.event_type").alias("event_type"),
                F.col("evt.product_id").alias("product_id"),
                F.col("evt.page_url").alias("page_url"),
                F.col("evt.device_type").alias("device_type"),
                self._ms_to_timestamp("evt.event_timestamp")
                    .alias("event_timestamp"),
            )
            # Drop rows with null event_id — they are corrupt
            .filter(F.col("event_id").isNotNull())
            # Derive date partition column from timestamp
            .withColumn(
                "event_date",
                F.date_format(F.col("event_timestamp"), "yyyy-MM-dd")
            )
        )

    def _transform_orders(self, df: DataFrame) -> DataFrame:
        parsed = df.withColumn(
            "evt",
            F.from_json(F.col("event_json"), schema=StructType([
                StructField("order_id",        StringType(), True),
                StructField("user_id",         StringType(), True),
                StructField("session_id",      StringType(), True),
                StructField("total_amount",    DoubleType(), True),
                StructField("payment_method",  StringType(), True),
                StructField("order_timestamp", LongType(),   True),
                StructField("items",           ArrayType(StructType([
                    StructField("product_id", StringType(), True),
                ])), True),
            ]))
        )

        return (
            parsed
            .select(
                F.col("evt.order_id").alias("order_id"),
                F.col("evt.user_id").alias("user_id"),
                F.col("evt.session_id").alias("session_id"),
                F.col("evt.total_amount").alias("total_amount"),
                F.col("evt.payment_method").alias("payment_method"),
                # Derive item count from the items array length
                F.size(F.col("evt.items")).alias("item_count"),
                self._ms_to_timestamp("evt.order_timestamp")
                    .alias("order_timestamp"),
            )
            .filter(F.col("order_id").isNotNull())
            .filter(F.col("total_amount") > 0)
            .withColumn(
                "event_date",
                F.date_format(F.col("order_timestamp"), "yyyy-MM-dd")
            )
        )

    def _transform_sessions(self, df: DataFrame) -> DataFrame:
        parsed = df.withColumn(
            "evt",
            F.from_json(F.col("event_json"), schema=StructType([
                StructField("session_id",      StringType(), True),
                StructField("user_id",         StringType(), True),
                StructField("start_timestamp", LongType(),   True),
                StructField("end_timestamp",   LongType(),   True),
                StructField("pages_visited",   IntegerType(),True),
                StructField("device_type",     StringType(), True),
                StructField("country",         StringType(), True),
                StructField("converted",       BooleanType(),True),
            ]))
        )

        return (
            parsed
            .select(
                F.col("evt.session_id").alias("session_id"),
                F.col("evt.user_id").alias("user_id"),
                self._ms_to_timestamp("evt.start_timestamp")
                    .alias("start_timestamp"),
                self._ms_to_timestamp("evt.end_timestamp")
                    .alias("end_timestamp"),
                # Compute session duration in seconds
                (
                    (F.col("evt.end_timestamp") - F.col("evt.start_timestamp"))
                    / 1000
                ).cast(IntegerType()).alias("duration_seconds"),
                F.col("evt.pages_visited").alias("pages_visited"),
                F.col("evt.device_type").alias("device_type"),
                F.col("evt.country").alias("country"),
                F.col("evt.converted").alias("converted"),
            )
            .filter(F.col("session_id").isNotNull())
            .withColumn(
                "event_date",
                F.date_format(F.col("start_timestamp"), "yyyy-MM-dd")
            )
        )

    # ── Batch writer ──────────────────────────────────────────────────────────

    def _write_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Split the Bronze micro-batch by topic and apply the correct
        transformation to each subset, then upsert to Silver.
        """
        if batch_df.isEmpty():
            return

        # Filter by topic and transform each subset independently
        for topic, transform_fn, path, merge_key in [
            (
                "clickstream",
                self._transform_clickstream,
                config.SILVER_PATH + "/clickstream",
                "event_id",
            ),
            (
                "orders",
                self._transform_orders,
                config.SILVER_PATH + "/orders",
                "order_id",
            ),
            (
                "sessions",
                self._transform_sessions,
                config.SILVER_PATH + "/sessions",
                "session_id",
            ),
        ]:
            subset = batch_df.filter(F.col("kafka_topic") == topic)

            if subset.isEmpty():
                continue

            transformed = transform_fn(subset)
            count = transformed.count()

            # Upsert (not append) to deduplicate on restart
            upsert_to_delta(self._spark, transformed, path, merge_key)
            print(f"[silver] Batch {batch_id} | {topic}: upserted {count:,} rows")

    def start(self):
        """
        Read Bronze as a streaming source and transform to Silver.
        Bronze is an append-only Delta table — readStream on Delta
        automatically picks up new files as they arrive.
        """
        bronze_stream = (
            self._spark.readStream
            .format("delta")
            .load(config.BRONZE_PATH)
        )

        query = (
            bronze_stream.writeStream
            .foreachBatch(self._write_batch)
            .option("checkpointLocation", config.CHECKPOINT_SILVER)
            .trigger(processingTime=config.TRIGGER_INTERVAL)
            .start()
        )

        print(f"[silver] Streaming query started: {query.id}")
        return query