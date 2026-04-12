import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType,
    DoubleType, ArrayType, TimestampType, DateType
)
from config import config
from utils.schema_fetcher import SchemaFetcher
from utils.delta_utils import append_to_delta, ensure_delta_table_exists


# Bronze schema: raw decoded events with Kafka metadata attached.
# All three topics land in one Bronze table — differentiated by
# the 'topic' column.
BRONZE_SCHEMA = StructType([
    StructField("kafka_topic",      StringType(),    False),
    StructField("kafka_partition",  IntegerType(),   False),
    StructField("kafka_offset",     LongType(),      False),
    StructField("kafka_timestamp",  TimestampType(), False),
    # Raw event stored as JSON string for maximum flexibility.
    # Silver will parse this into typed columns.
    StructField("event_json",       StringType(),    False),
    StructField("ingested_at",      TimestampType(), False),
    StructField("event_date",       DateType(),      False),
])


class BronzeWriter:
    """
    Reads from all three Kafka topics as a unified stream.
    Deserializes each Avro message and writes to Bronze Delta Lake.

    No business logic here — Bronze is a faithful copy of what
    arrived in Kafka, plus the Kafka metadata for debugging.
    """

    def __init__(self, spark: SparkSession):
        self._spark    = spark
        self._fetcher  = SchemaFetcher()
        ensure_delta_table_exists(
            self._spark,
            config.BRONZE_PATH,
            BRONZE_SCHEMA,
            partition_cols=["kafka_topic", "event_date"],
        )

    def _build_kafka_stream(self) -> DataFrame:
        """
        Create a streaming DataFrame that reads from all three topics.
        Spark polls Kafka every trigger interval and produces one
        micro-batch per topic partition.
        """
        return (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",  config.BOOTSTRAP_SERVERS)
            .option("subscribe",                config.TOPICS)
            # subscribe accepts a comma-separated list of topics.
            # Spark creates one consumer per partition across all topics.
            .option("startingOffsets",          config.STARTING_OFFSETS)
            .option("kafka.group.id",           config.CONSUMER_GROUP)
            .option("failOnDataLoss",           "false")
            # failOnDataLoss=false: if Kafka has deleted old messages
            # (log retention expired), don't crash — just skip them.
            .option("maxOffsetsPerTrigger",     "50000")
            # Cap each micro-batch at 50k messages. Without this,
            # a large backlog would create one enormous micro-batch
            # that takes forever and may OOM your worker.
            .load()
        )

    def _deserialize_batch(self, batch_df: DataFrame) -> DataFrame:
        """
        Called once per micro-batch by foreachBatch().
        Converts the raw Kafka bytes into decoded event rows.

        Kafka gives us:
          key       : bytes (user_id)
          value     : bytes (Confluent Avro wire format)
          topic     : string
          partition : int
          offset    : long
          timestamp : timestamp
        """
        fetcher = self._fetcher   # closure for use inside map

        def decode_row(row):
            """Applied to each row in the micro-batch."""
            try:
                record = fetcher.deserialize(bytes(row.value))
                return {
                    "kafka_topic":     row.topic,
                    "kafka_partition": row.partition,
                    "kafka_offset":    row.offset,
                    "kafka_timestamp": row.timestamp,
                    "event_json":      json.dumps(record),
                    "ingested_at":     __import__('datetime').datetime.utcnow(),
                    "event_date":      row.timestamp.date(),
                }
            except Exception as e:
                # Deserialization failure → log and return None
                # (None rows are filtered out below)
                print(f"[bronze] Deserialization error: {e}")
                return None

        # Convert Spark DataFrame to Python RDD to apply Python logic,
        # then convert back to DataFrame.
        # This is necessary because Avro deserialization requires
        # Python code (fastavro) that Spark can't push down to JVM.
        rdd = batch_df.rdd.map(decode_row).filter(lambda r: r is not None)

        if rdd.isEmpty():
            return self._spark.createDataFrame([], BRONZE_SCHEMA)

        return self._spark.createDataFrame(rdd, BRONZE_SCHEMA)

    def _write_batch(self, batch_df: DataFrame, batch_id: int):
        """
        foreachBatch callback. Called once per micro-batch with:
          batch_df : the current micro-batch as a static DataFrame
          batch_id : monotonically increasing batch identifier
        """
        if batch_df.isEmpty():
            print(f"[bronze] Batch {batch_id}: empty, skipping")
            return

        decoded = self._deserialize_batch(batch_df)

        if decoded.isEmpty():
            return

        append_to_delta(
            decoded,
            config.BRONZE_PATH,
            partition_cols=["kafka_topic", "event_date"],
        )

        count = decoded.count()
        print(f"[bronze] Batch {batch_id}: wrote {count:,} rows")

    def start(self):
        """
        Launch the Bronze streaming query. Returns the StreamingQuery
        object so main.py can await its termination.
        """
        kafka_stream = self._build_kafka_stream()

        query = (
            kafka_stream.writeStream
            .foreachBatch(self._write_batch)
            # foreachBatch is the key pattern for Delta Lake writes.
            # Instead of using a built-in Delta sink (which has
            # limitations), you get each micro-batch as a static
            # DataFrame and can run any arbitrary Spark operations on it.
            .option("checkpointLocation", config.CHECKPOINT_BRONZE)
            .trigger(processingTime=config.TRIGGER_INTERVAL)
            # processingTime trigger: Spark waits exactly 30 seconds
            # between micro-batches. The previous batch must finish
            # before the next one starts.
            .start()
        )

        print(f"[bronze] Streaming query started: {query.id}")
        return query
