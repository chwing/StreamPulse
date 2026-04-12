from __future__ import annotations
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def ensure_delta_table_exists(
    spark: SparkSession,
    path: str,
    schema,
    partition_cols: list[str] | None = None,
):
    """
    Create a Delta table at the given S3A path if it doesn't exist.
    This is idempotent — safe to call on every startup.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        writer = (
            spark.createDataFrame([], schema)
            .write
            .format("delta")
            .mode("overwrite")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        print(f"[delta] Created table at {path}")
    else:
        print(f"[delta] Table already exists at {path}")


def upsert_to_delta(
    spark: SparkSession,
    micro_batch_df: DataFrame,
    path: str,
    merge_key: str,
):
    """
    Merge (upsert) a micro-batch DataFrame into a Delta table.
    Used in Silver layer to handle deduplication:
      - If the merge_key already exists → UPDATE the row
      - If new → INSERT the row

    This prevents duplicate rows when Kafka delivers a message
    twice (at-least-once guarantee).
    """
    target = DeltaTable.forPath(spark, path)

    (
        target.alias("target")
        .merge(
            micro_batch_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_to_delta(df: DataFrame, path: str, partition_cols: list[str] | None = None):
    """
    Simple append write to a Delta table.
    Used for Bronze (raw) and Gold (aggregations) layers.
    """
    writer = df.write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)