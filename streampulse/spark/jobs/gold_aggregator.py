from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, TimestampType
)
from config import config
from utils.delta_utils import append_to_delta, ensure_delta_table_exists


class GoldAggregator:
    """
    Reads from Silver Delta tables and produces four Gold tables
    that the Grafana dashboard reads directly.

    Gold tables are pre-aggregated and small — optimized for
    fast analytical reads, not for row-level queries.

    Four tables:
      1. revenue_by_5min    → time-series revenue chart
      2. top_products       → bar chart of best-selling products
      3. conversion_funnel  → funnel drop-off visualization
      4. active_users       → gauge of recent unique users
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._init_gold_tables()

    def _init_gold_tables(self):
        schemas = [
            (config.GOLD_REVENUE,  StructType([
                StructField("window_start",  TimestampType(), False),
                StructField("window_end",    TimestampType(), False),
                StructField("total_revenue", DoubleType(),    False),
                StructField("order_count",   LongType(),      False),
                StructField("avg_order",     DoubleType(),    False),
            ])),
            (config.GOLD_PRODUCTS, StructType([
                StructField("window_start",  TimestampType(), False),
                StructField("product_id",    StringType(),    False),
                StructField("revenue",       DoubleType(),    False),
                StructField("order_count",   LongType(),      False),
            ])),
            (config.GOLD_FUNNEL, StructType([
                StructField("window_start",  TimestampType(), False),
                StructField("page_views",    LongType(),      False),
                StructField("add_to_carts",  LongType(),      False),
                StructField("purchases",     LongType(),      False),
                StructField("cart_rate",     DoubleType(),    False),
                StructField("conv_rate",     DoubleType(),    False),
            ])),
            (config.GOLD_USERS, StructType([
                StructField("window_start",  TimestampType(), False),
                StructField("unique_users",  LongType(),      False),
            ])),
        ]
        for path, schema in schemas:
            ensure_delta_table_exists(self._spark, path, schema)

    # ── Aggregation 1: revenue per 5-minute window ────────────────────────────

    def _compute_revenue(self, orders_df: DataFrame) -> DataFrame:
        """
        Group completed orders into 5-minute tumbling windows.
        Each row in the output = one 5-minute bucket of revenue.
        """
        return (
            orders_df
            .withWatermark("order_timestamp", config.WATERMARK_DELAY)
            # Watermark: tell Spark to wait up to 10 minutes for
            # late data before finalizing a window. Events arriving
            # later than 10 minutes after their window closes are dropped.
            .groupBy(
                F.window(
                    F.col("order_timestamp"),
                    config.WINDOW_DURATION   # "5 minutes"
                )
            )
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.count("order_id").alias("order_count"),
                F.avg("total_amount").alias("avg_order"),
            )
            # Flatten the window struct into two columns
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.round(F.col("total_revenue"), 2).alias("total_revenue"),
                F.col("order_count"),
                F.round(F.col("avg_order"), 2).alias("avg_order"),
            )
        )

    # ── Aggregation 2: top products per window ────────────────────────────────

    def _compute_top_products(self, orders_df: DataFrame) -> DataFrame:
        """
        Join orders back to Bronze clickstream to get product-level
        revenue. In a real system you'd join to a product catalog table.
        Here we compute per-order revenue grouped by session as a proxy.

        NOTE: In Phase 4 (dbt) you'll add a proper product dimension.
        For now we aggregate at order level by window.
        """
        return (
            orders_df
            .withWatermark("order_timestamp", config.WATERMARK_DELAY)
            .groupBy(
                F.window(F.col("order_timestamp"), config.WINDOW_DURATION),
                F.col("session_id"),   # proxy for product grouping
            )
            .agg(
                F.sum("total_amount").alias("revenue"),
                F.count("order_id").alias("order_count"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("session_id").alias("product_id"),
                F.round(F.col("revenue"), 2).alias("revenue"),
                F.col("order_count"),
            )
        )

    # ── Aggregation 3: conversion funnel per window ───────────────────────────

    def _compute_funnel(self, clicks_df: DataFrame) -> DataFrame:
        """
        Count page_view, add_to_cart, and purchase events per window.
        Derive conversion rates from the counts.
        """
        windowed = (
            clicks_df
            .withWatermark("event_timestamp", config.WATERMARK_DELAY)
            .groupBy(F.window(F.col("event_timestamp"), config.WINDOW_DURATION))
            .agg(
                # Count each event type selectively using when()
                F.sum(
                    F.when(F.col("event_type") == "page_view", 1).otherwise(0)
                ).alias("page_views"),
                F.sum(
                    F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
                ).alias("add_to_carts"),
                F.sum(
                    F.when(F.col("event_type") == "product_click", 1).otherwise(0)
                ).alias("purchases"),
            )
        )

        return (
            windowed
            .select(
                F.col("window.start").alias("window_start"),
                F.col("page_views"),
                F.col("add_to_carts"),
                F.col("purchases"),
                # cart rate = add_to_carts / page_views
                F.round(
                    F.when(F.col("page_views") > 0,
                        F.col("add_to_carts") / F.col("page_views") * 100
                    ).otherwise(0.0),
                    2
                ).alias("cart_rate"),
                # conversion rate = purchases / page_views
                F.round(
                    F.when(F.col("page_views") > 0,
                        F.col("purchases") / F.col("page_views") * 100
                    ).otherwise(0.0),
                    2
                ).alias("conv_rate"),
            )
        )

    # ── Aggregation 4: active unique users per window ─────────────────────────

    def _compute_active_users(self, clicks_df: DataFrame) -> DataFrame:
        """
        Count distinct users who sent any event in each 5-min window.
        This powers the "active users" gauge in Grafana.
        """
        return (
            clicks_df
            .withWatermark("event_timestamp", config.WATERMARK_DELAY)
            .groupBy(F.window(F.col("event_timestamp"), config.WINDOW_DURATION))
            .agg(F.countDistinct("user_id").alias("unique_users"))
            .select(
                F.col("window.start").alias("window_start"),
                F.col("unique_users"),
            )
        )

    # ── Batch writer ──────────────────────────────────────────────────────────

    def _write_batch(self, batch_df: DataFrame, batch_id: int):
        """
        batch_df here is actually a combined Silver stream.
        We split by source type and run each aggregation separately.
        """
        if batch_df.isEmpty():
            return

        # Split: clickstream rows vs order rows
        clicks = batch_df.filter(F.col("_source") == "clickstream")
        orders  = batch_df.filter(F.col("_source") == "orders")

        if not clicks.isEmpty():
            funnel = self._compute_funnel(clicks)
            append_to_delta(funnel, config.GOLD_FUNNEL)

            users = self._compute_active_users(clicks)
            append_to_delta(users, config.GOLD_USERS)

        if not orders.isEmpty():
            revenue = self._compute_revenue(orders)
            append_to_delta(revenue, config.GOLD_REVENUE)

            products = self._compute_top_products(orders)
            append_to_delta(products, config.GOLD_PRODUCTS)

        print(f"[gold] Batch {batch_id}: aggregations written")

    def start(self):
        """
        Read Silver clickstream and orders as unified stream.
        Tag each row with its source so _write_batch can split them.
        """
        clicks_stream = (
            self._spark.readStream
            .format("delta")
            .option("skipChangeCommits", "true")
            .load(config.SILVER_PATH + "/clickstream")
            .withColumn("_source", F.lit("clickstream"))
        )

        orders_stream = (
            self._spark.readStream
            .format("delta")
            .option("skipChangeCommits", "true")
            .load(config.SILVER_PATH + "/orders")
            .withColumn("_source", F.lit("orders"))
        )

        # Union the two streams into one for a single foreachBatch
        combined = clicks_stream.unionByName(orders_stream, allowMissingColumns=True)

        query = (
            combined.writeStream
            .foreachBatch(self._write_batch)
            .option("checkpointLocation", config.CHECKPOINT_GOLD)
            .trigger(processingTime=config.TRIGGER_INTERVAL)
            .start()
        )

        print(f"[gold] Streaming query started: {query.id}")
        return query
