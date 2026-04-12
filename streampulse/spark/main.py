import time
import logging
from pyspark.sql import SparkSession
from utils.spark_session import build_spark_session
from jobs.bronze_writer import BronzeWriter
from jobs.silver_transformer import SilverTransformer
from jobs.gold_aggregator import GoldAggregator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("streampulse.spark")


def main():
    logger.info("Building SparkSession...")
    spark = build_spark_session()
    logger.info(f"SparkSession ready — master: {spark.sparkContext.master}")

    # Start all three streaming queries.
    # Each runs in its own background thread.
    # They all share the same SparkSession and cluster resources.

    logger.info("Starting Bronze writer...")
    bronze = BronzeWriter(spark)
    bronze_query = bronze.start()

    # Brief pause to let Bronze establish its first checkpoint
    # before Silver starts reading from it.
    time.sleep(10)

    logger.info("Starting Silver transformer...")
    silver = SilverTransformer(spark)
    silver_query = silver.start()

    time.sleep(10)

    logger.info("Starting Gold aggregator...")
    gold = GoldAggregator(spark)
    gold_query = gold.start()

    logger.info(
        "All streaming queries running. "
        "Spark UI: http://localhost:4040"
    )

    # awaitAnyTermination blocks the main thread until one of
    # the queries fails or is stopped explicitly.
    # If any query crashes, main() exits and Docker restarts the container.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()