from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from config import config


def build_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession with:
      - Delta Lake support (ACID, time travel, schema evolution)
      - S3A connector configured to talk to MinIO
      - Kafka connector JARs (loaded via Maven coordinates)
    """

    builder = (
        SparkSession.builder
        .appName(config.APP_NAME)
        .master(config.SPARK_MASTER)

        # ── Delta Lake extension ───────────────────────────────────
        # Registers Delta as a data source and enables the
        # DeltaCatalog so you can use SQL like DESCRIBE HISTORY.
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # ── S3A → MinIO configuration ──────────────────────────────
        # S3A is the Hadoop filesystem implementation for S3-compatible
        # storage. You point it at MinIO instead of AWS S3.
        .config("spark.hadoop.fs.s3a.endpoint",          config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # path.style.access=true is required for MinIO.
        # AWS S3 uses virtual-hosted style (bucket.s3.amazonaws.com).
        # MinIO uses path style (minio:9000/bucket). Without this,
        # Spark looks for a DNS entry "streampulse-bronze.minio"
        # which doesn't exist and the connection fails.
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── Package dependencies (loaded from Maven at startup) ────
        # Spark downloads these JARs when the session starts.
        # kafka connector + delta lake + hadoop-aws (S3A)
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        # ── Performance tuning ────────────────────────────────────
        # How many shuffle partitions to use after a join/agg.
        # Default is 200 — absurd for a local/small cluster.
        # 4 matches your 2-core worker with headroom.
        .config("spark.sql.shuffle.partitions", "4")

        # Adaptive Query Execution: Spark re-optimizes plans
        # at runtime based on actual data statistics.
        .config("spark.sql.adaptive.enabled", "true")
        # Ensure Python workers on executors can import /app packages
        # (jobs/, utils/, config.py) when running foreachBatch RDD code.
        .config("spark.executorEnv.PYTHONPATH", "/app")
        .config("spark.driverEnv.PYTHONPATH", "/app")
        .config("spark.executorEnv.SCHEMA_REGISTRY_URL", config.SCHEMA_REGISTRY_URL)
    )

    # configure_spark_with_delta_pip wires Delta Lake properly
    # and we pass additional packages here so they don't get overwritten.
    extra_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

    # Reduce Spark's default verbosity — INFO logs from the
    # Java internals are extremely noisy. WARNING only.
    spark.sparkContext.setLogLevel("WARN")

    return spark
