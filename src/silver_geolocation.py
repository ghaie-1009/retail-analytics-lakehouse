
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    trim,
)

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"

builder = (
    SparkSession.builder
    .appName("SilverGeolocation")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    df = (
        spark.read.format("delta")
        .load(str(BRONZE_DIR / "geolocation"))
    )

    # Standardize text fields.
    df = (
        df
        .withColumn(
            "geolocation_city",
            lower(trim(col("geolocation_city"))),
        )
        .withColumn(
            "geolocation_state",
            trim(col("geolocation_state")),
        )
    )

    # Validate coordinates against geographic limits.
    df = df.withColumn(
        "is_valid_coordinate",
        col("geolocation_lat").isNotNull()
        & col("geolocation_lng").isNotNull()
        & col("geolocation_lat").between(-90, 90)
        & col("geolocation_lng").between(-180, 180),
    )

    # A ZIP prefix is required, but is not a unique key here.
    df = df.withColumn(
        "has_valid_zip_prefix",
        col("geolocation_zip_code_prefix").isNotNull(),
    )

    target_path = SILVER_DIR / "geolocation"

    (
        df.withColumn(
            "_silver_processed_at",
            current_timestamp(),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path))
    )

    silver_df = (
        spark.read.format("delta")
        .load(str(target_path))
    )

    print(f"Total rows: {silver_df.count():,}")

    print(
        "Invalid coordinates:",
        silver_df.filter(
            ~col("is_valid_coordinate")
        ).count(),
    )

    print(
        "Missing ZIP prefixes:",
        silver_df.filter(
            ~col("has_valid_zip_prefix")
        ).count(),
    )

    print("Rows by state:")
    (
        silver_df
        .groupBy("geolocation_state")
        .count()
        .orderBy(col("count").desc())
        .show(10, truncate=False)
    )

    print("ZIP prefixes with multiple records:")
    multiple_record_prefixes = (
    silver_df
    .groupBy("geolocation_zip_code_prefix")
    .count()
    .filter(col("count") > 1)
    .count()
    )

    print(
        "ZIP prefixes with multiple records:",
        multiple_record_prefixes,
    )
    

finally:
    spark.stop()