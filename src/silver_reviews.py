
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
    sum as spark_sum,
)

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"

builder = (
    SparkSession.builder
    .appName("SilverReviews")
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
        .load(str(BRONZE_DIR / "reviews"))
    )

    # Clean identifier and categorical text columns.
    for column_name in [
        "review_id",
        "order_id",
        "review_comment_title",
        "review_comment_message",
    ]:
        df = df.withColumn(
            column_name,
            trim(col(column_name)),
        )

    # The source stores review_score as a string.
    # Convert it to an integer for reliable validation and analysis.
    df = df.withColumn(
        "review_score",
        col("review_score").cast("int"),
    )

    
    df = (
        df
        .withColumn(
            "has_valid_review_id",
            col("review_id").isNotNull() & (col("review_id") != ""),
        )
        .withColumn(
            "has_valid_order_id",
            col("order_id").isNotNull() & (col("order_id") != ""),
        )
        .withColumn(
            "has_valid_score",
            col("review_score").isNotNull()
            & col("review_score").between(1, 5),
        )
        .withColumn(
            "is_valid_review",
            col("has_valid_review_id")
            & col("has_valid_order_id")
            & col("has_valid_score"),
        )
    )

    target_path = SILVER_DIR / "reviews"

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

    
    total_rows = silver_df.count()

    invalid_rows = silver_df.filter(
        ~col("is_valid_review")
    ).count()

    print(f"Total rows: {total_rows:,}")
    print(f"Invalid review rows: {invalid_rows:,}")

    print("Invalid rows by reason (reasons may overlap):")

    for flag in [
        "has_valid_review_id",
        "has_valid_order_id",
        "has_valid_score",
    ]:
        count = silver_df.filter(~col(flag)).count()
        print(f"{flag}: {count:,}")

    # Find groups with duplicate review IDs.
    
    duplicate_groups = (
        silver_df
        .groupBy("review_id")
        .count()
        .filter(
            col("review_id").isNotNull()
            & (col("count") > 1)
        )
    )

    print("Sample duplicate review IDs and their records:")

    duplicate_id_list = (
        duplicate_groups
        .select("review_id")
    )

    (
        silver_df
        .join(duplicate_id_list, on="review_id", how="inner")
        .orderBy("review_id")
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
        )
        .show(20, truncate=40)
    )
    duplicate_id_groups = duplicate_groups.count()

    # Count rows beyond the first occurrence of each duplicated ID.
    duplicate_excess_rows = (
        duplicate_groups
        .agg(
            spark_sum(col("count") - 1).alias("excess_rows")
        )
        .first()["excess_rows"]
        or 0
    )

    print(f"Duplicate review-ID groups: {duplicate_id_groups:,}")
    print(f"Excess rows from duplicate IDs: {duplicate_excess_rows:,}")

    print("Review score distribution:")
    (
        silver_df
        .groupBy("review_score")
        .count()
        .orderBy("review_score")
        .show()
    )

finally:
    spark.stop()