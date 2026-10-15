
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"

builder = (
    SparkSession.builder
    .appName("SilverDimensions")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tables = {
    "customers": {
        "key": "customer_id",
        "text_columns": ["customer_id", "customer_unique_id",
                         "customer_city", "customer_state"],
    },
    "products": {
        "key": "product_id",
        "text_columns": ["product_id", "product_category_name"],
    },
    "sellers": {
        "key": "seller_id",
        "text_columns": ["seller_id", "seller_city", "seller_state"],
    },
    "category_translation": {
        "key": "product_category_name",
        "text_columns": ["product_category_name",
                         "product_category_name_english"],
    },
}

for table_name, rules in tables.items():
    df = (
        spark.read.format("delta")
        .load(str(BRONZE_DIR / table_name))
    )

    # Standardize whitespace in selected text columns.
    for column_name in rules["text_columns"]:
        df = df.withColumn(column_name, trim(col(column_name)))

    # Add basic data-quality flags without dropping source records.
    key_column = rules["key"]
    df = df.withColumn(
        "is_valid_primary_key",
        col(key_column).isNotNull() & (col(key_column) != "")
    )

    target_path = SILVER_DIR / table_name

    (
        df.withColumn("_silver_processed_at", current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path))
    )

    silver_df = spark.read.format("delta").load(str(target_path))

    total_rows = silver_df.count()
    invalid_keys = silver_df.filter(
        ~col("is_valid_primary_key")
    ).count()

    print(
        f"{table_name}: rows={total_rows:,}, "
        f"invalid primary keys={invalid_keys:,}"
    )

spark.stop()