
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
)

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"

builder = (
    SparkSession.builder
    .appName("SilverTransactions")
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

tables = {
    "order_items": {
        "keys": ["order_id", "order_item_id"],
        "text_columns": [
            "order_id",
            "product_id",
            "seller_id",
        ],
    },
    "payments": {
        "keys": ["order_id", "payment_sequential"],
        "text_columns": [
            "order_id",
            "payment_type",
        ],
    },
}

try:
    for table_name, rules in tables.items():
        df = (
            spark.read.format("delta")
            .load(str(BRONZE_DIR / table_name))
        )

        for column_name in rules["text_columns"]:
            df = df.withColumn(
                column_name,
                trim(col(column_name)),
            )

        # A composite key is valid only when every
        # key column is populated.
        valid_key = None

        for key_column in rules["keys"]:
            condition = (
                col(key_column).isNotNull()
                & (trim(col(key_column).cast("string")) != "")
            )
            valid_key = (
                condition
                if valid_key is None
                else valid_key & condition
            )

        df = df.withColumn("is_valid_primary_key", valid_key)

        target_path = SILVER_DIR / table_name

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

        invalid_keys = silver_df.filter(
            ~col("is_valid_primary_key")
        ).count()

        duplicate_keys = (
            silver_df
            .groupBy(*rules["keys"])
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(
            f"{table_name}: rows={total_rows:,}, "
            f"invalid key rows={invalid_keys:,}, "
            f"duplicate key combinations={duplicate_keys:,}"
        )

finally:
    spark.stop()