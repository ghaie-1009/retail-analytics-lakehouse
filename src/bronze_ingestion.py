
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
RAW_DIR = PROJECT_DIR / "data" / "raw"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)

builder = (
    SparkSession.builder
    .appName("RetailAnalyticsBronzeIngestion")
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

source_files = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

for table_name, filename in source_files.items():
    source_path = RAW_DIR / filename

    if not source_path.exists():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    
    reader = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
    )

    # Review comments may contain embedded line breaks.
    if table_name == "reviews":
        reader = reader.option("multiLine", True)

    df = reader.csv(str(source_path)) 

    df = (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    target_path = BRONZE_DIR / table_name

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(target_path))
    )

    row_count = spark.read.format("delta").load(str(target_path)).count()

    print(
        f"Bronze table: {table_name} | "
        f"Rows written: {row_count} | "
        f"Location: {target_path}"
    )

spark.stop()