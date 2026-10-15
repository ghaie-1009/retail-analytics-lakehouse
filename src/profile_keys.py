from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
RAW_DIR = PROJECT_DIR / "data" / "raw"

spark = (
    SparkSession.builder
    .appName("RetailAnalyticsKeyProfiling")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

tables = {
    "orders": (
        "olist_orders_dataset.csv",
        ["order_id"],
    ),
    "customers": (
        "olist_customers_dataset.csv",
        ["customer_id"],
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        ["order_id", "order_item_id"],
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential"],
    ),
    "products": (
        "olist_products_dataset.csv",
        ["product_id"],
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        ["seller_id"],
    ),
}

for table_name, (filename, key_columns) in tables.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(RAW_DIR / filename))
    )

    total_rows = df.count()

    # Count rows with at least one missing key component.
    missing_key_condition = None

    for key in key_columns:
        condition = col(key).isNull()

        if missing_key_condition is None:
            missing_key_condition = condition
        else:
            missing_key_condition = (
                missing_key_condition | condition
            )

    missing_keys = df.filter(missing_key_condition).count()

    # Compare total and distinct composite-key counts.
    valid_keys = df.filter(~missing_key_condition)
    distinct_keys = valid_keys.select(*key_columns).distinct().count()

    duplicate_rows = valid_keys.count() - distinct_keys

    print(f"\n{'=' * 60}")
    print(f"TABLE: {table_name}")
    print(f"Key columns: {key_columns}")
    print(f"Total rows: {total_rows}")
    print(f"Rows with missing key components: {missing_keys}")
    print(f"Distinct valid keys: {distinct_keys}")
    print(f"Duplicate key rows: {duplicate_rows}")

spark.stop()