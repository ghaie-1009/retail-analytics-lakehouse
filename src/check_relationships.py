
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
RAW_DIR = PROJECT_DIR / "data" / "raw"

spark = (
    SparkSession.builder
    .appName("RetailAnalyticsRelationshipChecks")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def read_csv(filename):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(RAW_DIR / filename))
    )


orders = read_csv("olist_orders_dataset.csv")
customers = read_csv("olist_customers_dataset.csv")
items = read_csv("olist_order_items_dataset.csv")
payments = read_csv("olist_order_payments_dataset.csv")
products = read_csv("olist_products_dataset.csv")
sellers = read_csv("olist_sellers_dataset.csv")


def check_orphans(child_df, child_key, parent_df, parent_key, relationship):
    child_keys = (
        child_df.select(col(child_key).alias("join_key"))
        .filter(col("join_key").isNotNull())
        .distinct()
    )

    parent_keys = (
        parent_df.select(col(parent_key).alias("join_key"))
        .filter(col("join_key").isNotNull())
        .distinct()
    )

    orphan_count = child_keys.join(
        parent_keys,
        on="join_key",
        how="left_anti"
    ).count()

    print(f"{relationship}: {orphan_count} unmatched distinct keys")


check_orphans(
    orders, "customer_id",
    customers, "customer_id",
    "Orders -> Customers"
)

check_orphans(
    items, "order_id",
    orders, "order_id",
    "Order Items -> Orders"
)

check_orphans(
    items, "product_id",
    products, "product_id",
    "Order Items -> Products"
)

check_orphans(
    items, "seller_id",
    sellers, "seller_id",
    "Order Items -> Sellers"
)

check_orphans(
    payments, "order_id",
    orders, "order_id",
    "Payments -> Orders"
)

spark.stop()