
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lower,
    trim,
    when,
)

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

builder = (
    SparkSession.builder
    .appName("SilverOrders")
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

# 1. Read Bronze
orders = (
    spark.read.format("delta")
    .load(str(BRONZE_DIR / "orders"))
)

# 2. Standardize selected columns
orders = (
    orders
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", lower(trim(col("order_status"))))
)

# 3. Add data-quality flags

orders = (
    orders
    .withColumn(
        "is_valid_order_id",
        col("order_id").isNotNull() & (col("order_id") != "")
    )
    .withColumn(
        "is_valid_order_status",
        col("order_status").isNotNull() & (col("order_status") != "")
    )
    
.withColumn(
    "has_delivery_timestamp_issue",
    (
        (col("order_status") == "delivered")
        & col("order_delivered_customer_date").isNull()
    )
    |
    (
        col("order_delivered_customer_date").isNotNull()
        & col("order_purchase_timestamp").isNotNull()
        & (
            col("order_delivered_customer_date")
            < col("order_purchase_timestamp")
        )
    )
)
.withColumn(
        "delivery_quality_status",
        when(
            (col("order_status") == "delivered")
            & col("order_delivered_customer_date").isNull(),
            "MISSING_DELIVERY_TIMESTAMP",
        )
        .when(
            col("order_delivered_customer_date").isNotNull()
            & col("order_purchase_timestamp").isNotNull()
            & (
                col("order_delivered_customer_date")
                < col("order_purchase_timestamp")
            ),
            "DELIVERY_BEFORE_PURCHASE",
        )
        .otherwise("OK")
    )   
)

# 4. Write Silver Delta table
target_path = SILVER_DIR / "orders"

(
    orders
    .withColumn("_silver_processed_at", current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(str(target_path))
)


# 5. Verify results
silver_orders = spark.read.format("delta").load(str(target_path))

print(f"Silver orders row count: {silver_orders.count():,}")

print("Order statuses:")
silver_orders.groupBy("order_status").count().orderBy("order_status").show()

# 6. Check uniqueness of order_id
total_rows = silver_orders.count()
distinct_order_ids = silver_orders.select("order_id").distinct().count()

print(f"Total rows: {total_rows:,}")
print(f"Distinct order IDs: {distinct_order_ids:,}")
print(f"Duplicate order IDs: {total_rows - distinct_order_ids:,}")

# 7. Business-rule checks
silver_orders.selectExpr(
    """
    sum(
        CASE
            WHEN order_status = 'delivered'
                 AND order_delivered_customer_date IS NULL
            THEN 1 ELSE 0
        END
    ) AS delivered_without_delivery_timestamp
    """,
    """
    sum(
        CASE
            WHEN order_approved_at IS NOT NULL
                 AND order_purchase_timestamp IS NOT NULL
                 AND order_approved_at < order_purchase_timestamp
            THEN 1 ELSE 0
        END
    ) AS approval_before_purchase
    """,
).show()

print("Delivered orders missing delivery timestamps:")

silver_orders.filter(
    (col("order_status") == "delivered")
    & col("order_delivered_customer_date").isNull()
).select(
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
).show(20, truncate=False)
print("Delivery quality status summary:")

silver_orders.groupBy("delivery_quality_status") \
    .count() \
    .orderBy("delivery_quality_status") \
    .show()
spark.stop()