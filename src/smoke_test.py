from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("RetailAnalyticsLakehouse")
    .master("local[*]")
    .getOrCreate()
)

orders = [
    (1001, "Delivered", 1200.0),
    (1002, "Delivered", 800.0),
    (1003, "Cancelled", 500.0),
    (1004, "Delivered", 1500.0),
]

columns = ["order_id", "order_status", "order_amount"]

df = spark.createDataFrame(orders, columns)

print("Input data:")
df.show()

print("Delivered revenue:")
(
    df.filter(col("order_status") == "Delivered")
    .agg(spark_sum("order_amount").alias("delivered_revenue"))
    .show()
)

print("Spark version:", spark.version)

spark.stop()
