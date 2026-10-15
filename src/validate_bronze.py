
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"

builder = (
    SparkSession.builder
    .appName("ValidateBronze")
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

for path in sorted(BRONZE_DIR.iterdir()):
    if path.is_dir() and (path / "_delta_log").exists():
        df = spark.read.format("delta").load(str(path))

        print(
            f"{path.name:22} "
            f"rows={df.count():>10,} | "
            f"columns={len(df.columns):>2}"
        )

        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns

spark.stop()