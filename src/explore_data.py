from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_DIR = Path.home() / "retail-analytics-lakehouse"
RAW_DIR = PROJECT_DIR / "data" / "raw"

spark = (
    SparkSession.builder
    .appName("RetailAnalyticsDataExploration")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

csv_files = sorted(RAW_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found in {RAW_DIR}"
    )

print(f"Found {len(csv_files)} CSV files in {RAW_DIR}")

for file_path in csv_files:
    table_name = file_path.stem

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")
    print("=" * 80)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(str(file_path))
    )

    print("\nSchema:")
    df.printSchema()

    print(f"\nRow count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    print(f"Columns: {df.columns}")

    print("\nSample records:")
    df.show(3, truncate=False)

spark.stop()
