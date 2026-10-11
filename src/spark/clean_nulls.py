from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailNullCleaning")
    .master("local[*]")
    .getOrCreate()
)

# Read both Parquet files
df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

print("Original rows:", df.count())

# Remove rows where essential fields are missing
clean_df = df.dropna(
    subset=[
        "invoice",
        "stockcode",
        "quantity",
        "invoicedate",
        "price"
    ]
)

print("Rows after NULL cleaning:", clean_df.count())

print("\n=== REMAINING NULL COUNTS ===")

for column in clean_df.columns:
    null_count = clean_df.filter(
        col(column).isNull()
    ).count()

    print(f"{column}: {null_count}")

spark.stop()