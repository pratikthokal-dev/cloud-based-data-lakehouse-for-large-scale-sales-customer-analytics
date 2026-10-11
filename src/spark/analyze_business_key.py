from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailDuplicateInspection")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)


# Find repeated Invoice + StockCode combinations
duplicate_keys = (
    df.groupBy(
        "invoice",
        "stockcode"
    )
    .count()
    .filter(col("count") > 1)
    .select(
        "invoice",
        "stockcode"
    )
)


print("\n=== SAMPLE REPEATED INVOICE + STOCKCODE ===")

sample_keys = duplicate_keys.limit(5)


# Join the keys back to the original data
duplicate_records = (
    df.join(
        sample_keys,
        on=["invoice", "stockcode"],
        how="inner"
    )
    .orderBy(
        "invoice",
        "stockcode"
    )
)


duplicate_records.show(
    30,
    truncate=False
)


spark.stop()