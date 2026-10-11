from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailExactDeduplication")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    "data/raw/parquet/year_2009_2010.parquet",
    "data/raw/parquet/year_2010_2011.parquet"
)


print("\n=== BEFORE DEDUPLICATION ===")
original_count = df.count()
print("Rows:", original_count)


# Remove only completely identical rows
deduplicated_df = df.dropDuplicates()


print("\n=== AFTER DEDUPLICATION ===")
deduplicated_count = deduplicated_df.count()
print("Rows:", deduplicated_count)


# Calculate how many rows were removed
rows_removed = original_count - deduplicated_count

print("\n=== DEDUPLICATION RESULT ===")
print("Rows removed:", rows_removed)


spark.stop()