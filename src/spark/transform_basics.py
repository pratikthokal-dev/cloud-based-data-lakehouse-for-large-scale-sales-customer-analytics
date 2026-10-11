from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round


spark = (
    SparkSession.builder
    .appName("RetailTransformationBasics")
    .master("local[*]")
    .getOrCreate()
)

# Read Bronze/source Parquet
df = spark.read.parquet(
    "data/raw/parquet/year_2009_2010.parquet"
)

print("\n=== ORIGINAL DATA ===")
df.show(5, truncate=False)

# 1. SELECT
selected_df = df.select(
    "invoice",
    "stockcode",
    "description",
    "quantity",
    "price",
    "country"
)

print("\n=== SELECTED COLUMNS ===")
selected_df.show(5, truncate=False)

# 2. FILTER
filtered_df = selected_df.filter(
    col("quantity") > 0
)

print("\n=== POSITIVE QUANTITY ===")
filtered_df.show(5, truncate=False)

# 3. WITH COLUMN
transformed_df = filtered_df.withColumn(
    "revenue",
     round(col("quantity") * col("price"),2)
)

print("\n=== REVENUE COLUMN ===")
transformed_df.show(5, truncate=False)



# 4. DROP DUPLICATES
deduplicated_df = transformed_df.dropDuplicates()

print("\n=== AFTER DEDUPLICATION ===")
deduplicated_df.show(5, truncate=False)

spark.stop()