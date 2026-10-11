from pyspark.sql import SparkSession
from pyspark.sql.functions import count


spark = (
    SparkSession.builder
    .appName("RetailDuplicateAnalysis")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

print("Original rows:", df.count())

# Find completely identical rows
duplicate_groups = (
    df.groupBy(df.columns)
      .count()
      .filter("count > 1")
)

print("\n=== EXACT DUPLICATE GROUPS ===")
duplicate_groups.show(10, truncate=False)

print(
    "\nNumber of duplicate groups:",
    duplicate_groups.count()
)

# Calculate the extra duplicate rows
duplicate_extra_rows = (
    duplicate_groups
    .selectExpr("sum(count - 1) as duplicate_rows")
    .collect()[0]["duplicate_rows"]
)

print(
    "\nRows that would be removed:",
    duplicate_extra_rows
)

spark.stop()