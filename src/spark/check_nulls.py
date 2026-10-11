from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count

spark=(
    SparkSession.builder
    .appName("RetailNullAnalysis")
    .master("local[*]")
    .getOrCreate()
)

df=spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

print("Total rows:", df.count())

print("\n=== MISSING VALUE COUNTS ===")

null_counts = df.select([
    count(
        when(col(c).isNull(), 1)
    ).alias(c)
    for c in df.columns
])

null_counts.show()

spark.stop()