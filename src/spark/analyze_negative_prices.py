from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailNegativePriceAnalysis")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)


# Remove exact duplicates first
clean_df = df.dropDuplicates()


negative_price_df = clean_df.filter(
    col("price") < 0
)


print("\n=== NEGATIVE PRICE TRANSACTIONS ===")

negative_price_df.select(
    "invoice",
    "stockcode",
    "description",
    "quantity",
    "invoicedate",
    "price",
    "customer_id",
    "country"
).show(
    20,
    truncate=False
)


print(
    "\nNegative price rows:",
    negative_price_df.count()
)


spark.stop()