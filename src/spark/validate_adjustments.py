from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("RetailAdjustmentValidation")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/raw/parquet/year_2009_2010.parquet",
    "data/raw/parquet/year_2010_2011.parquet"
)

# Remove exact duplicates
clean_df = df.dropDuplicates()

adjustments = clean_df.filter(
    col("description") == "Adjust bad debt"
)

print("\n=== BAD DEBT ADJUSTMENTS ===")

adjustments.select(
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
    "\nAdjustment rows:",
    adjustments.count()
)

spark.stop()