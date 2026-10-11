from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,round


spark = (
    SparkSession.builder
    .appName("RetailTransactionClassification")
    .master("local[*]")
    .getOrCreate()
)

# Read both years
df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

# Classify each transaction
classified_df = df.withColumn(
    "transaction_type",
    when(
        col("description") == "Adjust bad debt",
        "ADJUSTMENT"
    )
    .when(
        col("invoice").startswith("C"),
        "CANCELLATION"
    )
    .when(
        col("quantity") < 0,
        "RETURN"
    )
    .otherwise(
        "SALE"
    )
)


classified_df = classified_df.withColumn(
    "revenue",
    round(
        col("quantity") * col("price"),
        2
    )
)

print("\n=== TRANSACTION TYPES ===")

classified_df.groupBy(
    "transaction_type"
).count().show()


print("\n=== SAMPLE CLASSIFIED DATA ===")

classified_df.select(
    "invoice",
    "stockcode",
    "quantity",
    "price",
    "revenue",
    "transaction_type"
).show(20, truncate=False)


spark.stop()