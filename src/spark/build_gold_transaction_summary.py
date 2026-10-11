from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("RetailGoldTransactionSummary")
    .master("local[*]")
    .getOrCreate()
)

# 1. READ SILVER
silver_df = spark.read.parquet(
    "data/silver/retail_transactions"
)

print("\n=== SILVER DATA ===")
print("Rows:", silver_df.count())

# 2. TRANSACTION TYPE SUMMARY
transaction_summary = (
    silver_df
    .groupBy("transaction_type")
    .agg(
        count("*").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        round(
            sum("revenue"),
            2
        ).alias("total_revenue")
    )
    .orderBy("transaction_type")
)

print("\n=== TRANSACTION SUMMARY ===")

transaction_summary.show(
    truncate=False
)

# 3. WRITE GOLD
output_path = "data/gold/transaction_summary"

transaction_summary.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== GOLD TRANSACTION SUMMARY CREATED ===")
print("Output:", output_path)

spark.stop()