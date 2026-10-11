from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum,
    avg,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("RetailGoldProductAnalytics")
    .master("local[*]")
    .getOrCreate()
)

# 1. READ SILVER
silver_df = spark.read.parquet(
    "data/silver/retail_transactions"
)

print("\n=== SILVER DATA ===")
print("Rows:", silver_df.count())

# 2. KEEP SALES
product_sales = silver_df.filter(
    col("transaction_type") == "SALE"
)

print("\n=== PRODUCT SALES ===")
print("Rows:", product_sales.count())

# 3. PRODUCT-LEVEL AGGREGATION
product_analytics = (
    product_sales
    .groupBy(
        "stockcode",
        "description"
    )
    .agg(
        countDistinct("invoice").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(
            sum("revenue"),
            2
        ).alias("total_revenue"),
        round(
            avg("price"),
            2
        ).alias("average_price")
    )
)

# 4. SORT BY REVENUE
product_analytics = product_analytics.orderBy(
    col("total_revenue").desc()
)

print("\n=== PRODUCT ANALYTICS ===")

product_analytics.show(
    20,
    truncate=False
)

# 5. WRITE GOLD
output_path = "data/gold/product_analytics"

product_analytics.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== GOLD PRODUCT ANALYTICS CREATED ===")
print("Output:", output_path)

spark.stop()