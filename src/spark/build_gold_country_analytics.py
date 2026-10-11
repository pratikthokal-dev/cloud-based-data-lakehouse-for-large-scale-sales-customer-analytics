from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("RetailGoldCountryAnalytics")
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
country_sales = silver_df.filter(
    col("transaction_type") == "SALE"
)

print("\n=== COUNTRY SALES ===")
print("Rows:", country_sales.count())

# 3. COUNTRY-LEVEL AGGREGATION
country_analytics = (
    country_sales
    .groupBy("country")
    .agg(
        countDistinct("invoice").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity"),
        round(
            sum("revenue"),
            2
        ).alias("total_revenue")
    )
)

# 4. CALCULATE AVERAGE ORDER VALUE
country_analytics = country_analytics.withColumn(
    "average_order_value",
    round(
        col("total_revenue") /
        col("total_orders"),
        2
    )
)

# 5. SORT BY REVENUE
country_analytics = country_analytics.orderBy(
    col("total_revenue").desc()
)

print("\n=== COUNTRY ANALYTICS ===")

country_analytics.show(
    30,
    truncate=False
)

# 6. WRITE GOLD
output_path = "data/gold/country_analytics"

country_analytics.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== GOLD COUNTRY ANALYTICS CREATED ===")
print("Output:", output_path)

spark.stop()