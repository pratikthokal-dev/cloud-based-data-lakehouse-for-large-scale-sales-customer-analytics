from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum,
    round,
    col
)

spark = (
    SparkSession.builder
    .appName("RetailGoldCustomerAnalytics")
    .master("local[*]")
    .getOrCreate()
)

# 1. READ SILVER
silver_df = spark.read.parquet(
    "data/silver/retail_transactions"
)

print("\n=== SILVER DATA ===")
print("Rows:", silver_df.count())

# 2. KEEP SALES WITH CUSTOMER ID
customer_sales = silver_df.filter(
    (col("transaction_type") == "SALE") &
    col("customer_id").isNotNull()
)

print("\n=== CUSTOMER SALES ===")
print("Rows:", customer_sales.count())

# 3. CUSTOMER-LEVEL AGGREGATION
customer_analytics = (
    customer_sales
    .groupBy(
        "customer_id",
        "country"
    )
    .agg(
        countDistinct("invoice").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(
            sum("revenue"),
            2
        ).alias("total_revenue")
    )
)

# 4. CALCULATE AVERAGE ORDER VALUE
customer_analytics = customer_analytics.withColumn(
    "average_order_value",
    round(
        col("total_revenue") /
        col("total_orders"),
        2
    )
)

# 5. SORT BY REVENUE
customer_analytics = customer_analytics.orderBy(
    col("total_revenue").desc()
)

print("\n=== CUSTOMER ANALYTICS ===")

customer_analytics.show(
    20,
    truncate=False
)

# 6. WRITE GOLD
output_path = "data/gold/customer_analytics"

customer_analytics.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== GOLD CUSTOMER ANALYTICS CREATED ===")
print("Output:", output_path)

spark.stop()