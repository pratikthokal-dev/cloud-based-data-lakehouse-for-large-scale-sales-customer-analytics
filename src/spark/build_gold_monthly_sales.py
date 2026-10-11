from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    month,
    count,
    sum,
    round
)

spark = (
    SparkSession.builder
    .appName("RetailGoldMonthlySales")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 1. READ SILVER
# ============================================================

silver_df = spark.read.parquet(
    "data/silver/retail_transactions"
)

print("\n=== SILVER DATA ===")
print("Rows:", silver_df.count())


# ============================================================
# 2. KEEP NORMAL SALES
# ============================================================

sales_df = silver_df.filter(
    silver_df.transaction_type == "SALE"
)

print("\n=== SALE TRANSACTIONS ===")
print("Rows:", sales_df.count())


# ============================================================
# 3. CREATE MONTHLY AGGREGATION
# ============================================================

monthly_sales = (
    sales_df
    .groupBy(
        year("invoicedate").alias("year"),
        month("invoicedate").alias("month")
    )
    .agg(
        count("*").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        round(
            sum("revenue"),
            2
        ).alias("total_revenue")
    )
    .orderBy(
        "year",
        "month"
    )
)


# ============================================================
# 4. DISPLAY RESULT
# ============================================================

print("\n=== MONTHLY SALES ===")

monthly_sales.show(
    30,
    truncate=False
)


# ============================================================
# 5. WRITE GOLD DATA
# ============================================================

output_path = "data/gold/monthly_sales"

monthly_sales.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== GOLD DATASET CREATED ===")
print("Output:", output_path)


spark.stop()