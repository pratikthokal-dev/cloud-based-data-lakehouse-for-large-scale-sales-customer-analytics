from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("RetailSilverValidation")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 1. READ SILVER DATA
# ============================================================

silver_df = spark.read.parquet(
    "data/silver/retail_transactions"
)

print("\n=== SILVER DATASET ===")

row_count = silver_df.count()

print("Rows:", row_count)
print("Columns:", len(silver_df.columns))


# ============================================================
# 2. SCHEMA
# ============================================================

print("\n=== SILVER SCHEMA ===")

silver_df.printSchema()


# ============================================================
# 3. SAMPLE DATA
# ============================================================

print("\n=== SAMPLE SILVER DATA ===")

silver_df.show(
    10,
    truncate=False
)


# ============================================================
# 4. REQUIRED NULL VALIDATION
# ============================================================

print("\n=== REQUIRED COLUMN NULLS ===")

required_columns = [
    "invoice",
    "stockcode",
    "quantity",
    "invoicedate",
    "price"
]

for column in required_columns:

    null_count = silver_df.filter(
        col(column).isNull()
    ).count()

    print(f"{column}: {null_count}")


# ============================================================
# 5. EXACT DUPLICATE VALIDATION
# ============================================================

print("\n=== DUPLICATE VALIDATION ===")

duplicate_groups = (
    silver_df
    .groupBy(silver_df.columns)
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    "Exact duplicate groups remaining:",
    duplicate_groups
)


# ============================================================
# 6. TRANSACTION TYPE VALIDATION
# ============================================================

print("\n=== TRANSACTION TYPES ===")

silver_df.groupBy(
    "transaction_type"
).count().show()


# ============================================================
# 7. REVENUE VALIDATION
# ============================================================

print("\n=== REVENUE VALIDATION ===")

invalid_revenue = silver_df.filter(
    col("revenue").isNull()
).count()

print(
    "NULL revenue rows:",
    invalid_revenue
)


# ============================================================
# 8. NEGATIVE PRICE VALIDATION
# ============================================================

print("\n=== NEGATIVE PRICE ===")

negative_price_count = silver_df.filter(
    col("price") < 0
).count()

print(
    "Negative price rows:",
    negative_price_count
)


# ============================================================
# 9. FINAL VALIDATION SUMMARY
# ============================================================

print("\n=== SILVER VALIDATION COMPLETE ===")

print("Rows:", row_count)
print("Columns:", len(silver_df.columns))
print("Duplicate groups:", duplicate_groups)
print("NULL revenue:", invalid_revenue)
print("Negative price rows:", negative_price_count)


spark.stop()