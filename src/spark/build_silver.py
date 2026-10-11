from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

spark = (
    SparkSession.builder
    .appName("RetailSilverLayer")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 1. READ BRONZE DATA
# ============================================================

df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

print("\n=== BRONZE DATA ===")
print("Rows:", df.count())


# ============================================================
# 2. REMOVE EXACT DUPLICATES
# ============================================================

clean_df = df.dropDuplicates()

print("\n=== AFTER DEDUPLICATION ===")
print("Rows:", clean_df.count())


# ============================================================
# 3. REMOVE ROWS MISSING ESSENTIAL FIELDS
# ============================================================

clean_df = clean_df.dropna(
    subset=[
        "invoice",
        "stockcode",
        "quantity",
        "invoicedate",
        "price"
    ]
)

print("\n=== AFTER NULL CLEANING ===")
print("Rows:", clean_df.count())


# ============================================================
# 4. CLASSIFY TRANSACTIONS
# ============================================================

clean_df = clean_df.withColumn(
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


# ============================================================
# 5. CALCULATE REVENUE
# ============================================================

clean_df = clean_df.withColumn(
    "revenue",
    round(
        col("quantity") * col("price"),
        2
    )
)


# ============================================================
# 6. SELECT SILVER COLUMNS
# ============================================================

silver_df = clean_df.select(
    "invoice",
    "stockcode",
    "description",
    "quantity",
    "invoicedate",
    "price",
    "customer_id",
    "country",
    "transaction_type",
    "revenue"
)


# ============================================================
# 7. VALIDATION
# ============================================================

print("\n=== SILVER TRANSACTION TYPES ===")

silver_df.groupBy(
    "transaction_type"
).count().show()


print("\n=== SILVER SAMPLE ===")

silver_df.show(
    10,
    truncate=False
)


# ============================================================
# 8. WRITE SILVER DATA
# ============================================================

output_path = "data/silver/retail_transactions"

silver_df.write.mode(
    "overwrite"
).parquet(
    output_path
)

print("\n=== SILVER LAYER CREATED ===")
print("Output:", output_path)


spark.stop()