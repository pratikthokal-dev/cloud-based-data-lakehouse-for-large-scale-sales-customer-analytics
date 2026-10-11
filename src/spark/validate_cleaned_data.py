from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


spark = (
    SparkSession.builder
    .appName("RetailCleanedDataValidation")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    "data/raw/parquet/year_2009_2010.parquet",
    "data/raw/parquet/year_2010_2011.parquet"
)


print("\n=== ORIGINAL DATA ===")
original_count = df.count()
print("Rows:", original_count)


# Remove exact duplicate rows
clean_df = df.dropDuplicates()


print("\n=== CLEANED DATA ===")
cleaned_count = clean_df.count()
print("Rows:", cleaned_count)


# --------------------------------------------------
# 1. Validate exact duplicates
# --------------------------------------------------

duplicate_count = (
    clean_df.groupBy(clean_df.columns)
    .count()
    .filter(col("count") > 1)
    .count()
)

print("\n=== EXACT DUPLICATE VALIDATION ===")
print("Duplicate groups remaining:", duplicate_count)


# --------------------------------------------------
# 2. Validate required NULL values
# --------------------------------------------------

print("\n=== REQUIRED COLUMN NULL VALIDATION ===")

required_columns = [
    "invoice",
    "stockcode",
    "quantity",
    "invoicedate",
    "price"
]

for column in required_columns:

    null_count = clean_df.filter(
        col(column).isNull()
    ).count()

    print(f"{column}: {null_count}")


# --------------------------------------------------
# 3. Check invalid quantities
# --------------------------------------------------

invalid_quantity_count = clean_df.filter(
    col("quantity") == 0
).count()

print("\n=== ZERO QUANTITY ===")
print("Rows:", invalid_quantity_count)


# --------------------------------------------------
# 4. Check invalid prices
# --------------------------------------------------

invalid_price_count = clean_df.filter(
    col("price") < 0
).count()

print("\n=== NEGATIVE PRICE ===")
print("Rows:", invalid_price_count)


# --------------------------------------------------
# 5. Final row-count validation
# --------------------------------------------------

print("\n=== FINAL VALIDATION ===")

print("Original rows:", original_count)
print("Cleaned rows:", cleaned_count)
print("Rows removed:", original_count - cleaned_count)


spark.stop()