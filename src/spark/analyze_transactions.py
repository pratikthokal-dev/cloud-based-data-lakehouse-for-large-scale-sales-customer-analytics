from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


spark = (
    SparkSession.builder
    .appName("RetailTransactionAnalysis")
    .master("local[*]")
    .getOrCreate()
)

# Read both years
df = spark.read.parquet(
    "data/bronze/year_2009_2010.parquet",
    "data/bronze/year_2010_2011.parquet"
)

print("\n=== TOTAL ROWS ===")
print(df.count())


# 1. Negative quantities
negative_quantity = df.filter(
    col("quantity") < 0
)

print("\n=== NEGATIVE QUANTITY ===")
print("Rows:", negative_quantity.count())


# 2. Zero quantities
zero_quantity = df.filter(
    col("quantity") == 0
)

print("\n=== ZERO QUANTITY ===")
print("Rows:", zero_quantity.count())


# 3. Negative prices
negative_price = df.filter(
    col("price") < 0
)

print("\n=== NEGATIVE PRICE ===")
print("Rows:", negative_price.count())


# 4. Zero prices
zero_price = df.filter(
    col("price") == 0
)

print("\n=== ZERO PRICE ===")
print("Rows:", zero_price.count())


# 5. Cancellation invoices
cancellations = df.filter(
    col("invoice").startswith("C")
)

print("\n=== CANCELLATION INVOICES ===")
print("Rows:", cancellations.count())


# 6. Show examples of negative quantities
print("\n=== SAMPLE NEGATIVE QUANTITY TRANSACTIONS ===")

negative_quantity.select(
    "invoice",
    "stockcode",
    "description",
    "quantity",
    "invoicedate",
    "price",
    "customer_id",
    "country"
).show(10, truncate=False)


spark.stop()