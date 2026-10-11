from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailDataInspection")
    .master("local[*]")
    .getOrCreate()
)

file_path = "data/raw/parquet/year_2009_2010.parquet"

df = spark.read.parquet(file_path)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== ROW COUNT ===")
print(df.count())

print("\n=== FIRST 5 ROWS ===")
df.show(5, truncate=False)

print("\n=== COLUMNS ===")
print(df.columns)

spark.stop()