from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailAnalytics")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)
print("Spark application:", spark.sparkContext.appName)

spark.stop()