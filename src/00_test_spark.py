from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Spark-Test")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("NYC TAXI - SPARK ENVIRONMENT TEST")
print("=" * 60)

print("Spark version:", spark.version)
print("Application:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)

df = spark.range(1, 11)

print("Number of rows:", df.count())

df.show()

spark.stop()

print("SPARK TEST SUCCESS")