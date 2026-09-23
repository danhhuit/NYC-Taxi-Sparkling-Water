from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Load-Data")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


YELLOW_PATH = "data/raw/yellow/yellow_tripdata_2025-01.parquet"
ZONE_PATH = "data/raw/zones/taxi_zone_lookup.csv"


print("=" * 70)
print("LOAD NYC TLC DATA")
print("=" * 70)


# =============================
# 1. LOAD YELLOW TAXI PARQUET
# =============================

yellow_df = spark.read.parquet(YELLOW_PATH)

print("\nYELLOW TAXI SCHEMA:")
yellow_df.printSchema()

yellow_count = yellow_df.count()

print("\nYELLOW TAXI ROW COUNT:")
print(yellow_count)

print("\nYELLOW TAXI SAMPLE:")
yellow_df.show(5, truncate=False)


# =============================
# 2. LOAD TAXI ZONE CSV
# =============================

zone_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ZONE_PATH)
)

print("\nTAXI ZONE SCHEMA:")
zone_df.printSchema()

print("\nTAXI ZONE ROW COUNT:")
print(zone_df.count())

print("\nTAXI ZONE SAMPLE:")
zone_df.show(10, truncate=False)


print("=" * 70)
print("DATA LOAD SUCCESS")
print("=" * 70)

spark.stop()