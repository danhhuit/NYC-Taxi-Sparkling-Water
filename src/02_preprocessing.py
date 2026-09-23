from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    unix_timestamp,
    hour,
    dayofweek,
    month,
    when,
    count,
    avg
)


spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Preprocessing")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


YELLOW_PATH = "data/raw/yellow/yellow_tripdata_2025-01.parquet"
ZONE_PATH = "data/raw/zones/taxi_zone_lookup.csv"

OUTPUT_FULL = "data/processed/taxi_features_full.parquet"
OUTPUT_DEMO = "data/processed/taxi_features_demo.parquet"


print("=" * 70)
print("NYC TAXI - PREPROCESSING")
print("=" * 70)


# ==========================================================
# 1. LOAD DATA
# ==========================================================

taxi_df = spark.read.parquet(YELLOW_PATH)

zone_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ZONE_PATH)
)

raw_count = taxi_df.count()

print("\nRAW ROW COUNT:")
print(raw_count)


# ==========================================================
# 2. CREATE TARGET: TRIP DURATION
# ==========================================================

taxi_df = taxi_df.withColumn(
    "trip_duration_minutes",
    (
        unix_timestamp("tpep_dropoff_datetime")
        - unix_timestamp("tpep_pickup_datetime")
    ) / 60.0
)


# ==========================================================
# 3. CREATE TIME FEATURES
# ==========================================================

taxi_df = (
    taxi_df
    .withColumn(
        "pickup_hour",
        hour("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_day_of_week",
        dayofweek("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_month",
        month("tpep_pickup_datetime")
    )
    .withColumn(
        "is_weekend",
        when(
            col("pickup_day_of_week").isin([1, 7]),
            1
        ).otherwise(0)
    )
)


# ==========================================================
# 4. CLEAN DATA
# ==========================================================

clean_df = taxi_df.filter(
    col("tpep_pickup_datetime").isNotNull()
    & col("tpep_dropoff_datetime").isNotNull()
    & col("PULocationID").isNotNull()
    & col("DOLocationID").isNotNull()

    # chuyến đi phải có thời gian hợp lệ
    & (col("trip_duration_minutes") >= 1)
    & (col("trip_duration_minutes") <= 180)

    # giới hạn dữ liệu đúng tháng 01/2025
    & (col("pickup_month") == 1)

    # passenger count hợp lý
    & (
        col("passenger_count").isNull()
        | (
            (col("passenger_count") >= 1)
            & (col("passenger_count") <= 6)
        )
    )
)


clean_count = clean_df.count()

print("\nCLEAN ROW COUNT:")
print(clean_count)

print("\nREMOVED ROWS:")
print(raw_count - clean_count)


# ==========================================================
# 5. PREPARE TAXI ZONE DATA
# ==========================================================

pickup_zone_df = zone_df.select(
    col("LocationID").alias("PULocationID"),
    col("Borough").alias("pickup_borough"),
    col("Zone").alias("pickup_zone")
)

dropoff_zone_df = zone_df.select(
    col("LocationID").alias("DOLocationID"),
    col("Borough").alias("dropoff_borough"),
    col("Zone").alias("dropoff_zone")
)


# ==========================================================
# 6. JOIN YELLOW TAXI + TAXI ZONE LOOKUP
# ==========================================================

joined_df = (
    clean_df
    .join(
        pickup_zone_df,
        on="PULocationID",
        how="left"
    )
    .join(
        dropoff_zone_df,
        on="DOLocationID",
        how="left"
    )
)


print("\nJOINED SAMPLE:")

joined_df.select(
    "tpep_pickup_datetime",
    "PULocationID",
    "pickup_borough",
    "pickup_zone",
    "DOLocationID",
    "dropoff_borough",
    "dropoff_zone",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "trip_duration_minutes"
).show(10, truncate=False)


# ==========================================================
# 7. SPARK SQL
# ==========================================================

joined_df.createOrReplaceTempView("taxi_trips")


print("\nTOP PICKUP BOROUGHS:")

spark.sql("""
    SELECT
        pickup_borough,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
    FROM taxi_trips
    WHERE pickup_borough IS NOT NULL
    GROUP BY pickup_borough
    ORDER BY total_trips DESC
""").show(truncate=False)


print("\nAVERAGE TRIP DURATION BY PICKUP HOUR:")

spark.sql("""
    SELECT
        pickup_hour,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
    FROM taxi_trips
    GROUP BY pickup_hour
    ORDER BY pickup_hour
""").show(24, truncate=False)


# ==========================================================
# 8. ROUTE STATISTICS
# ==========================================================

route_stats_df = (
    joined_df
    .groupBy(
        "PULocationID",
        "DOLocationID"
    )
    .agg(
        count("*").alias("route_trip_count"),
        avg("trip_duration_minutes").alias(
            "route_avg_duration"
        )
    )
)


print("\nTOP ROUTES:")

route_stats_df.orderBy(
    col("route_trip_count").desc()
).show(10, truncate=False)


# ==========================================================
# 9. JOIN ROUTE STATISTICS
# ==========================================================

feature_df = joined_df.join(
    route_stats_df,
    on=["PULocationID", "DOLocationID"],
    how="left"
)


# ==========================================================
# 10. SELECT USEFUL COLUMNS
# ==========================================================

feature_df = feature_df.select(
    "PULocationID",
    "DOLocationID",

    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",

    "passenger_count",

    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "is_weekend",

    "route_trip_count",
    "route_avg_duration",

    "trip_duration_minutes"
)


print("\nFINAL FEATURE SCHEMA:")
feature_df.printSchema()


print("\nFINAL FEATURE SAMPLE:")
feature_df.show(10, truncate=False)


# ==========================================================
# 11. SAVE FULL DATASET
# ==========================================================

feature_df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_FULL
)


# ==========================================================
# 12. CREATE SMALL DEMO DATASET
# ==========================================================

demo_df = feature_df.limit(500000)

demo_df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_DEMO
)


print("\nFULL DATASET SAVED:")
print(OUTPUT_FULL)

print("\nDEMO DATASET SAVED:")
print(OUTPUT_DEMO)

print("\nDEMO ROW COUNT:")
print(demo_df.count())


print("=" * 70)
print("PREPROCESSING SUCCESS")
print("=" * 70)


spark.stop()