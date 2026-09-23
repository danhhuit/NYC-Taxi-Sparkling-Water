from pyspark.sql import SparkSession


# ==========================================================
# 1. SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Prepare-Batch")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("NYC TAXI - PREPARE BATCH INPUT")
print("=" * 70)


# ==========================================================
# 2. LOAD ML DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_ml_ready.parquet"
)


print("\nTOTAL SOURCE ROWS:")
print(df.count())


# ==========================================================
# 3. RECREATE SAME SPLIT
# ==========================================================

train_df, valid_df, test_df = df.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)


print("\nTEST ROWS:")
print(test_df.count())


# ==========================================================
# 4. CREATE DEMO BATCH
# ==========================================================

batch_df = (
    test_df
    .select(
        "pickup_location",
        "dropoff_location",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    )
    .limit(10000)
)


batch_count = batch_df.count()


print("\nBATCH ROW COUNT:")
print(batch_count)


print("\nBATCH SAMPLE:")

batch_df.show(
    20,
    truncate=False
)


# ==========================================================
# 5. SAVE BATCH
# ==========================================================

output_path = (
    "data/processed/"
    "taxi_batch_input.parquet"
)


batch_df.write.mode(
    "overwrite"
).parquet(
    output_path
)


print("\nBATCH INPUT SAVED:")
print(output_path)


print("\nBATCH SCHEMA:")

batch_df.printSchema()


print("=" * 70)
print("BATCH INPUT PREPARATION SUCCESS")
print("=" * 70)


spark.stop()