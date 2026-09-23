from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit


spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Prepare-ML")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


INPUT_PATH = (
    "data/processed/"
    "taxi_features_multisource.parquet"
)

OUTPUT_PATH = (
    "data/processed/"
    "taxi_ml_ready.parquet"
)


print("=" * 70)
print("PREPARE LEAKAGE-SAFE ML DATA")
print("=" * 70)


df = spark.read.parquet(INPUT_PATH)


# ==========================================================
# Chỉ dùng feature biết được trước / lúc pickup
# ==========================================================

ml_df = (
    df.select(
        col("PULocationID")
            .cast("string")
            .alias("pickup_location"),

        col("DOLocationID")
            .cast("string")
            .alias("dropoff_location"),

        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "priority_score",
        "trip_duration_minutes"
    )

    # passenger_count đôi khi null
    .withColumn(
        "passenger_count",
        coalesce(
            col("passenger_count"),
            lit(1)
        )
    )

    # Zone không có metadata PostgreSQL => priority 0
    .withColumn(
        "priority_score",
        coalesce(
            col("priority_score"),
            lit(0)
        )
    )
)


print("\nML DATA SCHEMA:")
ml_df.printSchema()


print("\nML ROW COUNT:")
print(ml_df.count())


print("\nNULL CHECK:")

for c in ml_df.columns:
    null_count = (
        ml_df
        .filter(col(c).isNull())
        .count()
    )

    print(
        f"{c}: {null_count}"
    )


print("\nML SAMPLE:")

ml_df.show(
    10,
    truncate=False
)


ml_df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_PATH
)


print("\nOUTPUT:")
print(OUTPUT_PATH)

print("=" * 70)
print("ML DATA PREPARATION SUCCESS")
print("=" * 70)


spark.stop()