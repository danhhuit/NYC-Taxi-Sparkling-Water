import os
from pathlib import Path

import pysparkling
from pyspark.sql import SparkSession
from pysparkling import H2OContext, H2OConf


# ==========================================================
# WINDOWS FIX FOR PYSPARKLING JAR PATH
# ==========================================================

submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")

prefix = "--jars "
suffix = " pyspark-shell"

if submit_args.startswith(prefix) and submit_args.endswith(suffix):
    jar_path = submit_args[len(prefix):-len(suffix)]
    jar_uri = Path(jar_path).as_uri()

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar_uri} pyspark-shell"
    )

print(
    "PYSPARK_SUBMIT_ARGS =",
    os.environ.get("PYSPARK_SUBMIT_ARGS")
)


# ==========================================================
# 1. CREATE SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-H2OFrame-Conversion")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("SPARK DATAFRAME <-> H2OFRAME")
print("=" * 70)


# ==========================================================
# 2. START H2O INTERNAL BACKEND
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


# ==========================================================
# 3. LOAD PROCESSED DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_features_multisource.parquet"
)


# ==========================================================
# 4. SELECT ML-FRIENDLY COLUMNS
# ==========================================================

demo_df = (
    df.select(
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
        "route_trip_count",
        "route_avg_duration",
        "priority_score",
        "trip_duration_minutes"
    )
    .limit(100000)
)


print("\nSPARK DATAFRAME ROW COUNT:")
print(demo_df.count())


print("\nSPARK DATAFRAME SCHEMA:")
demo_df.printSchema()


print("\nSPARK DATAFRAME SAMPLE:")
demo_df.show(
    10,
    truncate=False
)


# ==========================================================
# 5. SPARK DATAFRAME -> H2OFRAME
# ==========================================================

h2o_frame = hc.asH2OFrame(
    demo_df,
    "nyc_taxi_h2o_frame"
)


print("\nH2OFRAME CREATED SUCCESSFULLY")

print("\nH2OFRAME ROWS:")
print(h2o_frame.nrows)

print("\nH2OFRAME COLUMNS:")
print(h2o_frame.ncols)

print("\nH2OFRAME COLUMN NAMES:")
print(h2o_frame.names)


# ==========================================================
# 6. H2OFRAME -> SPARK DATAFRAME
# ==========================================================

spark_df_back = hc.asSparkFrame(
    h2o_frame
)


print("\nCONVERTED BACK TO SPARK DATAFRAME")


print("\nROUND-TRIP ROW COUNT:")
print(spark_df_back.count())


print("\nROUND-TRIP SCHEMA:")
spark_df_back.printSchema()


print("\nROUND-TRIP SAMPLE:")
spark_df_back.show(
    10,
    truncate=False
)


print("=" * 70)
print("SPARK <-> H2OFRAME SUCCESS")
print("=" * 70)


spark.stop()