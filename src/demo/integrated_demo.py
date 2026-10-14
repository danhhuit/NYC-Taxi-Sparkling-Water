import os
import csv
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    min as spark_min,
    max as spark_max,
)

from pysparkling.ml import H2OMOJOModel


# ==========================================================
# FIX DUPLICATE SPARKLING WATER JAR ON WINDOWS
# ==========================================================

submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")

print("PYSPARK_SUBMIT_ARGS BEFORE FIX =", submit_args)

prefix = "--jars "
suffix = " pyspark-shell"

if submit_args.startswith(prefix) and submit_args.endswith(suffix):

    raw_jars = submit_args[len(prefix):-len(suffix)]

    jar_paths = [
        p.strip()
        for p in raw_jars.split(",")
        if p.strip()
    ]

    fixed_jars = []
    seen = set()

    for jar_path in jar_paths:

        if jar_path.startswith("file:"):
            jar_uri = jar_path
        else:
            jar_uri = Path(jar_path).resolve().as_uri()

        if jar_uri not in seen:
            seen.add(jar_uri)
            fixed_jars.append(jar_uri)

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--jars "
        + ",".join(fixed_jars)
        + " pyspark-shell"
    )

print(
    "PYSPARK_SUBMIT_ARGS AFTER FIX =",
    os.environ.get("PYSPARK_SUBMIT_ARGS")
)


# ==========================================================
# HEADER
# ==========================================================

print()
print("=" * 75)
print("NYC TAXI - SPARKLING WATER INTEGRATED DEMO")
print("=" * 75)


# ==========================================================
# 1. CREATE SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Integrated-Demo")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print()
print("[1/5] APACHE SPARK STARTED")
print("Spark Version:", spark.version)


# ==========================================================
# 2. LOAD PROCESSED DATA
# ==========================================================

data_path = "data/processed/taxi_ml_ready.parquet"

taxi_df = spark.read.parquet(data_path)

taxi_count = taxi_df.count()

print()
print("[2/5] PROCESSED DATA LOADED")
print("Dataset:", data_path)
print("Total Rows:", taxi_count)
print("Total Columns:", len(taxi_df.columns))

taxi_df.show(5, truncate=False)


# ==========================================================
# 3. SHOW MODEL COMPARISON
# ==========================================================

metrics_path = "outputs/metrics/model_comparison.csv"

print()
print("[3/5] MODEL COMPARISON")
print()

if os.path.exists(metrics_path):

    with open(metrics_path, "r", encoding="utf-8-sig") as file:

        reader = csv.DictReader(file)

        rows = list(reader)

        for row in rows:
            print(
                f"{row['model']:<20}"
                f" RMSE={float(row['rmse']):.4f}"
                f" MAE={float(row['mae']):.4f}"
                f" R2={float(row['r2']):.4f}"
            )

else:
    print("Model comparison file not found.")


# ==========================================================
# 4. LOAD MOJO + BATCH PREDICTION
# ==========================================================

batch_path = "data/processed/taxi_batch_input.parquet"

batch_df = spark.read.parquet(batch_path)

batch_count = batch_df.count()

print()
print("[4/5] H2O MOJO BATCH SCORING")
print("Batch Input Rows:", batch_count)

model_path = Path(
    "models/nyc_taxi_gbm_mojo"
).resolve().as_uri()

print("MOJO Model:", model_path)

model = (
    H2OMOJOModel
    .read()
    .load(model_path)
)

print("MOJO MODEL LOADED SUCCESSFULLY")

start_time = time.time()

predictions = model.transform(batch_df)

prediction_count = predictions.count()

elapsed = time.time() - start_time

print("Prediction Rows:", prediction_count)
print(f"Scoring Time: {elapsed:.3f} seconds")


# ==========================================================
# 5. PREDICTION SUMMARY
# ==========================================================

summary = (
    predictions
    .select("prediction")
    .agg(
        avg("prediction").alias("avg_prediction_minutes"),
        spark_min("prediction").alias("min_prediction_minutes"),
        spark_max("prediction").alias("max_prediction_minutes"),
    )
)

print()
print("[5/5] PREDICTION SUMMARY")

summary.show(truncate=False)

print("Prediction Sample:")

predictions.select(
    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "prediction",
).show(10, truncate=False)


# ==========================================================
# END
# ==========================================================

print()
print("=" * 75)
print("INTEGRATED DEMO SUCCESS")
print("=" * 75)

print()
print("DEMO PIPELINE:")
print(
    "Processed Taxi Data"
    " -> Apache Spark"
    " -> Model Comparison"
    " -> H2O MOJO"
    " -> Batch Prediction"
)

spark.stop()