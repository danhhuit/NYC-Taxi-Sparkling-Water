import os
import time
from pathlib import Path

import pysparkling

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pysparkling.ml import H2OMOJOModel


# ==========================================================
# WINDOWS FIX - PYSPARKLING JARS
# ==========================================================

submit_args = os.environ.get(
    "PYSPARK_SUBMIT_ARGS",
    ""
)

print(
    "PYSPARK_SUBMIT_ARGS BEFORE FIX =",
    submit_args
)

prefix = "--jars "
suffix = " pyspark-shell"

if (
    submit_args.startswith(prefix)
    and submit_args.endswith(suffix)
):

    raw_jars = submit_args[
        len(prefix):-len(suffix)
    ]

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
            jar_uri = (
                Path(jar_path)
                .resolve()
                .as_uri()
            )

        if jar_uri not in seen:
            seen.add(jar_uri)
            fixed_jars.append(jar_uri)

    os.environ[
        "PYSPARK_SUBMIT_ARGS"
    ] = (
        "--jars "
        + ",".join(fixed_jars)
        + " pyspark-shell"
    )


print(
    "PYSPARK_SUBMIT_ARGS AFTER FIX =",
    os.environ.get(
        "PYSPARK_SUBMIT_ARGS"
    )
)


# ==========================================================
# SPARK ONLY
#
# IMPORTANT:
# No H2OContext is created here.
# This proves MOJO scoring can run without an H2O cluster.
# ==========================================================

spark = (
    SparkSession.builder
    .appName(
        "NYC-Taxi-MOJO-Prediction"
    )
    .master("local[*]")
    .config(
        "spark.driver.memory",
        "2g"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


print("=" * 70)
print("NYC TAXI - LOAD MOJO AND PREDICT")
print("=" * 70)


# ==========================================================
# LOAD BATCH INPUT
# ==========================================================

batch_df = spark.read.parquet(
    "data/processed/"
    "taxi_batch_input.parquet"
)


batch_count = batch_df.count()


print(
    "\nBATCH INPUT ROWS:",
    batch_count
)


# ==========================================================
# LOAD SAVED MOJO MODEL
# ==========================================================

model_path = Path(
    "models/nyc_taxi_gbm_mojo"
).resolve()


model_uri = model_path.as_uri()


print(
    "\nLOADING MOJO MODEL:"
)

print(
    model_uri
)


model = (
    H2OMOJOModel
    .read()
    .load(
        model_uri
    )
)


print(
    "\nMOJO MODEL LOADED SUCCESSFULLY"
)


# ==========================================================
# PREDICT
# ==========================================================

start_time = time.perf_counter()


predictions = model.transform(
    batch_df
)


# Spark is lazy, so count() forces actual scoring
prediction_count = (
    predictions.count()
)


elapsed = (
    time.perf_counter()
    - start_time
)


print(
    "\nPREDICTION ROW COUNT:",
    prediction_count
)


print(
    f"SCORING TIME: {elapsed:.3f} seconds"
)


# ==========================================================
# VALIDATE PREDICTIONS
# ==========================================================

null_predictions = (
    predictions
    .filter(
        F.col(
            "prediction"
        ).isNull()
    )
    .count()
)


print(
    "NULL PREDICTIONS:",
    null_predictions
)


# ==========================================================
# SAMPLE
# ==========================================================

print(
    "\nMOJO PREDICTION SAMPLE:"
)


predictions.select(

    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "prediction"

).show(
    20,
    truncate=False
)


# ==========================================================
# SUMMARY
# ==========================================================

print(
    "\nMOJO PREDICTION SUMMARY:"
)


predictions.agg(

    F.count("*").alias(
        "total_predictions"
    ),

    F.avg(
        "prediction"
    ).alias(
        "avg_prediction_minutes"
    ),

    F.min(
        "prediction"
    ).alias(
        "min_prediction_minutes"
    ),

    F.max(
        "prediction"
    ).alias(
        "max_prediction_minutes"
    )

).show(
    truncate=False
)


# ==========================================================
# SAVE RESULT
# ==========================================================

output_path = (
    "outputs/predictions/"
    "mojo_batch_predictions"
)


predictions.select(

    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "prediction"

).write.mode(
    "overwrite"
).parquet(
    output_path
)


print(
    "\nOUTPUT SAVED:"
)

print(
    output_path
)


print(
    "\nH2OContext USED: NO"
)


print(
    "\n" + "=" * 70
)

print(
    "MOJO LOAD AND PREDICTION SUCCESS"
)

print(
    "=" * 70
)


spark.stop()