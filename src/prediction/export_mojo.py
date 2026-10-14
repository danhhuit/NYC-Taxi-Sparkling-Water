import os
from pathlib import Path

import pysparkling

from pyspark.sql import SparkSession

from pysparkling import H2OContext, H2OConf
from pysparkling.ml import H2OGBMRegressor


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
# SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName(
        "NYC-Taxi-Export-MOJO"
    )
    .master("local[*]")
    .config(
        "spark.driver.memory",
        "2g"
    )
    .config(
        "spark.executor.memory",
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
print("NYC TAXI - EXPORT FINAL GBM MOJO")
print("=" * 70)


# ==========================================================
# H2O CONTEXT
# ==========================================================

conf = (
    H2OConf()
    .setInternalClusterMode()
)

hc = H2OContext.getOrCreate(
    conf
)


# ==========================================================
# LOAD ML DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/"
    "taxi_ml_ready.parquet"
)


print(
    "\nTOTAL ROWS:",
    df.count()
)


# ==========================================================
# SAME SPLIT
# ==========================================================

train_df, valid_df, test_df = (
    df.randomSplit(
        [0.70, 0.15, 0.15],
        seed=42
    )
)


# Hyperparameters already selected using validation,
# therefore final deployment model uses train + validation.

train_final = (
    train_df
    .unionByName(
        valid_df
    )
    .cache()
)


train_count = (
    train_final.count()
)


print(
    "\nFINAL TRAINING ROWS:",
    train_count
)


# ==========================================================
# FEATURES
# ==========================================================

features = [
    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend"
]

label = (
    "trip_duration_minutes"
)


# ==========================================================
# FINAL GBM
# ==========================================================

gbm = H2OGBMRegressor(

    featuresCols=features,

    labelCol=label,

    columnsToCategorical=[
        "pickup_location",
        "dropoff_location"
    ],

    convertUnknownCategoricalLevelsToNa=True,

    distribution="gaussian",

    ntrees=150,

    maxDepth=8,

    learnRate=0.05,

    sampleRate=0.8,

    colSampleRate=0.8,

    seed=42,

    predictionCol="prediction",

    modelId="nyc_taxi_gbm_deployment"
)


print(
    "\nTRAINING DEPLOYMENT MODEL..."
)

print(
    "ntrees = 150"
)

print(
    "maxDepth = 8"
)

print(
    "learnRate = 0.05"
)


# ==========================================================
# TRAIN
# ==========================================================

model = gbm.fit(
    train_final
)


print(
    "\nFINAL GBM TRAINED SUCCESSFULLY"
)


# ==========================================================
# EXPORT SPARKLING WATER MOJO MODEL
# ==========================================================

model_path = (
    "models/nyc_taxi_gbm_mojo"
)


print(
    "\nEXPORTING MODEL..."
)

print(
    model_path
)


model.write().overwrite().save(
    model_path
)


print(
    "\nMODEL SAVED:"
)

print(
    model_path
)


# ==========================================================
# CHECK DIRECTORY
# ==========================================================

if os.path.exists(
    model_path
):

    print(
        "\nMODEL DIRECTORY EXISTS: YES"
    )

else:

    print(
        "\nMODEL DIRECTORY EXISTS: NO"
    )


print(
    "\n"
    + "=" * 70
)

print(
    "MOJO EXPORT SUCCESS"
)

print(
    "=" * 70
)


train_final.unpersist()

spark.stop()