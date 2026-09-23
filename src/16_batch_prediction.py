import os
from pathlib import Path

import pysparkling

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
            jar_uri = Path(
                jar_path
            ).resolve().as_uri()

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
# 1. SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Batch-Prediction")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("NYC TAXI - BATCH PREDICTION")
print("=" * 70)


# ==========================================================
# 2. H2O CONTEXT
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


# ==========================================================
# 3. LOAD ML DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_ml_ready.parquet"
)


# ==========================================================
# 4. RECREATE SAME SPLIT
# ==========================================================

train_df, valid_df, test_df = df.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)


# ==========================================================
# 5. FINAL TRAINING DATA
#
# Hyperparameters were selected using validation data.
# For deployment, retrain final GBM using train + validation.
# Test/batch data stays outside training.
# ==========================================================

train_final = (
    train_df
    .unionByName(valid_df)
    .cache()
)


train_count = train_final.count()


print("\nFINAL TRAINING ROWS:")
print(train_count)


# ==========================================================
# 6. LOAD BATCH INPUT
# ==========================================================

batch_df = spark.read.parquet(
    "data/processed/"
    "taxi_batch_input.parquet"
)


batch_count = batch_df.count()


print("\nBATCH INPUT ROWS:")
print(batch_count)


print("\nBATCH INPUT SAMPLE:")

batch_df.show(
    10,
    truncate=False
)


# ==========================================================
# 7. FEATURES
# ==========================================================

features = [
    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend"
]

label = "trip_duration_minutes"


# ==========================================================
# 8. FINAL GBM MODEL
#
# Best Grid Search:
# maxDepth = 8
# learnRate = 0.05
# ntrees = 150
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

    modelId="nyc_taxi_gbm_final_batch"
)


print("\nTRAINING FINAL GBM...")
print("maxDepth = 8")
print("learnRate = 0.05")
print("ntrees = 150")


# ==========================================================
# 9. TRAIN
# ==========================================================

model = gbm.fit(
    train_final
)


print("\nFINAL GBM TRAINED SUCCESSFULLY")


# ==========================================================
# 10. BATCH PREDICTION
# ==========================================================

predictions = model.transform(
    batch_df
)


prediction_count = predictions.count()


print("\nPREDICTION ROW COUNT:")
print(prediction_count)


print("\nBATCH PREDICTION SAMPLE:")

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
# 11. PREDICTION SUMMARY
# ==========================================================

print("\nPREDICTION SUMMARY:")

summary = predictions.agg(

    F.count("*").alias(
        "total_predictions"
    ),

    F.avg("prediction").alias(
        "avg_prediction_minutes"
    ),

    F.min("prediction").alias(
        "min_prediction_minutes"
    ),

    F.max("prediction").alias(
        "max_prediction_minutes"
    )

)

summary.show(
    truncate=False
)


# ==========================================================
# 12. SAVE BATCH PREDICTIONS
# ==========================================================

output_path = (
    "outputs/predictions/"
    "batch_predictions"
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


# ==========================================================
# 13. SAVE SMALL CSV SAMPLE FOR DEMO
# ==========================================================

csv_output = (
    "outputs/predictions/"
    "batch_predictions_sample_csv"
)


predictions.select(

    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "prediction"

).limit(
    100
).coalesce(
    1
).write.mode(
    "overwrite"
).option(
    "header",
    True
).csv(
    csv_output
)


print("\nOUTPUT SAVED:")

print(
    output_path
)

print(
    csv_output
)


print("\n" + "=" * 70)
print("BATCH PREDICTION SUCCESS")
print("=" * 70)


train_final.unpersist()

spark.stop()