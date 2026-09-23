import os
import json
from pathlib import Path

import pysparkling

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

from pysparkling import H2OContext, H2OConf
from pysparkling.ml import H2ODeepLearningRegressor


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
    .appName("NYC-Taxi-H2O-DeepLearning")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("NYC TAXI - H2O DEEP LEARNING REGRESSION")
print("=" * 70)


# ==========================================================
# 2. H2O CONTEXT
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


# ==========================================================
# 3. LOAD DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_ml_ready.parquet"
)


print("\nTOTAL ROWS:")
print(df.count())


# ==========================================================
# 4. FEATURES
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
# 5. SAME SPLIT AS GBM / DRF
# ==========================================================

train_df, valid_df, test_df = df.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)

train_df = train_df.cache()
valid_df = valid_df.cache()
test_df = test_df.cache()


train_count = train_df.count()
valid_count = valid_df.count()
test_count = test_df.count()


print("\nDATA SPLIT:")
print("Train      :", train_count)
print("Validation :", valid_count)
print("Test       :", test_count)
print(
    "Total      :",
    train_count + valid_count + test_count
)


# ==========================================================
# 6. DEEP LEARNING MODEL
# ==========================================================

dl = H2ODeepLearningRegressor(

    featuresCols=features,

    labelCol=label,

    columnsToCategorical=[
        "pickup_location",
        "dropoff_location"
    ],

    convertUnknownCategoricalLevelsToNa=True,

    distribution="gaussian",

    activation="Rectifier",

    hidden=[
        64,
        32
    ],

    epochs=20.0,

    standardize=True,

    adaptiveRate=True,

    validationDataFrame=valid_df,

    stoppingMetric="RMSE",

    stoppingRounds=5,

    scoreEachIteration=True,

    seed=42,

    predictionCol="prediction",

    modelId="nyc_taxi_deep_learning"
)


print("\nTRAINING H2O DEEP LEARNING...")
print("Architecture: 64 -> 32")
print("Maximum epochs: 20")


# ==========================================================
# 7. TRAIN
# ==========================================================

model = dl.fit(
    train_df
)


print("\nDEEP LEARNING MODEL TRAINED SUCCESSFULLY")


# ==========================================================
# 8. PREDICTION
# ==========================================================

predictions = model.transform(
    test_df
)


print("\nPREDICTION SAMPLE:")

predictions.select(
    label,
    "prediction",
    "pickup_location",
    "dropoff_location",
    "pickup_hour"
).show(
    20,
    truncate=False
)


# ==========================================================
# 9. EVALUATION
# ==========================================================

rmse_evaluator = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="r2"
)


rmse = rmse_evaluator.evaluate(
    predictions
)

mae = mae_evaluator.evaluate(
    predictions
)

r2 = r2_evaluator.evaluate(
    predictions
)


print("\n" + "=" * 70)
print("DEEP LEARNING TEST METRICS")
print("=" * 70)

print(
    f"RMSE : {rmse:.4f} minutes"
)

print(
    f"MAE  : {mae:.4f} minutes"
)

print(
    f"R2   : {r2:.4f}"
)

print("=" * 70)


# ==========================================================
# 10. SAVE METRICS
# ==========================================================

os.makedirs(
    "outputs/metrics",
    exist_ok=True
)

metrics = {

    "model":
        "H2O Deep Learning",

    "architecture":
        [64, 32],

    "epochs":
        20,

    "total_rows":
        train_count
        + valid_count
        + test_count,

    "train_rows":
        train_count,

    "validation_rows":
        valid_count,

    "test_rows":
        test_count,

    "features":
        features,

    "rmse":
        rmse,

    "mae":
        mae,

    "r2":
        r2
}


with open(
    "outputs/metrics/deep_learning_metrics.json",
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        metrics,
        f,
        indent=4,
        ensure_ascii=False
    )


# ==========================================================
# 11. SAVE PREDICTIONS
# ==========================================================

predictions.select(

    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",

    label,

    "prediction"

).write.mode(
    "overwrite"
).parquet(
    "outputs/predictions/deep_learning"
)


print("\nMETRICS SAVED:")
print(
    "outputs/metrics/"
    "deep_learning_metrics.json"
)

print("\nPREDICTIONS SAVED:")
print(
    "outputs/predictions/"
    "deep_learning"
)


print("\n" + "=" * 70)
print("H2O DEEP LEARNING SUCCESS")
print("=" * 70)


train_df.unpersist()
valid_df.unpersist()
test_df.unpersist()

spark.stop()