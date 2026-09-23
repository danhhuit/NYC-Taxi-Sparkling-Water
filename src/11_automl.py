import os
import json
from pathlib import Path

import pysparkling

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

from pysparkling import H2OContext, H2OConf
from pysparkling.ml import H2OAutoMLRegressor


# ==========================================================
# WINDOWS FIX - PYSPARKLING JAR
# ==========================================================

submit_args = os.environ.get(
    "PYSPARK_SUBMIT_ARGS",
    ""
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
    "PYSPARK_SUBMIT_ARGS =",
    os.environ.get(
        "PYSPARK_SUBMIT_ARGS"
    )
)


# ==========================================================
# 1. SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-H2O-AutoML")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("NYC TAXI - H2O AUTOML")
print("=" * 70)


# ==========================================================
# 2. H2O
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


# ==========================================================
# 3. LOAD DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_ml_ready.parquet"
)


# ==========================================================
# 4. SAME SPLIT
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


# ==========================================================
# 5. FEATURES
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
# 6. AUTOML
# ==========================================================

automl = H2OAutoMLRegressor(

    featuresCols=features,

    labelCol=label,

    columnsToCategorical=[
        "pickup_location",
        "dropoff_location"
    ],

    convertUnknownCategoricalLevelsToNa=True,

    # Tránh XGBoost trên Windows
    excludeAlgos=[
        "XGBoost"
    ],

    # Demo giữa kỳ -> không train quá nhiều
    maxModels=6,

    # Dùng blending thay vì CV nặng
    nfolds=0,

    validationDataFrame=valid_df,

    blendingDataFrame=valid_df,

    leaderboardDataFrame=valid_df,

    sortMetric="RMSE",

    stoppingMetric="RMSE",

    stoppingRounds=5,

    seed=42,

    predictionCol="prediction",

    projectName="nyc_taxi_automl"
)


print("\nSTARTING AUTOML...")
print("Maximum base models : 6")
print("XGBoost             : excluded")
print("Leaderboard metric  : RMSE")


# ==========================================================
# 7. TRAIN
# ==========================================================

best_model = automl.fit(
    train_df
)


print("\nAUTOML TRAINING SUCCESS")


# ==========================================================
# 8. LEADERBOARD
# ==========================================================

print("\n" + "=" * 70)
print("AUTOML LEADERBOARD")
print("=" * 70)


leaderboard = automl.getLeaderboard(
    "ALL"
)

leaderboard.show(
    20,
    truncate=False
)


# ==========================================================
# 9. TEST BEST MODEL
# ==========================================================

predictions = best_model.transform(
    test_df
)


print("\nBEST MODEL PREDICTION SAMPLE:")

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
# 10. METRICS
# ==========================================================

rmse_eval = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="rmse"
)

mae_eval = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="mae"
)

r2_eval = RegressionEvaluator(
    labelCol=label,
    predictionCol="prediction",
    metricName="r2"
)


rmse = rmse_eval.evaluate(
    predictions
)

mae = mae_eval.evaluate(
    predictions
)

r2 = r2_eval.evaluate(
    predictions
)


print("\n" + "=" * 70)
print("AUTOML BEST MODEL - TEST METRICS")
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
# 11. SAVE OUTPUT
# ==========================================================

os.makedirs(
    "outputs/metrics",
    exist_ok=True
)

os.makedirs(
    "outputs/predictions",
    exist_ok=True
)


metrics = {

    "model":
        "H2O AutoML Best Model",

    "max_models":
        6,

    "xgboost":
        "excluded",

    "train_rows":
        train_count,

    "validation_rows":
        valid_count,

    "test_rows":
        test_count,

    "rmse":
        rmse,

    "mae":
        mae,

    "r2":
        r2
}


with open(
    "outputs/metrics/"
    "automl_metrics.json",
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        metrics,
        f,
        indent=4,
        ensure_ascii=False
    )


leaderboard.write.mode(
    "overwrite"
).parquet(
    "outputs/metrics/"
    "automl_leaderboard"
)


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
    "outputs/predictions/"
    "automl_best"
)


print("\nAUTOML METRICS SAVED:")
print(
    "outputs/metrics/"
    "automl_metrics.json"
)

print("\nAUTOML LEADERBOARD SAVED:")
print(
    "outputs/metrics/"
    "automl_leaderboard"
)

print("\nAUTOML PREDICTIONS SAVED:")
print(
    "outputs/predictions/"
    "automl_best"
)


print("\n" + "=" * 70)
print("H2O AUTOML SUCCESS")
print("=" * 70)


train_df.unpersist()
valid_df.unpersist()
test_df.unpersist()

spark.stop()