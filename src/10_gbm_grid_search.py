import os
import json
from pathlib import Path
from itertools import product

import pysparkling

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

from pysparkling import H2OContext, H2OConf
from pysparkling.ml import H2OGBMRegressor


# ==========================================================
# WINDOWS FIX - PYSPARKLING JARS
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

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--jars "
        + ",".join(fixed_jars)
        + " pyspark-shell"
    )


# ==========================================================
# SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-H2O-GBM-GridSearch")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("H2O GBM GRID SEARCH")
print("=" * 70)


# ==========================================================
# H2O
# ==========================================================

conf = H2OConf().setInternalClusterMode()

hc = H2OContext.getOrCreate(conf)


# ==========================================================
# DATA
# ==========================================================

df = spark.read.parquet(
    "data/processed/taxi_ml_ready.parquet"
)


train_df, valid_df, test_df = df.randomSplit(
    [0.70, 0.15, 0.15],
    seed=42
)

train_df = train_df.cache()
valid_df = valid_df.cache()
test_df = test_df.cache()


print("\nDATA SPLIT:")
print("Train      :", train_df.count())
print("Validation :", valid_df.count())
print("Test       :", test_df.count())


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
# EVALUATORS
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


# ==========================================================
# GRID
# ==========================================================

max_depth_values = [
    4,
    6,
    8
]

learn_rate_values = [
    0.03,
    0.05
]


grid = list(
    product(
        max_depth_values,
        learn_rate_values
    )
)


print("\nTOTAL GRID CONFIGURATIONS:")
print(len(grid))


results = []

best_rmse = float("inf")
best_model = None
best_params = None


# ==========================================================
# GRID SEARCH
# ==========================================================

for index, (
    max_depth,
    learn_rate
) in enumerate(
    grid,
    start=1
):

    print("\n" + "=" * 70)

    print(
        f"GRID {index}/{len(grid)}"
    )

    print(
        f"maxDepth={max_depth}"
    )

    print(
        f"learnRate={learn_rate}"
    )


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

        maxDepth=max_depth,

        learnRate=learn_rate,

        sampleRate=0.8,

        colSampleRate=0.8,

        validationDataFrame=valid_df,

        stoppingMetric="RMSE",

        stoppingRounds=5,

        scoreTreeInterval=5,

        seed=42,

        predictionCol="prediction",

        modelId=(
            f"gbm_grid_"
            f"d{max_depth}_"
            f"lr{str(learn_rate).replace('.', '_')}"
        )
    )


    model = gbm.fit(
        train_df
    )


    valid_predictions = model.transform(
        valid_df
    )


    valid_rmse = rmse_eval.evaluate(
        valid_predictions
    )

    valid_mae = mae_eval.evaluate(
        valid_predictions
    )

    valid_r2 = r2_eval.evaluate(
        valid_predictions
    )


    print("\nVALIDATION RESULT:")

    print(
        f"RMSE = {valid_rmse:.4f}"
    )

    print(
        f"MAE  = {valid_mae:.4f}"
    )

    print(
        f"R2   = {valid_r2:.4f}"
    )


    result = {
        "maxDepth": max_depth,
        "learnRate": learn_rate,
        "ntrees": 150,
        "validation_rmse": valid_rmse,
        "validation_mae": valid_mae,
        "validation_r2": valid_r2
    }

    results.append(
        result
    )


    if valid_rmse < best_rmse:

        best_rmse = valid_rmse

        best_model = model

        best_params = result


# ==========================================================
# BEST CONFIG
# ==========================================================

print("\n" + "=" * 70)
print("BEST GRID CONFIGURATION")
print("=" * 70)

print(
    json.dumps(
        best_params,
        indent=4
    )
)


# ==========================================================
# FINAL TEST - ONLY BEST MODEL
# ==========================================================

test_predictions = best_model.transform(
    test_df
)


test_rmse = rmse_eval.evaluate(
    test_predictions
)

test_mae = mae_eval.evaluate(
    test_predictions
)

test_r2 = r2_eval.evaluate(
    test_predictions
)


print("\n" + "=" * 70)
print("BEST GBM GRID - TEST METRICS")
print("=" * 70)

print(
    f"RMSE : {test_rmse:.4f} minutes"
)

print(
    f"MAE  : {test_mae:.4f} minutes"
)

print(
    f"R2   : {test_r2:.4f}"
)

print("=" * 70)


# ==========================================================
# SAVE RESULTS
# ==========================================================

os.makedirs(
    "outputs/metrics",
    exist_ok=True
)


output = {
    "grid_results": results,

    "best_configuration": best_params,

    "test_metrics": {
        "rmse": test_rmse,
        "mae": test_mae,
        "r2": test_r2
    }
}


with open(
    "outputs/metrics/"
    "gbm_grid_search_metrics.json",
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        output,
        f,
        indent=4,
        ensure_ascii=False
    )


test_predictions.select(

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
    "gbm_grid_best"
)


print(
    "\nGRID SEARCH RESULTS SAVED"
)

print(
    "outputs/metrics/"
    "gbm_grid_search_metrics.json"
)

print("\n" + "=" * 70)
print("GBM GRID SEARCH SUCCESS")
print("=" * 70)


train_df.unpersist()
valid_df.unpersist()
test_df.unpersist()

spark.stop()