"""Train the first final-project GBM on chronological multi-month splits."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pysparkling
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame, SparkSession
from pysparkling import H2OConf, H2OContext
from pysparkling.ml import H2OGBMRegressor

from src.common.project import load_config, project_path, spark_path


def normalize_pysparkling_jars() -> None:
    submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    prefix = "--jars "
    suffix = " pyspark-shell"
    if not (submit_args.startswith(prefix) and submit_args.endswith(suffix)):
        return
    raw_paths = submit_args[len(prefix) : -len(suffix)]
    unique: list[str] = []
    for raw_path in raw_paths.split(","):
        raw_path = raw_path.strip()
        if not raw_path:
            continue
        uri = raw_path if raw_path.startswith("file:") else Path(raw_path).resolve().as_uri()
        if uri not in unique:
            unique.append(uri)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {','.join(unique)} pyspark-shell"


def deterministic_sample(
    frame: DataFrame,
    full_count: int,
    maximum_rows: int,
    seed: int,
) -> DataFrame:
    if full_count <= maximum_rows:
        return frame
    fraction = min(1.0, maximum_rows * 1.1 / full_count)
    return frame.sample(False, fraction, seed=seed).limit(maximum_rows)


def evaluator(metric: str) -> RegressionEvaluator:
    return RegressionEvaluator(
        labelCol="trip_duration_minutes",
        predictionCol="prediction",
        metricName=metric,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--max-train-rows", type=int, default=1_200_000)
    parser.add_argument("--max-validation-rows", type=int, default=400_000)
    parser.add_argument("--max-test-rows", type=int, default=400_000)
    parser.add_argument("--ntrees", type=int, default=150)
    parser.add_argument("--max-depth", type=int, default=8)
    args = parser.parse_args()
    config = load_config(args.config)
    normalize_pysparkling_jars()

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-H2O-GBM")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.executor.memory", config["spark"]["executor_memory"])
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    H2OContext.getOrCreate(H2OConf().setInternalClusterMode())

    train_full = spark.read.parquet(spark_path(config["paths"]["train"]))
    validation_full = spark.read.parquet(spark_path(config["paths"]["validation"]))
    test_full = spark.read.parquet(spark_path(config["paths"]["test"]))
    full_counts = {
        "train": train_full.count(),
        "validation": validation_full.count(),
        "test": test_full.count(),
    }
    seed = config["project"]["random_seed"]
    train = deterministic_sample(train_full, full_counts["train"], args.max_train_rows, seed).cache()
    validation = deterministic_sample(
        validation_full, full_counts["validation"], args.max_validation_rows, seed
    ).cache()
    test = deterministic_sample(test_full, full_counts["test"], args.max_test_rows, seed).cache()
    sample_counts = {
        "train": train.count(),
        "validation": validation.count(),
        "test": test.count(),
    }

    features = [
        "pickup_location",
        "dropoff_location",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
        "route_trip_count",
        "route_avg_duration",
        "hour_route_avg_duration",
    ]
    parameters = {
        "ntrees": args.ntrees,
        "maxDepth": args.max_depth,
        "learnRate": 0.05,
        "sampleRate": 0.8,
        "colSampleRate": 0.8,
        "seed": seed,
    }
    estimator = H2OGBMRegressor(
        featuresCols=features,
        labelCol="trip_duration_minutes",
        columnsToCategorical=["pickup_location", "dropoff_location"],
        convertUnknownCategoricalLevelsToNa=True,
        distribution="gaussian",
        ntrees=parameters["ntrees"],
        maxDepth=parameters["maxDepth"],
        learnRate=parameters["learnRate"],
        sampleRate=parameters["sampleRate"],
        colSampleRate=parameters["colSampleRate"],
        validationDataFrame=validation,
        stoppingMetric="RMSE",
        stoppingRounds=5,
        scoreTreeInterval=5,
        seed=seed,
        predictionCol="prediction",
        modelId="nyc_taxi_final_gbm",
    )

    print("=" * 78)
    print("FINAL H2O GBM - CHRONOLOGICAL SPLIT")
    print(f"Full rows   : {full_counts}")
    print(f"Sample rows : {sample_counts}")
    print(f"Features    : {features}")
    print("=" * 78)
    started = time.perf_counter()
    model = estimator.fit(train)
    training_time = time.perf_counter() - started
    predictions = model.transform(test).cache()
    rmse = evaluator("rmse").evaluate(predictions)
    mae = evaluator("mae").evaluate(predictions)
    r2 = evaluator("r2").evaluate(predictions)

    metrics = {
        "model": "H2O GBM Final Candidate",
        "split_strategy": "chronological",
        "full_rows": full_counts,
        "sample_rows": sample_counts,
        "features": features,
        "parameters": parameters,
        "training_time_seconds": training_time,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
    }
    metrics_dir = project_path(config["paths"]["metrics"])
    metrics_dir.mkdir(parents=True, exist_ok=True)
    for name in ("gbm_metrics.json", "final_model_metrics.json"):
        with (metrics_dir / name).open("w", encoding="utf-8") as stream:
            json.dump(metrics, stream, indent=2, ensure_ascii=False)

    prediction_path = project_path("outputs/predictions/final_test_predictions")
    prediction_path.parent.mkdir(parents=True, exist_ok=True)
    predictions.select(
        "tpep_pickup_datetime",
        "pickup_location",
        "dropoff_location",
        "pickup_borough",
        "dropoff_borough",
        "route_id",
        "pickup_hour",
        "pickup_day_of_week",
        "trip_duration_minutes",
        "prediction",
    ).write.mode("overwrite").parquet(prediction_path.resolve().as_posix())

    model_path = project_path("models/final_mojo")
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(model_path.resolve().as_posix())

    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    print(f"Model: {model_path}")
    print("FINAL MODEL SELECTED")
    predictions.unpersist()
    train.unpersist()
    validation.unpersist()
    test.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
