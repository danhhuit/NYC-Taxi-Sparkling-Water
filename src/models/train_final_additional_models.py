"""Train final chronological DRF and Deep Learning comparison models."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pysparkling
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pysparkling import H2OConf, H2OContext
from pysparkling.ml import H2ODRFRegressor, H2ODeepLearningRegressor

from src.common.project import load_config, project_path, spark_path


def normalize_jars() -> None:
    value = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if not (value.startswith("--jars ") and value.endswith(" pyspark-shell")):
        return
    unique = []
    for item in value[len("--jars ") : -len(" pyspark-shell")].split(","):
        item = item.strip()
        if not item:
            continue
        uri = item if item.startswith("file:") else Path(item).resolve().as_uri()
        if uri not in unique:
            unique.append(uri)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {','.join(unique)} pyspark-shell"


def metric(frame, name: str) -> float:
    return RegressionEvaluator(
        labelCol="trip_duration_minutes",
        predictionCol="prediction",
        metricName=name,
    ).evaluate(frame)


def evaluate(model, frame) -> dict:
    prediction = model.transform(frame).cache()
    result = {
        "rmse": metric(prediction, "rmse"),
        "mae": metric(prediction, "mae"),
        "r2": metric(prediction, "r2"),
    }
    prediction.unpersist()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--models",
        choices=("all", "drf", "deep-learning"),
        default="all",
        help="Train both models or only one final candidate.",
    )
    parser.add_argument(
        "--promote-drf",
        action="store_true",
        help="Save DRF as models/final_mojo and refresh final test predictions.",
    )
    args = parser.parse_args()
    config = load_config()
    normalize_jars()
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-Additional-Models")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.executor.memory", config["spark"]["executor_memory"])
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    H2OContext.getOrCreate(H2OConf().setInternalClusterMode())

    seed = config["project"]["random_seed"]
    paths = config["paths"]
    limits = {"train": 1_200_000, "validation": 400_000, "test": 400_000}
    frames = {}
    full_counts = {}
    for name in ("train", "validation", "test"):
        full = spark.read.parquet(spark_path(paths[name]))
        count = full.count()
        full_counts[name] = count
        fraction = min(1.0, limits[name] * 1.1 / count)
        frames[name] = full.sample(False, fraction, seed).limit(limits[name]).cache()
        frames[name].count()

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
    common = {
        "featuresCols": features,
        "labelCol": "trip_duration_minutes",
        "columnsToCategorical": ["pickup_location", "dropoff_location"],
        "convertUnknownCategoricalLevelsToNa": True,
        "distribution": "gaussian",
        "validationDataFrame": frames["validation"],
        "stoppingMetric": "RMSE",
        "stoppingRounds": 5,
        "seed": seed,
        "predictionCol": "prediction",
    }
    candidates = [
        (
            "DRF",
            H2ODRFRegressor(
                **common,
                ntrees=100,
                maxDepth=20,
                minRows=10,
                sampleRate=0.632,
                modelId="nyc_taxi_final_drf",
            ),
            {"ntrees": 100, "maxDepth": 20, "minRows": 10, "sampleRate": 0.632},
            "drf_metrics.json",
        ),
        (
            "Deep Learning",
            H2ODeepLearningRegressor(
                **common,
                hidden=[64, 32],
                epochs=15,
                activation="Rectifier",
                modelId="nyc_taxi_final_deep_learning",
            ),
            {"hidden": [64, 32], "epochs": 15, "activation": "Rectifier"},
            "deep_learning_metrics.json",
        ),
    ]
    metrics_dir = project_path(paths["metrics"])
    for name, estimator, parameters, filename in candidates:
        model_key = "drf" if name == "DRF" else "deep-learning"
        if args.models not in ("all", model_key):
            continue
        print(f"TRAINING FINAL {name.upper()}...")
        started = time.perf_counter()
        model = estimator.fit(frames["train"])
        training_time = time.perf_counter() - started
        payload = {
            "model": name,
            "split_strategy": "chronological",
            "full_rows": full_counts,
            "sample_rows": limits,
            "features": features,
            "parameters": parameters,
            "training_time_seconds": training_time,
            "train_metrics": evaluate(model, frames["train"]),
            "validation_metrics": evaluate(model, frames["validation"]),
            "test_metrics": evaluate(model, frames["test"]),
        }
        with (metrics_dir / filename).open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, ensure_ascii=False)
        model_directory = "drf_mojo" if name == "DRF" else "deep_learning_mojo"
        model.write().overwrite().save(
            project_path(f"models/{model_directory}").resolve().as_posix()
        )
        if name == "DRF" and args.promote_drf:
            model.write().overwrite().save(
                project_path("models/final_mojo").resolve().as_posix()
            )
            final_payload = {
                **payload,
                "model": "H2O DRF Final Model",
                **payload["test_metrics"],
            }
            with (metrics_dir / "final_model_metrics.json").open(
                "w", encoding="utf-8"
            ) as stream:
                json.dump(final_payload, stream, indent=2, ensure_ascii=False)
            final_predictions = model.transform(frames["test"]).cache()
            final_predictions.write.mode("overwrite").parquet(
                project_path("outputs/predictions/final_test_predictions")
                .resolve()
                .as_posix()
            )
            final_predictions.unpersist()
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    for frame in frames.values():
        frame.unpersist()
    spark.stop()
    print("FINAL ADDITIONAL MODELS SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
