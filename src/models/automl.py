"""H2O AutoML on the final chronological 12-month split."""

from __future__ import annotations

import argparse
import csv
import json
import time

import pysparkling  # noqa: F401
from pyspark.sql import SparkSession
from pysparkling import H2OConf, H2OContext
from pysparkling.ml import H2OAutoMLRegressor

from src.common.modeling import (
    CATEGORICAL_COLUMNS,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    bounded_sample,
    normalize_pysparkling_jars,
    regression_metrics,
)
from src.common.project import load_config, project_path, spark_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--max-train-rows", type=int, default=600_000)
    parser.add_argument("--max-validation-rows", type=int, default=200_000)
    parser.add_argument("--max-test-rows", type=int, default=200_000)
    parser.add_argument("--max-models", type=int, default=4)
    parser.add_argument("--max-runtime-secs", type=float, default=480.0)
    args = parser.parse_args()
    config = load_config(args.config)
    normalize_pysparkling_jars()
    seed = config["project"]["random_seed"]

    spark = (
        SparkSession.builder.appName("NYC-Taxi-Final-H2O-AutoML")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.executor.memory", config["spark"]["executor_memory"])
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    H2OContext.getOrCreate(H2OConf().setInternalClusterMode())

    full = {
        name: spark.read.parquet(spark_path(config["paths"][name])).select(
            *FEATURE_COLUMNS, LABEL_COLUMN
        )
        for name in ("train", "validation", "test")
    }
    full_counts = {name: frame.count() for name, frame in full.items()}
    limits = {
        "train": args.max_train_rows,
        "validation": args.max_validation_rows,
        "test": args.max_test_rows,
    }
    frames = {
        name: bounded_sample(frame, full_counts[name], limits[name], seed).cache()
        for name, frame in full.items()
    }
    sample_counts = {name: frame.count() for name, frame in frames.items()}

    estimator = H2OAutoMLRegressor(
        featuresCols=FEATURE_COLUMNS,
        labelCol=LABEL_COLUMN,
        columnsToCategorical=CATEGORICAL_COLUMNS,
        convertUnknownCategoricalLevelsToNa=True,
        excludeAlgos=["XGBoost"],
        maxModels=args.max_models,
        maxRuntimeSecs=args.max_runtime_secs,
        nfolds=0,
        validationDataFrame=frames["validation"],
        blendingDataFrame=frames["validation"],
        leaderboardDataFrame=frames["validation"],
        sortMetric="RMSE",
        stoppingMetric="RMSE",
        stoppingRounds=5,
        seed=seed,
        predictionCol="prediction",
        projectName="nyc_taxi_final_automl",
    )
    started = time.perf_counter()
    best_model = estimator.fit(frames["train"])
    training_time = time.perf_counter() - started
    leaderboard = estimator.getLeaderboard("ALL")
    leaderboard_rows = [row.asDict(recursive=True) for row in leaderboard.collect()]
    predictions = best_model.transform(frames["test"]).cache()
    test_metrics = regression_metrics(predictions)
    best_model_id = str(leaderboard_rows[0].get("model_id", "")) if leaderboard_rows else ""

    payload = {
        "model": "H2O AutoML Best Model",
        "best_model_id": best_model_id,
        "split_strategy": "chronological",
        "full_rows": full_counts,
        "sample_rows": sample_counts,
        "features": FEATURE_COLUMNS,
        "max_models": args.max_models,
        "max_runtime_seconds": args.max_runtime_secs,
        "excluded_algorithms": ["XGBoost"],
        "training_time_seconds": training_time,
        "test_metrics": test_metrics,
        **test_metrics,
    }
    metrics_dir = project_path(config["paths"]["metrics"])
    metrics_dir.mkdir(parents=True, exist_ok=True)
    with (metrics_dir / "automl_metrics.json").open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, ensure_ascii=False)
    with (metrics_dir / "automl_test_metrics.json").open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, ensure_ascii=False)
    leaderboard_path = metrics_dir / "automl_leaderboard.csv"
    if leaderboard_rows:
        with leaderboard_path.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(leaderboard_rows[0]))
            writer.writeheader()
            writer.writerows(leaderboard_rows)
    else:
        leaderboard_path.write_text("model_id\n", encoding="utf-8")

    predictions.write.mode("overwrite").parquet(
        project_path("outputs/predictions/automl_best").resolve().as_posix()
    )
    best_model.write().overwrite().save(
        project_path("models/automl_best_mojo").resolve().as_posix()
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("H2O AUTOML SUCCESS")
    predictions.unpersist()
    for frame in frames.values():
        frame.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
