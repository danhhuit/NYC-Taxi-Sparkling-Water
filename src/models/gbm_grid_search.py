"""Chronological GBM grid search for the 12-month final dataset."""

from __future__ import annotations

import argparse
import csv
import json
import time
from itertools import product

import pysparkling  # noqa: F401
from pyspark.sql import SparkSession
from pysparkling import H2OConf, H2OContext
from pysparkling.ml import H2OGBMRegressor

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
    parser.add_argument("--max-train-rows", type=int, default=1_200_000)
    parser.add_argument("--max-validation-rows", type=int, default=400_000)
    parser.add_argument("--max-test-rows", type=int, default=400_000)
    parser.add_argument("--ntrees", type=int, default=150)
    args = parser.parse_args()
    config = load_config(args.config)
    normalize_pysparkling_jars()
    seed = config["project"]["random_seed"]

    spark = (
        SparkSession.builder.appName("NYC-Taxi-Final-GBM-GridSearch")
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

    grid = list(product((4, 6, 8), (0.03, 0.05)))
    results: list[dict[str, float | int]] = []
    best_model = None
    best_row = None
    started_all = time.perf_counter()
    for index, (max_depth, learn_rate) in enumerate(grid, start=1):
        print(f"GRID {index}/{len(grid)}: maxDepth={max_depth}, learnRate={learn_rate}")
        estimator = H2OGBMRegressor(
            featuresCols=FEATURE_COLUMNS,
            labelCol=LABEL_COLUMN,
            columnsToCategorical=CATEGORICAL_COLUMNS,
            convertUnknownCategoricalLevelsToNa=True,
            distribution="gaussian",
            ntrees=args.ntrees,
            maxDepth=max_depth,
            learnRate=learn_rate,
            sampleRate=0.8,
            colSampleRate=0.8,
            validationDataFrame=frames["validation"],
            stoppingMetric="RMSE",
            stoppingRounds=5,
            scoreTreeInterval=5,
            seed=seed,
            predictionCol="prediction",
            modelId=f"final_grid_d{max_depth}_lr{str(learn_rate).replace('.', '_')}",
        )
        started = time.perf_counter()
        model = estimator.fit(frames["train"])
        validation_metrics = regression_metrics(model.transform(frames["validation"]))
        row = {
            "maxDepth": max_depth,
            "learnRate": learn_rate,
            "ntrees": args.ntrees,
            "validation_rmse": validation_metrics["rmse"],
            "validation_mae": validation_metrics["mae"],
            "validation_r2": validation_metrics["r2"],
            "training_time_seconds": time.perf_counter() - started,
        }
        results.append(row)
        if best_row is None or row["validation_rmse"] < best_row["validation_rmse"]:
            best_row, best_model = row, model

    assert best_model is not None and best_row is not None
    test_predictions = best_model.transform(frames["test"]).cache()
    test_metrics = regression_metrics(test_predictions)
    output = {
        "model": "H2O GBM Grid Search",
        "split_strategy": "chronological",
        "full_rows": full_counts,
        "sample_rows": sample_counts,
        "features": FEATURE_COLUMNS,
        "grid_results": results,
        "best_configuration": best_row,
        "test_metrics": test_metrics,
        "total_time_seconds": time.perf_counter() - started_all,
    }

    metrics_dir = project_path(config["paths"]["metrics"])
    metrics_dir.mkdir(parents=True, exist_ok=True)
    with (metrics_dir / "gbm_grid_search_metrics.json").open("w", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, ensure_ascii=False)
    with (metrics_dir / "gbm_best_params.json").open("w", encoding="utf-8") as stream:
        json.dump(best_row, stream, indent=2, ensure_ascii=False)
    with (metrics_dir / "gbm_grid_results.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    test_predictions.write.mode("overwrite").parquet(
        project_path("outputs/predictions/gbm_grid_best").resolve().as_posix()
    )
    best_model.write().overwrite().save(
        project_path("models/gbm_grid_best_mojo").resolve().as_posix()
    )
    print(json.dumps(output, indent=2, ensure_ascii=False))
    print("GBM GRID SEARCH SUCCESS")
    test_predictions.unpersist()
    for frame in frames.values():
        frame.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
