"""Load the final MOJO independently and perform reproducible batch scoring."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import time
from pathlib import Path

import pysparkling
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pysparkling.ml import H2OMOJOModel

from src.common.project import load_config, project_path, spark_path


def normalize_pysparkling_jars() -> None:
    value = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if not (value.startswith("--jars ") and value.endswith(" pyspark-shell")):
        return
    paths = value[len("--jars ") : -len(" pyspark-shell")].split(",")
    unique: list[str] = []
    for value in paths:
        value = value.strip()
        if not value:
            continue
        uri = value if value.startswith("file:") else Path(value).resolve().as_uri()
        if uri not in unique:
            unique.append(uri)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {','.join(unique)} pyspark-shell"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--rows",
        type=int,
        default=0,
        help="Maximum rows to score; 0 means every row in the test split.",
    )
    parser.add_argument(
        "--deployment-validation",
        action="store_true",
        help=(
            "Score and validate every requested row without exporting the large "
            "batch/prediction Parquet datasets. Intended for ephemeral Docker "
            "and Kubernetes validation containers."
        ),
    )
    args = parser.parse_args()
    config = load_config(args.config)
    normalize_pysparkling_jars()

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-MOJO-Scoring")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source = spark.read.parquet(spark_path(config["paths"]["test"]))
    batch = (source.limit(args.rows) if args.rows > 0 else source).cache()
    batch_count = batch.count()
    if not args.deployment_validation:
        batch_path = project_path("data/processed/batch_input.parquet")
        batch.write.mode("overwrite").parquet(batch_path.resolve().as_posix())

    model_path = project_path("models/final_mojo").resolve()
    model = H2OMOJOModel.read().load(model_path.as_uri())
    started = time.perf_counter()
    predictions = model.transform(batch).cache()
    prediction_count = predictions.count()
    scoring_time = time.perf_counter() - started
    null_predictions = predictions.filter(F.col("prediction").isNull()).count()
    aggregate = predictions.agg(
        F.avg("prediction").alias("average_prediction"),
        F.min("prediction").alias("min_prediction"),
        F.max("prediction").alias("max_prediction"),
    ).first()
    summary = {
        "rows": prediction_count,
        "requested_rows": args.rows,
        "all_input_rows": args.rows <= 0,
        "null_predictions": null_predictions,
        "average_prediction": aggregate["average_prediction"],
        "min_prediction": aggregate["min_prediction"],
        "max_prediction": aggregate["max_prediction"],
        "scoring_time_seconds": scoring_time,
        "rows_per_second": prediction_count / scoring_time if scoring_time else None,
        "h2o_context_used": False,
        "deployment_validation": args.deployment_validation,
    }

    if not args.deployment_validation:
        output_path = project_path("outputs/predictions/batch_predictions")
        predictions.write.mode("overwrite").parquet(output_path.resolve().as_posix())
        mojo_output_path = project_path("outputs/predictions/mojo_predictions")
        predictions.write.mode("overwrite").parquet(mojo_output_path.resolve().as_posix())
    sample_columns = [
        "pickup_location",
        "dropoff_location",
        "pickup_hour",
        "trip_duration_minutes",
        "prediction",
    ]
    sample_rows = [
        row.asDict(recursive=True)
        for row in predictions.select(*sample_columns).limit(1000).collect()
    ]
    sample_path = project_path("outputs/predictions/batch_predictions_sample.csv")
    if sample_path.is_dir():
        shutil.rmtree(sample_path)
    with sample_path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=sample_columns)
        writer.writeheader()
        writer.writerows(sample_rows)

    metrics_path = project_path("outputs/metrics/mojo_scoring_summary.json")
    with metrics_path.open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, ensure_ascii=False)

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        "FINAL MOJO DEPLOYMENT VALIDATION SUCCESS"
        if args.deployment_validation
        else "FINAL MOJO SCORING SUCCESS"
    )
    predictions.unpersist()
    batch.unpersist()
    spark.stop()
    return 0 if null_predictions == 0 and prediction_count == batch_count else 1


if __name__ == "__main__":
    raise SystemExit(main())
