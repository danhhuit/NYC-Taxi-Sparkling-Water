"""Shared helpers for final-project model training and evaluation."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame


FEATURE_COLUMNS = [
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
LABEL_COLUMN = "trip_duration_minutes"
CATEGORICAL_COLUMNS = ["pickup_location", "dropoff_location"]


def normalize_pysparkling_jars() -> None:
    """Convert Windows jar paths in PYSPARK_SUBMIT_ARGS to file URIs."""
    value = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if not (value.startswith("--jars ") and value.endswith(" pyspark-shell")):
        return
    unique: list[str] = []
    for raw_path in value[len("--jars ") : -len(" pyspark-shell")].split(","):
        raw_path = raw_path.strip()
        if not raw_path:
            continue
        uri = raw_path if raw_path.startswith("file:") else Path(raw_path).resolve().as_uri()
        if uri not in unique:
            unique.append(uri)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {','.join(unique)} pyspark-shell"


def bounded_sample(
    frame: DataFrame,
    full_count: int,
    maximum_rows: int,
    seed: int,
) -> DataFrame:
    """Return all rows when maximum_rows <= 0, otherwise a reproducible cap."""
    if maximum_rows <= 0 or full_count <= maximum_rows:
        return frame
    fraction = min(1.0, maximum_rows * 1.1 / full_count)
    return frame.sample(False, fraction, seed=seed).limit(maximum_rows)


def regression_metrics(frame: DataFrame) -> dict[str, float]:
    return {
        metric: RegressionEvaluator(
            labelCol=LABEL_COLUMN,
            predictionCol="prediction",
            metricName=metric,
        ).evaluate(frame)
        for metric in ("rmse", "mae", "r2")
    }
