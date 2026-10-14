"""Create final-model error tables and charts from test predictions."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.project import load_config, project_path, spark_path


def save_rows(path: Path, rows: list[dict], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def collect(frame, columns: list[str]) -> list[dict]:
    return [row.asDict(recursive=True) for row in frame.select(*columns).collect()]


def plot_metric(rows: list[dict], x: str, output: Path, title: str) -> None:
    labels = [str(row[x]) for row in rows]
    values = [float(row["mae"]) for row in rows]
    plt.figure(figsize=(12, 6))
    plt.bar(labels, values, color="#2f75b5")
    plt.title(title)
    plt.xlabel(x.replace("_", " ").title())
    plt.ylabel("MAE (minutes)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output, dpi=160)
    plt.close()


def aggregate(frame, group_column: str):
    return (
        frame.groupBy(group_column)
        .agg(
            F.count("*").alias("rows"),
            F.avg(F.abs("error")).alias("mae"),
            F.sqrt(F.avg(F.pow("error", 2))).alias("rmse"),
            F.avg("error").alias("mean_error"),
        )
        .orderBy(group_column)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--predictions",
        default="outputs/predictions/final_test_predictions",
        help="Parquet directory containing trip_duration_minutes and prediction",
    )
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-Error-Analysis")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    frame = (
        spark.read.parquet(spark_path(args.predictions))
        .withColumn("error", F.col("prediction") - F.col("trip_duration_minutes"))
        .withColumn(
            "trip_duration_range",
            F.when(F.col("trip_duration_minutes") < 10, "01_<10")
            .when(F.col("trip_duration_minutes") < 20, "02_10-20")
            .when(F.col("trip_duration_minutes") < 40, "03_20-40")
            .when(F.col("trip_duration_minutes") < 60, "04_40-60")
            .otherwise("05_>=60"),
        )
    ).cache()

    metrics_dir = project_path(config["paths"]["metrics"])
    charts_dir = project_path(config["paths"]["charts"])
    specifications = [
        ("pickup_hour", "error_by_hour.csv", "error_by_hour.png", "MAE by Pickup Hour"),
        (
            "pickup_day_of_week",
            "error_by_day_of_week.csv",
            "error_by_day_of_week.png",
            "MAE by Pickup Day of Week",
        ),
        ("pickup_borough", "error_by_borough.csv", "error_by_borough.png", "MAE by Pickup Borough"),
        (
            "dropoff_borough",
            "error_by_dropoff_borough.csv",
            "error_by_dropoff_borough.png",
            "MAE by Dropoff Borough",
        ),
        (
            "trip_duration_range",
            "error_by_duration_range.csv",
            "error_by_duration_range.png",
            "MAE by Trip Duration Range",
        ),
    ]
    columns = ["group", "rows", "mae", "rmse", "mean_error"]
    for group_column, csv_name, chart_name, title in specifications:
        grouped = aggregate(frame, group_column)
        raw_rows = collect(grouped, [group_column, "rows", "mae", "rmse", "mean_error"])
        rows = [{"group": row.pop(group_column), **row} for row in raw_rows]
        save_rows(metrics_dir / csv_name, rows, columns)
        plot_metric(
            [{group_column: row["group"], **row} for row in rows],
            group_column,
            charts_dir / chart_name,
            title,
        )

    route_rows = (
        frame.groupBy("route_id")
        .agg(
            F.count("*").alias("rows"),
            F.avg(F.abs("error")).alias("mae"),
            F.sqrt(F.avg(F.pow("error", 2))).alias("rmse"),
            F.avg("error").alias("mean_error"),
        )
        .filter(F.col("rows") >= 100)
        .orderBy(F.desc("mae"))
        .limit(100)
    )
    rows = collect(route_rows, ["route_id", "rows", "mae", "rmse", "mean_error"])
    save_rows(
        metrics_dir / "error_by_route.csv",
        rows,
        ["route_id", "rows", "mae", "rmse", "mean_error"],
    )

    frame.unpersist()
    spark.stop()
    print("FINAL ERROR ANALYSIS SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
