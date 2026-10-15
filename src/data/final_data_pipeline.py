"""Build the leakage-safe six-month final dataset and chronological splits."""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.common.project import (
    ensure_output_directories,
    load_config,
    postgres_password,
    project_path,
    spark_path,
)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_spark(config: dict) -> SparkSession:
    spark_config = config["spark"]
    builder = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-Data-Pipeline")
        .master(spark_config["master"])
        .config("spark.driver.memory", spark_config["driver_memory"])
        .config("spark.executor.memory", spark_config["executor_memory"])
        .config("spark.sql.shuffle.partitions", str(spark_config["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", config["project"]["timezone"])
    )
    jdbc_jar = project_path("../ivy-cache/cache/org.postgresql/postgresql/jars/postgresql-42.7.13.jar")
    if jdbc_jar.exists():
        builder = builder.config("spark.jars", jdbc_jar.resolve().as_uri())
    return builder.getOrCreate()


def raw_summary(spark: SparkSession, files: list[Path], metrics_dir: Path) -> None:
    rows: list[dict] = []
    for file in files:
        frame = spark.read.parquet(file.resolve().as_posix())
        stats = frame.agg(
            F.count("*").alias("rows"),
            F.min("tpep_pickup_datetime").alias("min_pickup_time"),
            F.max("tpep_pickup_datetime").alias("max_pickup_time"),
        ).first()
        month = file.stem.replace("yellow_tripdata_", "")
        rows.append(
            {
                "month": month,
                "rows": stats["rows"],
                "columns": len(frame.columns),
                "file_size_mb": round(file.stat().st_size / 1_048_576, 2),
                "min_pickup_time": stats["min_pickup_time"],
                "max_pickup_time": stats["max_pickup_time"],
            }
        )
        print(f"RAW {month}: {stats['rows']:,} rows")
    write_csv(
        metrics_dir / "raw_dataset_summary.csv",
        ["month", "rows", "columns", "file_size_mb", "min_pickup_time", "max_pickup_time"],
        rows,
    )


def attach_postgres(frame: DataFrame, spark: SparkSession, config: dict) -> tuple[DataFrame, dict]:
    postgres = config["postgres"]
    password = postgres_password(config)
    summary = {
        "taxi_rows": frame.count(),
        "postgres_rows": 0,
        "matched_rows": 0,
        "unmatched_rows": 0,
        "match_rate": 0.0,
        "jdbc_status": "SKIPPED_PASSWORD_NOT_SET",
    }
    if not password:
        enriched = (
            frame
            .withColumn("zone_type", F.lit(None).cast("string"))
            .withColumn("congestion_level", F.lit(None).cast("string"))
            .withColumn("priority_score", F.lit(0).cast("integer"))
        )
        summary["unmatched_rows"] = summary["taxi_rows"]
        return enriched, summary

    jdbc_url = f"jdbc:postgresql://{postgres['host']}:{postgres['port']}/{postgres['database']}"
    try:
        zone_stats = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", postgres["table"])
            .option("user", postgres["user"])
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        postgres_rows = zone_stats.count()
        enriched = frame.join(
            F.broadcast(zone_stats),
            frame["pickup_location"] == zone_stats["zone_id"],
            "left",
        ).drop("zone_id")
        matched_rows = enriched.filter(F.col("priority_score").isNotNull()).count()
        enriched = enriched.withColumn(
            "priority_score", F.coalesce(F.col("priority_score"), F.lit(0))
        )
        summary.update(
            {
                "postgres_rows": postgres_rows,
                "matched_rows": matched_rows,
                "unmatched_rows": summary["taxi_rows"] - matched_rows,
                "match_rate": round(matched_rows * 100.0 / summary["taxi_rows"], 4),
                "jdbc_status": "CONNECTED",
            }
        )
        return enriched, summary
    except Exception as exc:
        print(f"WARNING: PostgreSQL join skipped: {exc}")
        enriched = (
            frame
            .withColumn("zone_type", F.lit(None).cast("string"))
            .withColumn("congestion_level", F.lit(None).cast("string"))
            .withColumn("priority_score", F.lit(0).cast("integer"))
        )
        summary["unmatched_rows"] = summary["taxi_rows"]
        summary["jdbc_status"] = f"ERROR_{type(exc).__name__}"
        return enriched, summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    config = load_config(args.config)
    ensure_output_directories(config)

    metrics_dir = project_path(config["paths"]["metrics"])
    raw_dir = project_path(config["paths"]["raw_yellow"])
    files = sorted(raw_dir.glob("yellow_tripdata_*.parquet"))
    expected_start = config["dataset"]["start_month"]
    expected_end = config["dataset"]["end_month"]
    files = [
        path
        for path in files
        if expected_start <= path.stem.replace("yellow_tripdata_", "") <= expected_end
    ]
    if not files:
        raise FileNotFoundError(f"No configured Parquet files found in {raw_dir}")

    spark = make_spark(config)
    spark.sparkContext.setLogLevel("WARN")
    started = time.perf_counter()
    print("=" * 78)
    print("NYC TAXI FINAL DATA PIPELINE")
    print(f"Files: {len(files)} ({expected_start} -> {expected_end})")
    print("=" * 78)

    raw_summary(spark, files, metrics_dir)
    dataset = config["dataset"]
    input_paths = [path.resolve().as_posix() for path in files]
    raw = (
        spark.read.option("mergeSchema", "true").parquet(*input_paths)
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "source_month",
            F.regexp_extract("source_file", r"yellow_tripdata_(\d{4}-\d{2})\.parquet", 1),
        )
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
    ).persist(StorageLevel.DISK_ONLY)

    raw_rows = raw.count()
    invalid_duration = raw.filter(
        F.col("trip_duration_minutes").isNull()
        | (F.col("trip_duration_minutes") < dataset["min_duration_minutes"])
        | (F.col("trip_duration_minutes") > dataset["max_duration_minutes"])
    ).count()
    null_pickup = raw.filter(F.col("tpep_pickup_datetime").isNull()).count()
    null_dropoff = raw.filter(F.col("tpep_dropoff_datetime").isNull()).count()
    invalid_passenger = raw.filter(
        F.col("passenger_count").isNotNull()
        & (
            (F.col("passenger_count") < dataset["min_passenger_count"])
            | (F.col("passenger_count") > dataset["max_passenger_count"])
        )
    ).count()

    identity_columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
    ]
    available_identity = [column for column in identity_columns if column in raw.columns]
    identity_values = [
        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
        for column in available_identity
    ]
    deduplicated = (
        raw.withColumn(
            "_identity_hash",
            F.sha2(F.concat_ws("||", *identity_values), 256),
        )
        .repartition(config["spark"]["shuffle_partitions"], "_identity_hash")
        .dropDuplicates(["_identity_hash"])
        .drop("_identity_hash")
    )
    duplicate_rows = raw_rows - deduplicated.count()

    cleaned = (
        deduplicated
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(
            F.col("trip_duration_minutes").between(
                dataset["min_duration_minutes"], dataset["max_duration_minutes"]
            )
        )
        .filter(
            F.col("PULocationID").between(
                dataset["min_location_id"], dataset["max_location_id"]
            )
        )
        .filter(
            F.col("DOLocationID").between(
                dataset["min_location_id"], dataset["max_location_id"]
            )
        )
        .filter(
            F.col("passenger_count").isNull()
            | F.col("passenger_count").between(
                dataset["min_passenger_count"], dataset["max_passenger_count"]
            )
        )
        .filter(F.date_format("tpep_pickup_datetime", "yyyy-MM") == F.col("source_month"))
        .withColumn("passenger_count", F.coalesce("passenger_count", F.lit(1.0)))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn(
            "is_weekend",
            F.when(F.col("pickup_day_of_week").isin(1, 7), 1).otherwise(0),
        )
    ).persist(StorageLevel.DISK_ONLY)
    clean_rows = cleaned.count()
    cleaned.write.mode("overwrite").partitionBy("pickup_month").parquet(
        spark_path(config["paths"]["clean"])
    )

    cleaning_row = {
        "raw_rows": raw_rows,
        "clean_rows": clean_rows,
        "removed_rows": raw_rows - clean_rows,
        "removed_percentage": round((raw_rows - clean_rows) * 100.0 / raw_rows, 4),
        "null_pickup": null_pickup,
        "null_dropoff": null_dropoff,
        "invalid_duration": invalid_duration,
        "invalid_passenger_count": invalid_passenger,
        "duplicates": duplicate_rows,
    }
    write_csv(metrics_dir / "cleaning_summary.csv", list(cleaning_row), [cleaning_row])

    zones = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(spark_path(config["paths"]["zone_lookup"]))
    )
    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    base_features = (
        cleaned.join(F.broadcast(pickup_zones), "PULocationID", "left")
        .join(F.broadcast(dropoff_zones), "DOLocationID", "left")
        .withColumn("pickup_location", F.col("PULocationID").cast("string"))
        .withColumn("dropoff_location", F.col("DOLocationID").cast("string"))
        .withColumn(
            "route_id",
            F.concat_ws("-", F.col("pickup_location"), F.col("dropoff_location")),
        )
    ).persist(StorageLevel.DISK_ONLY)

    split_config = config["split"]
    train_base = base_features.filter(
        F.col("tpep_pickup_datetime") <= F.to_timestamp(F.lit(split_config["train_end"]))
    )
    validation_base = base_features.filter(
        (F.col("tpep_pickup_datetime") > F.to_timestamp(F.lit(split_config["train_end"])))
        & (
            F.col("tpep_pickup_datetime")
            <= F.to_timestamp(F.lit(split_config["validation_end"]))
        )
    )
    test_base = base_features.filter(
        (
            F.col("tpep_pickup_datetime")
            > F.to_timestamp(F.lit(split_config["validation_end"]))
        )
        & (F.col("tpep_pickup_datetime") <= F.to_timestamp(F.lit(split_config["test_end"])))
    )

    global_train_mean = train_base.agg(F.avg("trip_duration_minutes")).first()[0]
    route_stats = train_base.groupBy("route_id").agg(
        F.count("*").alias("route_trip_count"),
        F.avg("trip_duration_minutes").alias("route_avg_duration"),
    )
    hour_route_stats = train_base.groupBy("route_id", "pickup_hour").agg(
        F.avg("trip_duration_minutes").alias("hour_route_avg_duration")
    )

    def add_history(frame: DataFrame) -> DataFrame:
        return (
            frame.join(F.broadcast(route_stats), "route_id", "left")
            .join(F.broadcast(hour_route_stats), ["route_id", "pickup_hour"], "left")
            .withColumn("route_trip_count", F.coalesce("route_trip_count", F.lit(0)))
            .withColumn(
                "route_avg_duration",
                F.coalesce("route_avg_duration", F.lit(global_train_mean)),
            )
            .withColumn(
                "hour_route_avg_duration",
                F.coalesce(
                    "hour_route_avg_duration",
                    "route_avg_duration",
                    F.lit(global_train_mean),
                ),
            )
        )

    train = add_history(train_base)
    validation = add_history(validation_base)
    test = add_history(test_base)
    all_features = train.unionByName(validation).unionByName(test)
    all_features.write.mode("overwrite").partitionBy("pickup_month").parquet(
        spark_path(config["paths"]["features"])
    )

    multisource, join_summary = attach_postgres(all_features, spark, config)
    multisource.write.mode("overwrite").partitionBy("pickup_month").parquet(
        spark_path(config["paths"]["multisource"])
    )
    write_csv(
        metrics_dir / "multisource_join_summary.csv",
        list(join_summary),
        [join_summary],
    )

    model_columns = [
        "tpep_pickup_datetime",
        "pickup_location",
        "dropoff_location",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
        "route_id",
        "pickup_borough",
        "dropoff_borough",
        "pickup_zone",
        "dropoff_zone",
        "route_trip_count",
        "route_avg_duration",
        "hour_route_avg_duration",
        "zone_type",
        "congestion_level",
        "priority_score",
        "trip_duration_minutes",
    ]
    available_columns = [column for column in model_columns if column in multisource.columns]
    final_frame = multisource.select(*available_columns).persist(StorageLevel.DISK_ONLY)

    split_frames = {
        "train": final_frame.filter(
            F.col("tpep_pickup_datetime") <= F.to_timestamp(F.lit(split_config["train_end"]))
        ),
        "validation": final_frame.filter(
            (
                F.col("tpep_pickup_datetime")
                > F.to_timestamp(F.lit(split_config["train_end"]))
            )
            & (
                F.col("tpep_pickup_datetime")
                <= F.to_timestamp(F.lit(split_config["validation_end"]))
            )
        ),
        "test": final_frame.filter(
            (
                F.col("tpep_pickup_datetime")
                > F.to_timestamp(F.lit(split_config["validation_end"]))
            )
            & (
                F.col("tpep_pickup_datetime")
                <= F.to_timestamp(F.lit(split_config["test_end"]))
            )
        ),
    }
    split_paths = {
        "train": config["paths"]["train"],
        "validation": config["paths"]["validation"],
        "test": config["paths"]["test"],
    }
    split_rows: list[dict] = []
    for name, frame in split_frames.items():
        frame.write.mode("overwrite").parquet(spark_path(split_paths[name]))
        stats = frame.agg(
            F.count("*").alias("rows"),
            F.min("tpep_pickup_datetime").alias("min_date"),
            F.max("tpep_pickup_datetime").alias("max_date"),
        ).first()
        split_rows.append(
            {
                "split": name,
                "rows": stats["rows"],
                "min_date": stats["min_date"],
                "max_date": stats["max_date"],
                "percentage": round(stats["rows"] * 100.0 / clean_rows, 4),
            }
        )
        print(f"{name.upper():10s}: {stats['rows']:,} rows")
    write_csv(
        metrics_dir / "split_summary.csv",
        ["split", "rows", "min_date", "max_date", "percentage"],
        split_rows,
    )

    dictionary = [
        ("pickup_location", "string", "Pickup taxi zone ID", "NYC TLC", True, "low"),
        ("dropoff_location", "string", "Dropoff taxi zone ID", "NYC TLC", True, "low"),
        ("passenger_count", "double", "Passenger count (null imputed to 1)", "NYC TLC", True, "low"),
        ("pickup_hour", "integer", "Pickup hour 0-23", "derived", True, "low"),
        ("pickup_day_of_week", "integer", "Spark day of week 1-7", "derived", True, "low"),
        ("pickup_month", "integer", "Pickup month", "derived", True, "low"),
        ("is_weekend", "integer", "Weekend indicator", "derived", True, "low"),
        ("route_id", "string", "Pickup-dropoff route key", "derived", False, "low"),
        ("pickup_borough", "string", "Pickup borough", "Taxi Zone Lookup", False, "low"),
        ("dropoff_borough", "string", "Dropoff borough", "Taxi Zone Lookup", False, "low"),
        ("route_trip_count", "long", "Route count calculated from train only", "train history", True, "controlled"),
        ("route_avg_duration", "double", "Route mean duration calculated from train only", "train history", True, "controlled"),
        ("hour_route_avg_duration", "double", "Route-hour mean duration calculated from train only", "train history", True, "controlled"),
        ("priority_score", "integer", "Demonstration JDBC metadata", "PostgreSQL", False, "synthetic"),
        ("trip_duration_minutes", "double", "Regression target", "derived target", False, "target"),
    ]
    write_csv(
        metrics_dir / "feature_dictionary.csv",
        ["feature_name", "data_type", "description", "source", "used_in_model", "leakage_risk"],
        [
            dict(zip(
                ["feature_name", "data_type", "description", "source", "used_in_model", "leakage_risk"],
                row,
            ))
            for row in dictionary
        ],
    )

    elapsed = time.perf_counter() - started
    print(f"\nElapsed: {elapsed:.2f} seconds")
    print("FINAL DATASET READY")
    raw.unpersist()
    cleaned.unpersist()
    base_features.unpersist()
    final_frame.unpersist()
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
