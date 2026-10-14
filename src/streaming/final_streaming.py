"""Run 10 micro-batches through the final MOJO and persist predictions."""

from __future__ import annotations

import argparse
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


def reset_directory(path: Path) -> None:
    resolved = path.resolve()
    project = project_path(".").resolve()
    if project not in resolved.parents:
        raise RuntimeError(f"Refusing to reset path outside project: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--rows-per-batch", type=int, default=100)
    args = parser.parse_args()
    config = load_config(args.config)
    normalize_pysparkling_jars()

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Final-Streaming-MOJO")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    staging = project_path("data/stream/staging_final")
    incoming = project_path("data/stream/incoming_final")
    checkpoint = project_path("data/stream/checkpoint_final")
    output = project_path("outputs/streaming/final_predictions")
    for path in (staging, incoming, checkpoint, output):
        reset_directory(path)

    total_rows = args.batches * args.rows_per_batch
    source = (
        spark.read.parquet(spark_path("data/processed/batch_input.parquet"))
        .limit(total_rows)
        .withColumn("event_id", F.monotonically_increasing_id())
    )
    rows = [row.asDict(recursive=True) for row in source.collect()]
    if len(rows) != total_rows:
        raise RuntimeError(f"Expected {total_rows} input rows, got {len(rows)}")

    for index in range(args.batches):
        start = index * args.rows_per_batch
        batch_rows = rows[start : start + args.rows_per_batch]
        file_path = staging / f"batch_{index + 1:02d}.json"
        with file_path.open("w", encoding="utf-8") as stream:
            for row in batch_rows:
                row["tpep_pickup_datetime"] = str(row["tpep_pickup_datetime"])
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        shutil.copy2(file_path, incoming / file_path.name)

    schema = source.schema
    stream_frame = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(incoming.resolve().as_posix())
    )
    model = H2OMOJOModel.read().load(project_path("models/final_mojo").resolve().as_uri())
    totals = {"batches": 0, "rows": 0, "null_predictions": 0}

    def score_batch(batch_frame, batch_id: int) -> None:
        predictions = model.transform(batch_frame).cache()
        count = predictions.count()
        null_count = predictions.filter(F.col("prediction").isNull()).count()
        predictions.write.mode("overwrite").parquet(
            (output / f"batch_{batch_id:02d}").resolve().as_posix()
        )
        totals["batches"] += 1
        totals["rows"] += count
        totals["null_predictions"] += null_count
        print(f"MICRO-BATCH {batch_id}: rows={count}, null_predictions={null_count}")
        predictions.unpersist()

    started = time.perf_counter()
    query = (
        stream_frame.writeStream
        .foreachBatch(score_batch)
        .option("checkpointLocation", checkpoint.resolve().as_posix())
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    elapsed = time.perf_counter() - started
    summary = {
        **totals,
        "expected_batches": args.batches,
        "expected_rows": total_rows,
        "elapsed_seconds": elapsed,
        "rows_per_second": totals["rows"] / elapsed if elapsed else None,
        "model": "models/final_mojo",
    }
    summary_path = project_path("outputs/metrics/streaming_summary.json")
    with summary_path.open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, ensure_ascii=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    success = (
        totals["rows"] == total_rows
        and totals["null_predictions"] == 0
        and totals["batches"] >= args.batches
    )
    spark.stop()
    print("FINAL STREAMING DEMO SUCCESS" if success else "FINAL STREAMING DEMO FAILED")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
