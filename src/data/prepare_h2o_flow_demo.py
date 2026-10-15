"""Prepare deterministic medium-size train/test Parquet files for H2O Flow.

The local H2O cluster has a 2 GB heap. Loading the full 46.7-million-row frame
is not appropriate for that cluster, while a tiny 1,000-row CSV does not
demonstrate a meaningful modeling workflow. This module creates:

* 200,000 rows sampled from the chronological train split.
* 50,000 rows sampled from the chronological test split.

Each output is a single Parquet file so it is straightforward to import
manually in H2O Flow without accidentally selecting Spark CRC/SUCCESS files.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.modeling import FEATURE_COLUMNS, LABEL_COLUMN
from src.common.project import load_config, project_path, spark_path


DEFAULT_TRAIN_ROWS = 200_000
DEFAULT_TEST_ROWS = 50_000
DEFAULT_SEED = 42
OUTPUT_DIRECTORY = project_path("data/flow/local_demo")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train-rows", type=int, default=DEFAULT_TRAIN_ROWS)
    parser.add_argument("--test-rows", type=int, default=DEFAULT_TEST_ROWS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def split_counts() -> dict[str, int]:
    summary_path = project_path("outputs/metrics/split_summary.csv")
    if not summary_path.is_file():
        raise FileNotFoundError(
            f"Missing split summary: {summary_path}. Run the data pipeline first."
        )
    with summary_path.open("r", encoding="utf-8-sig", newline="") as stream:
        return {row["split"]: int(row["rows"]) for row in csv.DictReader(stream)}


def medium_sample(
    frame: DataFrame,
    full_rows: int,
    requested_rows: int,
    seed: int,
) -> DataFrame:
    if requested_rows <= 0:
        raise ValueError("Requested rows must be positive.")
    if full_rows < requested_rows:
        raise ValueError(
            f"Cannot sample {requested_rows:,} rows from a {full_rows:,}-row split."
        )

    # Oversample by 25%, then cap. This avoids a global random sort over tens of
    # millions of rows while reliably producing the exact requested row count.
    fraction = min(1.0, requested_rows * 1.25 / full_rows)
    return (
        frame.select(*(FEATURE_COLUMNS + [LABEL_COLUMN]))
        .dropna(subset=FEATURE_COLUMNS + [LABEL_COLUMN])
        .sample(withReplacement=False, fraction=fraction, seed=seed)
        .limit(requested_rows)
    )


def write_single_parquet(
    frame: DataFrame,
    target: Path,
    expected_rows: int,
) -> dict[str, object]:
    temporary = target.parent / f".{target.stem}_spark_tmp"
    if temporary.exists():
        shutil.rmtree(temporary)
    if target.exists():
        target.unlink()

    frame.coalesce(1).write.mode("overwrite").parquet(temporary.resolve().as_posix())
    part_files = list(temporary.glob("part-*.parquet"))
    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one Parquet part in {temporary}, found {len(part_files)}."
        )
    shutil.move(str(part_files[0]), target)
    shutil.rmtree(temporary)

    verified_rows = frame.count()
    if verified_rows != expected_rows:
        raise RuntimeError(
            f"Expected {expected_rows:,} rows for {target.name}, got {verified_rows:,}."
        )

    return {
        "path": target.resolve().as_posix(),
        "rows": verified_rows,
        "size_mb": round(target.stat().st_size / (1024 * 1024), 2),
    }


def main() -> int:
    args = parse_args()
    if args.train_rows <= 0 or args.test_rows <= 0:
        raise ValueError("--train-rows and --test-rows must be positive.")

    config = load_config()
    counts = split_counts()
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    train_target = OUTPUT_DIRECTORY / f"h2o_train_demo_{args.train_rows}.parquet"
    test_target = OUTPUT_DIRECTORY / f"h2o_test_demo_{args.test_rows}.parquet"
    manifest_path = OUTPUT_DIRECTORY / "manifest.json"

    if (
        not args.force
        and train_target.is_file()
        and test_target.is_file()
        and manifest_path.is_file()
    ):
        print("H2O FLOW LOCAL DEMO DATA ALREADY READY")
        print(train_target.resolve())
        print(test_target.resolve())
        return 0

    spark = (
        SparkSession.builder.appName("Prepare-H2O-Flow-Local-Demo")
        .master(config["spark"]["master"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        train_source = spark.read.parquet(spark_path(config["paths"]["train"]))
        test_source = spark.read.parquet(spark_path(config["paths"]["test"]))
        train_sample = medium_sample(
            train_source, counts["train"], args.train_rows, args.seed
        ).cache()
        test_sample = medium_sample(
            test_source, counts["test"], args.test_rows, args.seed + 1
        ).cache()

        train_count = train_sample.count()
        test_count = test_sample.count()
        if train_count != args.train_rows or test_count != args.test_rows:
            raise RuntimeError(
                "Sampling did not produce the requested counts: "
                f"train={train_count:,}/{args.train_rows:,}, "
                f"test={test_count:,}/{args.test_rows:,}."
            )

        train_info = write_single_parquet(
            train_sample, train_target, args.train_rows
        )
        test_info = write_single_parquet(test_sample, test_target, args.test_rows)

        manifest = {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "purpose": "Medium-size local H2O Flow GUI modeling demo",
            "seed": args.seed,
            "sampling": "deterministic random sample from chronological splits",
            "train_source_rows": counts["train"],
            "test_source_rows": counts["test"],
            "train": train_info,
            "test": test_info,
            "features": FEATURE_COLUMNS,
            "response": LABEL_COLUMN,
            "notes": [
                "Train and test remain separate.",
                "These files are for GUI demonstration, not the final reported model.",
            ],
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print("=" * 70)
        print("H2O FLOW LOCAL DEMO DATA READY")
        print("=" * 70)
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
