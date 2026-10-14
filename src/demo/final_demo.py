"""Read-only final-project proof that summarizes every completed pipeline stage."""

from __future__ import annotations

import csv
import json
import platform
import sys
from pathlib import Path

from src.common.project import PROJECT_ROOT, load_config, project_path


def exists(path: str | Path) -> bool:
    return project_path(path).exists()


def print_csv(path: str | Path, max_rows: int = 10) -> None:
    full_path = project_path(path)
    if not full_path.exists():
        print(f"MISSING: {full_path.relative_to(PROJECT_ROOT)}")
        return
    with full_path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    print(f"{full_path.name}: {len(rows)} row(s)")
    for row in rows[:max_rows]:
        print("  " + " | ".join(f"{key}={value}" for key, value in row.items()))


def print_json(path: str | Path) -> None:
    full_path = project_path(path)
    if not full_path.exists():
        print(f"MISSING: {full_path.relative_to(PROJECT_ROOT)}")
        return
    with full_path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def section(number: int, title: str) -> None:
    print("\n" + "=" * 78)
    print(f"[{number}] {title}")
    print("=" * 78)


def main() -> int:
    config = load_config()
    section(1, "Environment")
    print(f"Python: {platform.python_version()}")
    print(f"Platform: {platform.platform()}")
    print(f"Config: {config['_config_path']}")
    print(f"Environment log: {exists('outputs/logs/environment_check.log')}")

    section(2, "Load processed data")
    for label, path in (
        ("clean", config["paths"]["clean"]),
        ("features", config["paths"]["features"]),
        ("multisource", config["paths"]["multisource"]),
        ("train", config["paths"]["train"]),
        ("validation", config["paths"]["validation"]),
        ("test", config["paths"]["test"]),
    ):
        print(f"{label:12s}: {'READY' if exists(path) else 'MISSING'} - {path}")

    section(3, "Dataset summary")
    print_csv("outputs/metrics/raw_dataset_summary.csv")
    print_csv("outputs/metrics/cleaning_summary.csv")
    print_csv("outputs/metrics/split_summary.csv")
    print_csv("outputs/metrics/multisource_join_summary.csv")

    section(4, "Model comparison")
    print_csv("outputs/metrics/model_comparison.csv")
    print_csv("outputs/metrics/gbm_grid_results.csv")
    print_csv("outputs/metrics/automl_leaderboard.csv")

    section(5, "Best model")
    print_json("outputs/metrics/final_model_metrics.json")

    section(6, "MOJO load")
    mojo_ready = exists("models/final_mojo")
    print(f"Final MOJO: {'READY' if mojo_ready else 'MISSING'}")

    section(7, "Batch scoring")
    batch_ready = exists("outputs/predictions/batch_predictions")
    print(f"Batch predictions: {'READY' if batch_ready else 'MISSING'}")
    print(
        "MOJO predictions: "
        f"{'READY' if exists('outputs/predictions/mojo_predictions') else 'MISSING'}"
    )

    section(8, "Prediction summary")
    print_json("outputs/metrics/mojo_scoring_summary.json")

    section(9, "Streaming proof")
    streaming_ready = exists("outputs/streaming/final_predictions")
    print(f"Streaming output: {'READY' if streaming_ready else 'MISSING'}")
    print_json("outputs/metrics/streaming_summary.json")

    section(10, "Deployment proof")
    print(f"Dockerfile: {exists('docker/Dockerfile')}")
    print(f"Kubernetes final job: {exists('kubernetes/final-job.yaml')}")
    print(f"H2O Flow script: {exists('scripts/start_h2o_flow_full.cmd')}")
    print(f"spark-submit log: {exists('outputs/logs/spark_submit.log')}")

    required = [
        config["paths"]["train"],
        config["paths"]["validation"],
        config["paths"]["test"],
        "outputs/metrics/gbm_grid_results.csv",
        "outputs/metrics/gbm_best_params.json",
        "outputs/metrics/automl_leaderboard.csv",
        "outputs/metrics/automl_test_metrics.json",
        "outputs/metrics/model_comparison.csv",
        "outputs/metrics/feature_importance.csv",
        "outputs/charts/rmse_comparison.png",
        "outputs/charts/mae_comparison.png",
        "outputs/charts/r2_comparison.png",
        "outputs/charts/feature_importance.png",
        "models/final_mojo",
        "outputs/metrics/mojo_scoring_summary.json",
        "outputs/predictions/mojo_predictions",
        "outputs/metrics/streaming_summary.json",
        "outputs/streaming/final_predictions",
        "outputs/logs/spark_submit.log",
    ]
    missing = [path for path in required if not exists(path)]
    if missing:
        print("\nFINAL INTEGRATED DEMO INCOMPLETE")
        print("Missing:")
        for path in missing:
            print(f"- {path}")
        return 2

    print("\nFINAL INTEGRATED DEMO SUCCESS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
