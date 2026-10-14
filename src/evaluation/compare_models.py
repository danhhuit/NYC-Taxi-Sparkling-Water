"""Compare only final chronological model results."""

from __future__ import annotations

import csv
import json

from src.common.project import project_path


def load_json(filename: str) -> dict:
    with project_path(f"outputs/metrics/{filename}").open("r", encoding="utf-8") as stream:
        return json.load(stream)


def metric_block(payload: dict) -> dict:
    return payload.get("test_metrics", payload)


def make_row(model: str, payload: dict) -> dict:
    metrics = metric_block(payload)
    sample = payload.get("sample_rows", {})
    full = payload.get("full_rows", {})
    return {
        "model": model,
        "rmse": metrics["rmse"],
        "mae": metrics["mae"],
        "r2": metrics["r2"],
        "split_strategy": payload.get("split_strategy", "chronological"),
        "train_rows": sample.get("train", payload.get("train_rows", "")),
        "validation_rows": sample.get("validation", payload.get("validation_rows", "")),
        "test_rows": sample.get("test", payload.get("test_rows", "")),
        "full_train_rows": full.get("train", ""),
        "full_validation_rows": full.get("validation", ""),
        "full_test_rows": full.get("test", ""),
    }


def main() -> int:
    sources = [
        ("GBM", "gbm_metrics.json"),
        ("DRF", "drf_metrics.json"),
        ("Deep Learning", "deep_learning_metrics.json"),
        ("GBM Grid Search", "gbm_grid_search_metrics.json"),
        ("AutoML", "automl_test_metrics.json"),
    ]
    rows = [make_row(name, load_json(filename)) for name, filename in sources]
    rows.sort(key=lambda item: item["rmse"])

    metrics_dir = project_path("outputs/metrics")
    csv_path = metrics_dir / "model_comparison.csv"
    json_path = metrics_dir / "model_comparison.json"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with json_path.open("w", encoding="utf-8") as stream:
        json.dump(rows, stream, indent=2, ensure_ascii=False)

    print("=" * 90)
    print(f"{'Model':<22}{'RMSE':>12}{'MAE':>12}{'R2':>12}{'Test rows':>16}")
    print("-" * 90)
    for row in rows:
        print(
            f"{row['model']:<22}{row['rmse']:>12.4f}{row['mae']:>12.4f}"
            f"{row['r2']:>12.4f}{str(row['test_rows']):>16}"
        )
    print(f"\nLOWEST RMSE MODEL: {rows[0]['model']}")
    print("MODEL COMPARISON SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
