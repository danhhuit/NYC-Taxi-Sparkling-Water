"""Create the required final model-comparison charts."""

from __future__ import annotations

import csv

import matplotlib.pyplot as plt

from src.common.project import project_path


def chart(models: list[str], values: list[float], metric: str, filename: str) -> None:
    plt.figure(figsize=(11, 6))
    bars = plt.bar(models, values, color="#2F75B5")
    for bar, value in zip(bars, values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.4f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    plt.title(f"Final chronological model comparison — {metric.upper()}")
    plt.xlabel("Model")
    plt.ylabel(metric.upper() if metric == "r2" else f"{metric.upper()} (minutes)")
    plt.xticks(rotation=18, ha="right")
    if metric == "r2":
        plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(project_path(f"outputs/charts/{filename}"), dpi=300)
    plt.close()


def main() -> int:
    source = project_path("outputs/metrics/model_comparison.csv")
    with source.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    output_dir = project_path("outputs/charts")
    output_dir.mkdir(parents=True, exist_ok=True)
    models = [row["model"] for row in rows]
    chart(models, [float(row["rmse"]) for row in rows], "rmse", "rmse_comparison.png")
    chart(models, [float(row["mae"]) for row in rows], "mae", "mae_comparison.png")
    chart(models, [float(row["r2"]) for row in rows], "r2", "r2_comparison.png")
    print("METRICS VISUALIZATION SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
