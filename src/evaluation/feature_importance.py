"""Extract real feature importance from the exported final GBM metadata."""

from __future__ import annotations

import csv
import json

import matplotlib.pyplot as plt

from src.common.project import project_path


def load_importance() -> list[dict]:
    metadata_path = project_path("models/final_mojo/metadata/part-00000")
    with metadata_path.open("r", encoding="utf-8") as stream:
        metadata = json.load(stream)
    param_map = metadata["paramMap"]
    serialized = param_map.get("featureImportances", {}).get("value", {})
    rows = serialized.get("rows", [])
    if rows:
        return [
            {
                "feature": row["Variable"],
                "relative_importance": row["Relative Importance"],
                "scaled_importance": row["Scaled Importance"],
                "percentage": row["Percentage"],
            }
            for row in rows
        ]
    details = json.loads(param_map["modelDetails"])
    variable_importances = details["variable_importances"]
    names = variable_importances["data"][0]
    relative, scaled, percentage = variable_importances["data"][1:4]
    return [
        {
            "feature": name,
            "relative_importance": rel,
            "scaled_importance": scale,
            "percentage": pct,
        }
        for name, rel, scale, pct in zip(names, relative, scaled, percentage)
    ]


def main() -> int:
    rows = sorted(load_importance(), key=lambda row: row["percentage"], reverse=True)
    metrics_path = project_path("outputs/metrics/feature_importance.csv")
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    chart_path = project_path("outputs/charts/feature_importance.png")
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    chart_rows = list(reversed(rows))
    plt.figure(figsize=(11, 7))
    bars = plt.barh(
        [row["feature"] for row in chart_rows],
        [row["percentage"] * 100 for row in chart_rows],
        color="#70AD47",
    )
    for bar in bars:
        plt.text(
            bar.get_width(),
            bar.get_y() + bar.get_height() / 2,
            f" {bar.get_width():.2f}%",
            va="center",
            fontsize=9,
        )
    plt.title("Final GBM feature importance")
    plt.xlabel("Percentage importance")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=300)
    plt.close()
    print(f"Saved {metrics_path}")
    print(f"Saved {chart_path}")
    print("FEATURE IMPORTANCE SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
