import os
import json
import csv


METRICS_DIR = "outputs/metrics"

OUTPUT_CSV = os.path.join(
    METRICS_DIR,
    "model_comparison.csv"
)

OUTPUT_JSON = os.path.join(
    METRICS_DIR,
    "model_comparison.json"
)


# ==========================================================
# LOAD JSON
# ==========================================================

def load_json(filename):
    path = os.path.join(
        METRICS_DIR,
        filename
    )

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as f:
        return json.load(f)


# ==========================================================
# 1. GBM BASELINE
# ==========================================================

gbm = load_json(
    "gbm_baseline_metrics.json"
)

models = [
    {
        "model": "GBM Baseline",
        "rmse": gbm["rmse"],
        "mae": gbm["mae"],
        "r2": gbm["r2"]
    }
]


# ==========================================================
# 2. DRF
# ==========================================================

drf = load_json(
    "drf_metrics.json"
)

models.append(
    {
        "model": "DRF",
        "rmse": drf["rmse"],
        "mae": drf["mae"],
        "r2": drf["r2"]
    }
)


# ==========================================================
# 3. DEEP LEARNING
# ==========================================================

dl = load_json(
    "deep_learning_metrics.json"
)

models.append(
    {
        "model": "Deep Learning",
        "rmse": dl["rmse"],
        "mae": dl["mae"],
        "r2": dl["r2"]
    }
)


# ==========================================================
# 4. GBM GRID SEARCH
# ==========================================================

grid = load_json(
    "gbm_grid_search_metrics.json"
)

models.append(
    {
        "model": "GBM Grid Search",
        "rmse": grid[
            "test_metrics"
        ]["rmse"],

        "mae": grid[
            "test_metrics"
        ]["mae"],

        "r2": grid[
            "test_metrics"
        ]["r2"]
    }
)


# ==========================================================
# 5. AUTOML
# ==========================================================

automl = load_json(
    "automl_metrics.json"
)

models.append(
    {
        "model": "AutoML",
        "rmse": automl["rmse"],
        "mae": automl["mae"],
        "r2": automl["r2"]
    }
)


# ==========================================================
# SORT BY RMSE
# ==========================================================

models_sorted = sorted(
    models,
    key=lambda x: x["rmse"]
)


# ==========================================================
# PRINT RESULT
# ==========================================================

print("=" * 72)

print(
    "NYC TAXI - MODEL COMPARISON"
)

print("=" * 72)

print(
    f"{'Model':<20}"
    f"{'RMSE':>12}"
    f"{'MAE':>12}"
    f"{'R2':>12}"
)

print("-" * 72)


for model in models_sorted:

    print(
        f"{model['model']:<20}"
        f"{model['rmse']:>12.4f}"
        f"{model['mae']:>12.4f}"
        f"{model['r2']:>12.4f}"
    )


print("-" * 72)

print(
    "LOWEST RMSE MODEL:",
    models_sorted[0]["model"]
)


# ==========================================================
# SAVE CSV
# ==========================================================

with open(
    OUTPUT_CSV,
    "w",
    newline="",
    encoding="utf-8-sig"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=[
            "model",
            "rmse",
            "mae",
            "r2"
        ]
    )

    writer.writeheader()

    writer.writerows(
        models_sorted
    )


# ==========================================================
# SAVE JSON
# ==========================================================

with open(
    OUTPUT_JSON,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        models_sorted,
        f,
        indent=4,
        ensure_ascii=False
    )


print("\nSAVED:")
print(OUTPUT_CSV)
print(OUTPUT_JSON)

print("=" * 72)
print("MODEL COMPARISON SUCCESS")
print("=" * 72)