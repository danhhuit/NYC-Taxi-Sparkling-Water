import os
import csv
import matplotlib.pyplot as plt


INPUT_FILE = (
    "outputs/metrics/"
    "model_comparison.csv"
)

OUTPUT_DIR = (
    "outputs/metrics/charts"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ==========================================================
# LOAD MODEL COMPARISON
# ==========================================================

models = []
rmse_values = []
mae_values = []
r2_values = []


with open(
    INPUT_FILE,
    "r",
    encoding="utf-8-sig"
) as f:

    reader = csv.DictReader(f)

    for row in reader:

        models.append(
            row["model"]
        )

        rmse_values.append(
            float(row["rmse"])
        )

        mae_values.append(
            float(row["mae"])
        )

        r2_values.append(
            float(row["r2"])
        )


print("=" * 70)
print("NYC TAXI - MODEL METRICS VISUALIZATION")
print("=" * 70)

print("Models loaded:", len(models))


# ==========================================================
# FUNCTION: ADD VALUES ABOVE BARS
# ==========================================================

def add_labels(bars, decimals=4):

    for bar in bars:

        value = bar.get_height()

        plt.text(
            bar.get_x()
            + bar.get_width() / 2,

            value,

            f"{value:.{decimals}f}",

            ha="center",
            va="bottom",
            fontsize=9
        )


# ==========================================================
# 1. RMSE CHART
# ==========================================================

plt.figure(
    figsize=(10, 6)
)

bars = plt.bar(
    models,
    rmse_values
)

add_labels(bars)

plt.title(
    "Model Comparison - RMSE"
)

plt.xlabel(
    "Model"
)

plt.ylabel(
    "RMSE (minutes)"
)

plt.xticks(
    rotation=20,
    ha="right"
)

plt.tight_layout()

rmse_path = os.path.join(
    OUTPUT_DIR,
    "model_rmse_comparison.png"
)

plt.savefig(
    rmse_path,
    dpi=300
)

plt.close()


# ==========================================================
# 2. MAE CHART
# ==========================================================

plt.figure(
    figsize=(10, 6)
)

bars = plt.bar(
    models,
    mae_values
)

add_labels(bars)

plt.title(
    "Model Comparison - MAE"
)

plt.xlabel(
    "Model"
)

plt.ylabel(
    "MAE (minutes)"
)

plt.xticks(
    rotation=20,
    ha="right"
)

plt.tight_layout()

mae_path = os.path.join(
    OUTPUT_DIR,
    "model_mae_comparison.png"
)

plt.savefig(
    mae_path,
    dpi=300
)

plt.close()


# ==========================================================
# 3. R2 CHART
# ==========================================================

plt.figure(
    figsize=(10, 6)
)

bars = plt.bar(
    models,
    r2_values
)

add_labels(bars)

plt.title(
    "Model Comparison - R2"
)

plt.xlabel(
    "Model"
)

plt.ylabel(
    "R2 Score"
)

plt.xticks(
    rotation=20,
    ha="right"
)

plt.ylim(
    0,
    1
)

plt.tight_layout()

r2_path = os.path.join(
    OUTPUT_DIR,
    "model_r2_comparison.png"
)

plt.savefig(
    r2_path,
    dpi=300
)

plt.close()


# ==========================================================
# FINISH
# ==========================================================

print("\nCHARTS SAVED:")

print(rmse_path)
print(mae_path)
print(r2_path)

print("\n" + "=" * 70)
print("METRICS VISUALIZATION SUCCESS")
print("=" * 70)