import os
import csv
import matplotlib.pyplot as plt


OUTPUT_DIR = "outputs/metrics"
CHART_DIR = "outputs/metrics/charts"

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)

os.makedirs(
    CHART_DIR,
    exist_ok=True
)


# ==========================================================
# FEATURE IMPORTANCE
# Best GBM Grid Search:
# maxDepth=8, learnRate=0.05, ntrees=150
# ==========================================================

features = [
    "pickup_location",
    "dropoff_location",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "passenger_count"
]

importance = [
    0.521630,
    0.412256,
    0.041352,
    0.016244,
    0.006542,
    0.001976
]


# ==========================================================
# PRINT
# ==========================================================

print("=" * 70)
print("GBM GRID SEARCH - FEATURE IMPORTANCE")
print("=" * 70)

for feature, value in zip(
    features,
    importance
):
    print(
        f"{feature:<25}"
        f"{value:>10.6f} "
        f"({value * 100:>6.2f}%)"
    )


# ==========================================================
# SAVE CSV
# ==========================================================

csv_path = os.path.join(
    OUTPUT_DIR,
    "gbm_feature_importance.csv"
)

with open(
    csv_path,
    "w",
    newline="",
    encoding="utf-8-sig"
) as f:

    writer = csv.writer(f)

    writer.writerow(
        [
            "feature",
            "importance",
            "percentage"
        ]
    )

    for feature, value in zip(
        features,
        importance
    ):
        writer.writerow(
            [
                feature,
                value,
                value * 100
            ]
        )


# ==========================================================
# CHART
# ==========================================================

features_reverse = list(
    reversed(features)
)

importance_reverse = list(
    reversed(importance)
)


plt.figure(
    figsize=(10, 6)
)

bars = plt.barh(
    features_reverse,
    importance_reverse
)


for bar in bars:

    value = bar.get_width()

    plt.text(
        value,
        bar.get_y()
        + bar.get_height() / 2,

        f" {value * 100:.2f}%",

        va="center",
        fontsize=9
    )


plt.title(
    "GBM Grid Search - Feature Importance"
)

plt.xlabel(
    "Importance"
)

plt.ylabel(
    "Feature"
)

plt.tight_layout()


chart_path = os.path.join(
    CHART_DIR,
    "gbm_feature_importance.png"
)

plt.savefig(
    chart_path,
    dpi=300
)

plt.close()


print("\nSAVED:")
print(csv_path)
print(chart_path)

print("\n" + "=" * 70)
print("FEATURE IMPORTANCE SUCCESS")
print("=" * 70)