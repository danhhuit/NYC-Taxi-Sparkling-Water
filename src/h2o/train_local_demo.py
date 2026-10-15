"""Train and validate the reproducible medium-size H2O Flow DRF demo."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import h2o
from h2o.estimators import H2ORandomForestEstimator

from src.common.modeling import FEATURE_COLUMNS, LABEL_COLUMN
from src.common.project import project_path


TRAIN_FRAME = "nyc_taxi_demo_train_200k"
TEST_FRAME = "nyc_taxi_demo_test_50k"
MODEL_ID = "nyc_taxi_flow_demo_drf"
PREDICTION_FRAME = "nyc_taxi_flow_demo_predictions_50k"
METRICS_PATH = project_path("outputs/metrics/h2o_flow_demo_metrics.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:54321")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def get_required_frame(frame_id: str, expected_rows: int):
    frame = h2o.get_frame(frame_id)
    if frame is None:
        raise RuntimeError(
            f"Missing H2O frame {frame_id}. Run scripts\\load_h2o_flow_demo.cmd first."
        )
    if frame.nrows != expected_rows:
        raise RuntimeError(
            f"Frame {frame_id} has {frame.nrows:,} rows; expected {expected_rows:,}."
        )
    return frame


def main() -> int:
    args = parse_args()
    h2o.connect(url=args.url, verbose=False)
    train = get_required_frame(TRAIN_FRAME, 200_000)
    test = get_required_frame(TEST_FRAME, 50_000)

    model = h2o.get_model(MODEL_ID)
    if model is not None and args.force:
        h2o.remove(model)
        model = None
    if model is None:
        model = H2ORandomForestEstimator(
            model_id=MODEL_ID,
            ntrees=30,
            max_depth=12,
            min_rows=20,
            sample_rate=0.8,
            mtries=-1,
            seed=42,
            score_each_iteration=True,
        )
        model.train(
            x=FEATURE_COLUMNS,
            y=LABEL_COLUMN,
            training_frame=train,
            validation_frame=test,
        )

    old_prediction = h2o.get_frame(PREDICTION_FRAME)
    if old_prediction is not None:
        h2o.remove(old_prediction)
    prediction = h2o.assign(model.predict(test), PREDICTION_FRAME)
    validation = model.model_performance(test)

    result = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "cluster": args.url,
        "model_id": MODEL_ID,
        "algorithm": "DRF",
        "train_frame": TRAIN_FRAME,
        "train_rows": train.nrows,
        "test_frame": TEST_FRAME,
        "test_rows": test.nrows,
        "prediction_frame": PREDICTION_FRAME,
        "prediction_rows": prediction.nrows,
        "validation_rmse": validation.rmse(),
        "validation_mae": validation.mae(),
        "validation_r2": validation.r2(),
        "features": FEATURE_COLUMNS,
        "response": LABEL_COLUMN,
        "parameters": {
            "ntrees": 30,
            "max_depth": 12,
            "min_rows": 20,
            "sample_rate": 0.8,
            "mtries": -1,
            "seed": 42,
        },
        "note": (
            "Medium-size GUI demonstration metrics; official final-model metrics "
            "remain in outputs/metrics/final_model_metrics.json."
        ),
    }
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print("=" * 70)
    print("H2O FLOW MEDIUM DRF DEMO SUCCESS")
    print("=" * 70)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
