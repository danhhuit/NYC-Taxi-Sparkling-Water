"""Import the full NYC Taxi frame into standalone H2O and capture real UI/API evidence.

Run this only after scripts/start_h2o_flow_full.cmd has passed its RAM preflight.
The script never substitutes a sample frame: it fails if the parsed frame does not
contain the expected 46,714,819 rows.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import h2o
from h2o.estimators.gbm import H2OGradientBoostingEstimator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
IMPORT_PATH = PROJECT_ROOT / "data" / "flow" / "full_12_months"
SCREENSHOTS = PROJECT_ROOT / "docs" / "screenshots"
EXPECTED_ROWS = 46_714_819
FRAME_KEY = "nyc_taxi_full_12_months"
MODEL_ID = "nyc_taxi_flow_gbm"
PREDICTION_KEY = "nyc_taxi_flow_predictions_10000"
RESPONSE = "trip_duration_minutes"
PREFERRED_FEATURES = [
    "pickup_location",
    "dropoff_location",
    "passenger_count",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "is_weekend",
    "route_trip_count",
    "route_avg_duration",
    "hour_route_avg_duration",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:54321")
    parser.add_argument("--prediction-rows", type=int, default=10_000)
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_ROWS)
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help="Reuse the named H2O frame/model when present; otherwise import/train.",
    )
    return parser.parse_args()


def existing_frame(key: str):
    try:
        return h2o.get_frame(key)
    except (EnvironmentError, ValueError):
        return None


def existing_model(key: str):
    try:
        return h2o.get_model(key)
    except (EnvironmentError, ValueError):
        return None


def chrome_path() -> str:
    candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    located = shutil.which("chrome") or shutil.which("chrome.exe")
    if located:
        return located
    raise RuntimeError("Google Chrome was not found; cannot capture required evidence.")


def capture(chrome: str, url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    profile = target.parent / ".chrome-h2o-flow-capture"
    command = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--hide-scrollbars",
        "--no-first-run",
        f"--user-data-dir={profile}",
        "--window-size=1600,1200",
        "--virtual-time-budget=5000",
        f"--screenshot={target}",
        url,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=45)
    if completed.returncode != 0 or not target.is_file() or target.stat().st_size == 0:
        raise RuntimeError(
            f"Chrome screenshot failed for {url}: {completed.stderr.strip() or completed.stdout.strip()}"
        )


def main() -> int:
    args = parse_args()
    if not IMPORT_PATH.is_dir() or not any(IMPORT_PATH.glob("*.parquet")):
        raise RuntimeError(f"No staged Parquet files found in {IMPORT_PATH}")

    h2o.connect(url=args.url, verbose=False)
    frame = existing_frame(FRAME_KEY) if args.reuse_existing else None
    if frame is None:
        print(f"Importing all Parquet files from {IMPORT_PATH} into {FRAME_KEY} ...", flush=True)
        frame = h2o.import_file(
            path=IMPORT_PATH.resolve().as_posix(),
            destination_frame=FRAME_KEY,
            parse=True,
        )

    row_count = frame.nrows
    if row_count != args.expected_rows:
        raise RuntimeError(
            f"Full-frame verification failed: expected {args.expected_rows:,} rows, got {row_count:,}. "
            "No screenshots were created."
        )
    if RESPONSE not in frame.columns:
        raise RuntimeError(f"Response column {RESPONSE!r} is absent from imported frame.")

    missing = [name for name in PREFERRED_FEATURES if name not in frame.columns]
    if missing:
        raise RuntimeError(f"Expected feature columns are absent: {', '.join(missing)}")

    model = existing_model(MODEL_ID) if args.reuse_existing else None
    if model is None:
        print("Training the reproducible Flow GBM on the verified full frame ...", flush=True)
        model = H2OGradientBoostingEstimator(
            model_id=MODEL_ID,
            ntrees=10,
            max_depth=5,
            min_rows=100,
            learn_rate=0.1,
            sample_rate=0.7,
            col_sample_rate=0.8,
            seed=42,
            score_tree_interval=1,
        )
        model.train(x=PREFERRED_FEATURES, y=RESPONSE, training_frame=frame)

    prediction_input = frame[: args.prediction_rows, PREFERRED_FEATURES]
    prediction_frame = model.predict(prediction_input)
    prediction_frame.set_name(PREDICTION_KEY)
    # Forces retrieval of the same model metrics shown by H2O/Flow.
    metrics = model.model_performance(train=True)

    base = args.url.rstrip("/")
    encoded_frame = quote(FRAME_KEY, safe="")
    encoded_model = quote(model.model_id, safe="")
    encoded_prediction = quote(PREDICTION_KEY, safe="")
    chrome = chrome_path()
    evidence_urls = {
        "h2o_flow_cluster.png": f"{base}/flow/index.html",
        "h2o_flow_frame.png": f"{base}/3/Frames/{encoded_frame}",
        "h2o_flow_gbm.png": f"{base}/3/Models/{encoded_model}",
        "h2o_flow_metrics.png": f"{base}/3/ModelMetrics/models/{encoded_model}/frames/{encoded_frame}",
        # The model response contains output.variable_importances from H2O itself.
        "h2o_flow_importance.png": f"{base}/3/Models/{encoded_model}",
        "h2o_flow_prediction.png": f"{base}/3/Frames/{encoded_prediction}",
    }
    for name, url in evidence_urls.items():
        print(f"Capturing {name} from {url}", flush=True)
        capture(chrome, url, SCREENSHOTS / name)

    manifest = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "cluster_url": base,
        "frame_key": FRAME_KEY,
        "frame_rows": row_count,
        "model_id": model.model_id,
        "response_column": RESPONSE,
        "features": PREFERRED_FEATURES,
        "prediction_frame": PREDICTION_KEY,
        "prediction_rows": prediction_frame.nrows,
        "training_rmse": metrics.rmse(),
        "training_mae": metrics.mae(),
        "training_r2": metrics.r2(),
        "evidence_urls": evidence_urls,
    }
    (SCREENSHOTS / "h2o_flow_evidence_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"H2O Flow full-data capture failed: {error}", file=sys.stderr)
        raise SystemExit(1)
