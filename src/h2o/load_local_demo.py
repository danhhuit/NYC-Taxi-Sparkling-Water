"""Load the medium train/test demo files into an already running H2O cluster."""

from __future__ import annotations

import argparse
import json

import h2o

from src.common.project import project_path


TRAIN_PATH = project_path(
    "data/flow/local_demo/h2o_train_demo_200000.parquet"
)
TEST_PATH = project_path(
    "data/flow/local_demo/h2o_test_demo_50000.parquet"
)
TRAIN_FRAME = "nyc_taxi_demo_train_200k"
TEST_FRAME = "nyc_taxi_demo_test_50k"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:54321")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def load_frame(path, frame_id: str, expected_rows: int, force: bool):
    existing = h2o.get_frame(frame_id)
    if existing is not None and not force:
        if existing.nrows != expected_rows:
            raise RuntimeError(
                f"Existing frame {frame_id} has {existing.nrows:,} rows; "
                f"expected {expected_rows:,}. Use --force to replace it."
            )
        return existing
    if existing is not None:
        h2o.remove(existing)
    frame = h2o.import_file(
        path=path.resolve().as_posix(),
        destination_frame=frame_id,
        parse=True,
    )
    if frame.nrows != expected_rows:
        raise RuntimeError(
            f"Frame {frame_id} has {frame.nrows:,} rows; expected {expected_rows:,}."
        )
    return frame


def main() -> int:
    args = parse_args()
    for path in (TRAIN_PATH, TEST_PATH):
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing {path}. Run scripts\\prepare_h2o_flow_demo.cmd first."
            )

    h2o.connect(url=args.url, verbose=False)
    train = load_frame(TRAIN_PATH, TRAIN_FRAME, 200_000, args.force)
    test = load_frame(TEST_PATH, TEST_FRAME, 50_000, args.force)

    result = {
        "cluster": args.url,
        "train_frame": TRAIN_FRAME,
        "train_rows": train.nrows,
        "test_frame": TEST_FRAME,
        "test_rows": test.nrows,
        "columns": train.columns,
        "response": "trip_duration_minutes",
    }
    print("=" * 70)
    print("H2O FLOW MEDIUM DEMO FRAMES READY")
    print("=" * 70)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
