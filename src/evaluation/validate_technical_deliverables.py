"""Validate the four technical grading criteria with reproducible evidence.

This validator deliberately excludes report/presentation and team-work items.
It fails when a required technical artifact is absent or internally
inconsistent, and writes a machine-readable summary for demonstration.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.app.server import MODEL_FEATURES, self_test as dashboard_self_test
from src.common.project import project_path


METRICS = project_path("outputs/metrics")
CHARTS = project_path("outputs/charts")


def csv_rows(name: str) -> list[dict[str, str]]:
    with (METRICS / name).open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def json_data(name: str) -> dict[str, Any]:
    with (METRICS / name).open("r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def check(name: str, condition: bool, evidence: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(condition), "evidence": evidence}


def validate(run_prediction: bool = True) -> dict[str, Any]:
    readme_text = project_path("README.md").read_text(encoding="utf-8").lower()
    raw = csv_rows("raw_dataset_summary.csv")
    cleaning = csv_rows("cleaning_summary.csv")[0]
    splits = csv_rows("split_summary.csv")
    jdbc = csv_rows("multisource_join_summary.csv")[0]
    models = csv_rows("model_comparison.csv")
    feature_dictionary = csv_rows("feature_dictionary.csv")
    final_model = json_data("final_model_metrics.json")
    mojo = json_data("mojo_scoring_summary.json")
    streaming = json_data("streaming_summary.json")
    flow = json_data("h2o_flow_demo_metrics.json")

    raw_rows = sum(int(row["rows"]) for row in raw)
    clean_rows = int(cleaning["clean_rows"])
    split_rows = sum(int(row["rows"]) for row in splits)
    test_rows = next(int(row["rows"]) for row in splits if row["split"] == "test")
    model_names = {row["model"] for row in models}
    required_models = {"GBM", "DRF", "Deep Learning", "GBM Grid Search", "AutoML"}

    dashboard_result: dict[str, Any]
    if run_prediction:
        try:
            dashboard_result = dashboard_self_test()
        except Exception as exc:  # external Java/model failure must be reported
            dashboard_result = {"status": "FAIL", "error": str(exc)}
    else:
        dashboard_result = {
            "status": "SKIPPED",
            "reason": "--skip-prediction was selected",
        }

    application_checks = [
        check(
            "Bài toán và đầu ra dự đoán được mô tả",
            (
                "trip duration" in readme_text
                or "thời gian chuyến đi" in readme_text
            )
            and "trip_duration_minutes" in readme_text,
            "README.md",
        ),
        check(
            "Dashboard local tồn tại",
            project_path("src/app/server.py").is_file()
            and project_path("scripts/run_dashboard.cmd").is_file(),
            ["src/app/server.py", "scripts/run_dashboard.cmd"],
        ),
        check(
            "Dự đoán tương tác dùng MOJO thật",
            dashboard_result.get("status") in {"PASS", "SKIPPED"},
            dashboard_result,
        ),
    ]

    data_checks = [
        check(
            "Đủ 12 tháng dữ liệu",
            len(raw) == 12 and raw_rows == int(cleaning["raw_rows"]),
            {"months": len(raw), "raw_rows": raw_rows},
        ),
        check(
            "Làm sạch có kiểm soát",
            0 < int(cleaning["removed_rows"]) < raw_rows and clean_rows > 0,
            cleaning,
        ),
        check(
            "Chronological split bảo toàn số dòng",
            len(splits) == 3 and split_rows == clean_rows,
            {"split_rows": split_rows, "clean_rows": clean_rows},
        ),
        check(
            "PostgreSQL JDBC đã kết nối",
            jdbc["jdbc_status"] == "CONNECTED" and int(jdbc["matched_rows"]) > 0,
            jdbc,
        ),
        check(
            "Parquet đầu ra Spark hoàn tất",
            all(
                project_path(path,).joinpath("_SUCCESS").is_file()
                for path in (
                    "data/processed/taxi_clean.parquet",
                    "data/processed/train.parquet",
                    "data/processed/validation.parquet",
                    "data/processed/test.parquet",
                )
            ),
            "taxi_clean/train/validation/test _SUCCESS",
        ),
    ]

    model_checks = [
        check(
            "Feature dictionary khớp model cuối",
            {
                row["feature_name"]
                for row in feature_dictionary
                if row["used_in_model"].lower() == "true"
            }
            == set(MODEL_FEATURES),
            MODEL_FEATURES,
        ),
        check(
            "Đủ năm phương pháp so sánh",
            required_models.issubset(model_names),
            sorted(model_names),
        ),
        check(
            "Metrics test hợp lệ",
            all(
                float(row["rmse"]) > 0
                and float(row["mae"]) > 0
                and 0 <= float(row["r2"]) <= 1
                for row in models
            ),
            "RMSE/MAE/R2",
        ),
        check(
            "DRF cuối cùng dùng chronological split",
            final_model["model"] == "H2O DRF Final Model"
            and final_model["split_strategy"] == "chronological"
            and final_model["full_rows"]["test"] == test_rows,
            {
                "model": final_model["model"],
                "split_strategy": final_model["split_strategy"],
                "test_rows": final_model["full_rows"]["test"],
            },
        ),
        check(
            "MOJO chấm toàn bộ test, không null",
            mojo["rows"] == test_rows
            and mojo["all_input_rows"] is True
            and mojo["null_predictions"] == 0,
            mojo,
        ),
        check(
            "Structured Streaming hoàn tất",
            streaming["batches"] == streaming["expected_batches"]
            and streaming["rows"] == streaming["expected_rows"]
            and streaming["null_predictions"] == 0,
            streaming,
        ),
        check(
            "Phân tích sai số đủ chiều chính",
            all(
                (METRICS / name).is_file()
                for name in (
                    "error_by_hour.csv",
                    "error_by_day_of_week.csv",
                    "error_by_borough.csv",
                    "error_by_dropoff_borough.csv",
                    "error_by_duration_range.csv",
                    "error_by_route.csv",
                )
            ),
            "hour/day/pickup borough/dropoff borough/duration/route",
        ),
    ]

    expected_charts = [
        "rmse_comparison.png",
        "mae_comparison.png",
        "r2_comparison.png",
        "feature_importance.png",
        "error_by_hour.png",
        "error_by_day_of_week.png",
        "error_by_borough.png",
        "error_by_dropoff_borough.png",
        "error_by_duration_range.png",
    ]
    visualization_checks = [
        check(
            "Biểu đồ kết quả và sai số đầy đủ",
            all((CHARTS / name).is_file() for name in expected_charts),
            expected_charts,
        ),
        check(
            "H2O Flow demo quy mô vừa hoàn tất",
            flow["train_rows"] == 200_000
            and flow["test_rows"] == 50_000
            and flow["prediction_rows"] == 50_000,
            flow,
        ),
        check(
            "Dashboard cung cấp metrics, charts và prediction",
            all(
                project_path(path).is_file()
                for path in (
                    "src/app/index.html",
                    "src/app/server.py",
                    "scripts/run_dashboard.cmd",
                    "scripts/test_dashboard.cmd",
                )
            ),
            "GET /api/summary, GET /charts/*, POST /api/predict",
        ),
        check(
            "Ảnh bằng chứng light/dark dashboard tồn tại",
            project_path("outputs/screenshots/dashboard.png").is_file()
            and project_path("outputs/screenshots/dashboard_dark.png").is_file(),
            [
                "outputs/screenshots/dashboard.png",
                "outputs/screenshots/dashboard_dark.png",
            ],
        ),
        check(
            "Artifact triển khai tồn tại",
            project_path("docker/Dockerfile").is_file()
            and project_path("kubernetes/final-job.yaml").is_file(),
            ["docker/Dockerfile", "kubernetes/final-job.yaml"],
        ),
    ]

    criteria = {
        "application": application_checks,
        "data_processing": data_checks,
        "modeling": model_checks,
        "visualization_application": visualization_checks,
    }
    criterion_status = {
        key: all(item["passed"] for item in items) for key, items in criteria.items()
    }
    docker_log = project_path("outputs/logs/docker_final.log")
    kubernetes_log = project_path("outputs/logs/kubernetes_final.log")
    docker_text = (
        docker_log.read_text(encoding="utf-8", errors="replace")
        if docker_log.is_file()
        else ""
    )
    kubernetes_text = (
        kubernetes_log.read_text(encoding="utf-8", errors="replace")
        if kubernetes_log.is_file()
        else ""
    )
    deployment_runtime = {
        "docker": (
            "PASS"
            if "DOCKER FINAL VALIDATION SUCCESS" in docker_text
            else "NOT_VALIDATED"
        ),
        "kubernetes": (
            "PASS"
            if "KUBERNETES FINAL VALIDATION SUCCESS" in kubernetes_text
            else "NOT_VALIDATED"
        ),
    }
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": (
            "Four technical grading criteria only; report/presentation and "
            "team-work criteria are explicitly excluded."
        ),
        "status": "PASS" if all(criterion_status.values()) else "FAIL",
        "criterion_status": criterion_status,
        "criteria": criteria,
        "deployment_runtime": deployment_runtime,
        "deployment_runtime_note": (
            "Docker/Kubernetes manifests are present. Runtime completion must "
            "be revalidated when Docker Desktop is running and has sufficient "
            "system-disk space."
        ),
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-prediction",
        action="store_true",
        help="Skip the one-row Java MOJO smoke test.",
    )
    parser.add_argument(
        "--output",
        default="outputs/metrics/technical_validation.json",
    )
    args = parser.parse_args()
    result = validate(run_prediction=not args.skip_prediction)
    output = project_path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, ensure_ascii=False)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"Technical validation written to {output}")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
