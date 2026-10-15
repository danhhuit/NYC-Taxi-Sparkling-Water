"""Dependency-free local dashboard backed by the exported DRF MOJO.

The application intentionally uses only Python's standard library.  It serves
the existing metrics/charts and performs a real one-row prediction through
H2O's ``PredictCsv`` tool and ``models/final_mojo/mojo_model``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import subprocess
import tempfile
import webbrowser
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from src.common.project import project_path


HOST = "127.0.0.1"
PORT = 8501
MODEL_PATH = project_path("models/final_mojo/mojo_model")
H2O_JAR = project_path(".venv/Lib/site-packages/h2o/backend/bin/h2o.jar")
METRICS_DIR = project_path("outputs/metrics")
CHARTS_DIR = project_path("outputs/charts")
ZONE_LOOKUP = project_path("data/raw/zones/taxi_zone_lookup.csv")

MODEL_FEATURES = [
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

MODEL_REGISTRY = {
    "drf": {
        "label": "DRF",
        "comparison_name": "DRF",
        "path": project_path("models/final_mojo/mojo_model"),
    },
    "gbm_grid": {
        "label": "GBM Grid Search",
        "comparison_name": "GBM Grid Search",
        "path": project_path("models/gbm_grid_best_mojo/mojo_model"),
    },
    "gbm": {
        "label": "GBM",
        "comparison_name": "GBM",
        "path": project_path("models/nyc_taxi_gbm_mojo/mojo_model"),
    },
    "automl": {
        "label": "AutoML",
        "comparison_name": "AutoML",
        "path": project_path("models/automl_best_mojo/mojo_model"),
    },
    "deep_learning": {
        "label": "Deep Learning",
        "comparison_name": "Deep Learning",
        "path": project_path("models/deep_learning_mojo/mojo_model"),
    },
}


class InputError(ValueError):
    """Raised when an API prediction request contains invalid input."""


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def load_summary() -> dict[str, Any]:
    cleaning = read_csv_rows(METRICS_DIR / "cleaning_summary.csv")[0]
    splits = read_csv_rows(METRICS_DIR / "split_summary.csv")
    models = read_csv_rows(METRICS_DIR / "model_comparison.csv")
    mojo = read_json(METRICS_DIR / "mojo_scoring_summary.json")
    streaming = read_json(METRICS_DIR / "streaming_summary.json")
    flow = read_json(METRICS_DIR / "h2o_flow_demo_metrics.json")
    return {
        "dataset": {
            "raw_rows": int(cleaning["raw_rows"]),
            "clean_rows": int(cleaning["clean_rows"]),
            "removed_rows": int(cleaning["removed_rows"]),
            "removed_percentage": float(cleaning["removed_percentage"]),
        },
        "splits": [
            {
                **row,
                "rows": int(row["rows"]),
                "percentage": float(row["percentage"]),
            }
            for row in splits
        ],
        "models": [
            {
                "model": row["model"],
                "rmse": float(row["rmse"]),
                "mae": float(row["mae"]),
                "r2": float(row["r2"]),
                "test_rows": int(row["test_rows"]),
            }
            for row in models
        ],
        "mojo": mojo,
        "streaming": streaming,
        "h2o_flow": flow,
    }


def load_analytics() -> dict[str, Any]:
    """Load the complete visualization payload from retained pipeline metrics."""

    monthly = [
        {
            "month": row["month"],
            "rows": int(row["rows"]),
            "columns": int(row["columns"]),
            "file_size_mb": float(row["file_size_mb"]),
            "min_pickup_time": row["min_pickup_time"],
            "max_pickup_time": row["max_pickup_time"],
        }
        for row in read_csv_rows(METRICS_DIR / "raw_dataset_summary.csv")
    ]
    splits = [
        {
            "split": row["split"],
            "rows": int(row["rows"]),
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "percentage": float(row["percentage"]),
        }
        for row in read_csv_rows(METRICS_DIR / "split_summary.csv")
    ]
    cleaning_source = read_csv_rows(METRICS_DIR / "cleaning_summary.csv")[0]
    cleaning = {
        key: (
            float(value)
            if key == "removed_percentage"
            else int(value)
        )
        for key, value in cleaning_source.items()
    }
    feature_importance = [
        {
            "feature": row["feature"],
            "relative_importance": float(row["relative_importance"]),
            "scaled_importance": float(row["scaled_importance"]),
            "percentage": float(row["percentage"]),
        }
        for row in read_csv_rows(METRICS_DIR / "feature_importance.csv")
    ]

    def error_rows(name: str, group_column: str = "group") -> list[dict[str, Any]]:
        return [
            {
                "group": row[group_column],
                "rows": int(row["rows"]),
                "mae": float(row["mae"]),
                "rmse": float(row["rmse"]),
                "mean_error": float(row["mean_error"]),
            }
            for row in read_csv_rows(METRICS_DIR / name)
        ]

    return {
        "monthly": monthly,
        "splits": splits,
        "cleaning": cleaning,
        "feature_importance": feature_importance,
        "errors": {
            "hour": error_rows("error_by_hour.csv"),
            "day_of_week": error_rows("error_by_day_of_week.csv"),
            "pickup_borough": error_rows("error_by_borough.csv"),
            "dropoff_borough": error_rows("error_by_dropoff_borough.csv"),
            "duration_range": error_rows("error_by_duration_range.csv"),
            "route": error_rows("error_by_route.csv", "route_id")[:20],
        },
        "pipeline": {
            "jdbc": read_csv_rows(METRICS_DIR / "multisource_join_summary.csv")[0],
            "mojo": read_json(METRICS_DIR / "mojo_scoring_summary.json"),
            "streaming": read_json(METRICS_DIR / "streaming_summary.json"),
            "h2o_flow": read_json(METRICS_DIR / "h2o_flow_demo_metrics.json"),
        },
    }


def load_prediction_models() -> list[dict[str, Any]]:
    metrics = {
        row["model"]: row
        for row in read_csv_rows(METRICS_DIR / "model_comparison.csv")
    }
    result = []
    for key, model in MODEL_REGISTRY.items():
        row = metrics.get(model["comparison_name"], {})
        result.append(
            {
                "key": key,
                "label": model["label"],
                "available": model["path"].is_file(),
                "rmse": float(row["rmse"]) if row else None,
                "mae": float(row["mae"]) if row else None,
                "r2": float(row["r2"]) if row else None,
                "test_rows": int(row["test_rows"]) if row else None,
            }
        )
    return result


def load_zones() -> list[dict[str, Any]]:
    zones: list[dict[str, Any]] = []
    for row in read_csv_rows(ZONE_LOOKUP):
        location_id = row.get("LocationID") or row.get("location_id")
        if not location_id:
            continue
        zones.append(
            {
                "id": int(location_id),
                "borough": row.get("Borough", ""),
                "zone": row.get("Zone", ""),
                "service_zone": row.get("service_zone", ""),
            }
        )
    return zones


def _number(
    payload: dict[str, Any],
    key: str,
    *,
    minimum: float,
    maximum: float,
    integer: bool = False,
) -> int | float:
    try:
        value = float(payload[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise InputError(f"{key} phải là một số hợp lệ.") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise InputError(f"{key} phải nằm trong khoảng {minimum:g}–{maximum:g}.")
    if integer:
        if not value.is_integer():
            raise InputError(f"{key} phải là số nguyên.")
        return int(value)
    return value


def normalize_prediction_input(payload: dict[str, Any]) -> dict[str, int | float | str]:
    pickup_location = _number(
        payload, "pickup_location", minimum=1, maximum=265, integer=True
    )
    dropoff_location = _number(
        payload, "dropoff_location", minimum=1, maximum=265, integer=True
    )
    passenger_count = _number(
        payload, "passenger_count", minimum=0, maximum=9, integer=False
    )

    raw_datetime = str(payload.get("pickup_datetime", "")).strip()
    try:
        pickup_datetime = datetime.fromisoformat(raw_datetime)
    except ValueError as exc:
        raise InputError("pickup_datetime phải có định dạng ngày giờ hợp lệ.") from exc

    # Spark dayofweek: Sunday=1, Monday=2, ..., Saturday=7.
    pickup_day_of_week = ((pickup_datetime.weekday() + 1) % 7) + 1
    is_weekend = int(pickup_day_of_week in (1, 7))

    return {
        "pickup_location": str(pickup_location),
        "dropoff_location": str(dropoff_location),
        "passenger_count": passenger_count,
        "pickup_hour": pickup_datetime.hour,
        "pickup_day_of_week": pickup_day_of_week,
        "pickup_month": pickup_datetime.month,
        "is_weekend": is_weekend,
        "route_trip_count": _number(
            payload, "route_trip_count", minimum=0, maximum=50_000_000
        ),
        "route_avg_duration": _number(
            payload, "route_avg_duration", minimum=1, maximum=180
        ),
        "hour_route_avg_duration": _number(
            payload, "hour_route_avg_duration", minimum=1, maximum=180
        ),
    }


def find_java() -> Path:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java.exe"
        if candidate.exists():
            return candidate
    resolved = shutil.which("java")
    if resolved:
        return Path(resolved)
    raise RuntimeError("Không tìm thấy Java. Hãy chạy scripts\\activate_project.cmd.")


def predict_with_mojo(
    features: dict[str, int | float | str], model_key: str = "drf"
) -> float:
    if model_key not in MODEL_REGISTRY:
        raise InputError(f"Model không hợp lệ: {model_key}")
    model_path = MODEL_REGISTRY[model_key]["path"]
    if not model_path.is_file():
        raise RuntimeError(f"Không tìm thấy MOJO: {model_path}")
    if not H2O_JAR.is_file():
        raise RuntimeError(f"Không tìm thấy h2o.jar: {H2O_JAR}")

    temp_root = project_path("outputs/tmp")
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="interactive-", dir=temp_root) as folder:
        folder_path = Path(folder)
        input_path = folder_path / "input.csv"
        output_path = folder_path / "prediction.csv"
        with input_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=MODEL_FEATURES)
            writer.writeheader()
            writer.writerow(features)

        command = [
            str(find_java()),
            "-Xmx1g",
            "-cp",
            str(H2O_JAR),
            "hex.genmodel.tools.PredictCsv",
            "--mojo",
            str(model_path),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--decimal",
        ]
        result = subprocess.run(
            command,
            cwd=project_path("."),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if result.returncode != 0 or not output_path.is_file():
            detail = (result.stderr or result.stdout or "unknown error").strip()
            raise RuntimeError(f"MOJO scoring thất bại: {detail[-1200:]}")
        rows = read_csv_rows(output_path)
        if len(rows) != 1:
            raise RuntimeError("MOJO không trả về đúng một prediction.")
        prediction = float(rows[0]["predict"])
        if not math.isfinite(prediction):
            raise RuntimeError("MOJO trả về prediction không hợp lệ.")
        return prediction


def artifact_status() -> dict[str, bool]:
    expected_charts = {
        "rmse_comparison.png",
        "mae_comparison.png",
        "r2_comparison.png",
        "feature_importance.png",
        "error_by_hour.png",
        "error_by_day_of_week.png",
        "error_by_borough.png",
        "error_by_dropoff_borough.png",
        "error_by_duration_range.png",
    }
    return {
        "model_mojo": MODEL_PATH.is_file(),
        "metrics": (METRICS_DIR / "model_comparison.csv").is_file(),
        "charts": all((CHARTS_DIR / name).is_file() for name in expected_charts),
        "streaming": (METRICS_DIR / "streaming_summary.json").is_file(),
        "h2o_flow_demo": (METRICS_DIR / "h2o_flow_demo_metrics.json").is_file(),
    }


def self_test() -> dict[str, Any]:
    sample = normalize_prediction_input(
        {
            "pickup_location": 161,
            "dropoff_location": 236,
            "passenger_count": 1,
            "pickup_datetime": "2025-11-05T09:00",
            "route_trip_count": 10000,
            "route_avg_duration": 12,
            "hour_route_avg_duration": 15,
        }
    )
    predictions: dict[str, Any] = {}
    for model_key, model in MODEL_REGISTRY.items():
        try:
            predictions[model_key] = {
                "label": model["label"],
                "prediction_minutes": predict_with_mojo(sample, model_key),
                "status": "PASS",
            }
        except Exception as exc:
            predictions[model_key] = {
                "label": model["label"],
                "status": "FAIL",
                "error": str(exc),
            }
    all_models_passed = all(
        value["status"] == "PASS" for value in predictions.values()
    )
    result = {
        "status": "PASS" if all_models_passed else "FAIL",
        "prediction_minutes": predictions["drf"].get("prediction_minutes"),
        "model": str(MODEL_PATH.relative_to(project_path("."))),
        "model_predictions": predictions,
        "artifacts": artifact_status(),
    }
    if not all(result["artifacts"].values()):
        result["status"] = "FAIL"
    return result


def _build_legacy_index_html() -> str:
    return """<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>NYC Taxi Trip Duration</title>
  <style>
    :root{--navy:#071a2f;--blue:#0a84ff;--cyan:#28d7e5;--yellow:#ffd447;
      --ink:#12243a;--muted:#5e7188;--paper:#f4f8fb;--card:#fff;--good:#16845b}
    *{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;
      color:var(--ink);background:var(--paper)}header{background:linear-gradient(120deg,var(--navy),#123e68);
      color:white;padding:44px max(5vw,24px) 70px;position:relative;overflow:hidden}
    header:after{content:"";position:absolute;width:350px;height:350px;border:70px solid #ffffff12;
      border-radius:50%;right:-80px;top:-130px}.eyebrow{color:var(--yellow);font-weight:800;
      letter-spacing:.12em;text-transform:uppercase;font-size:12px}h1{font-size:clamp(32px,5vw,62px);
      margin:10px 0 8px;max-width:900px;line-height:1.02}.lead{max-width:760px;color:#c9dcf0;
      font-size:18px;line-height:1.55}.shell{max-width:1240px;margin:-38px auto 60px;padding:0 22px;
      position:relative}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:18px}.card{
      background:var(--card);border:1px solid #dce7f0;border-radius:18px;padding:22px;
      box-shadow:0 10px 30px #08213b10}.metric{grid-column:span 3}.metric b{display:block;
      font-size:27px;margin-top:8px}.metric span{color:var(--muted);font-size:13px}.wide{grid-column:span 12}
    .half{grid-column:span 6}h2{margin:0 0 16px;font-size:24px}h3{margin:0 0 10px}
    form{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}label{font-size:13px;
      font-weight:700;color:#334d66}input,select{width:100%;margin-top:6px;border:1px solid #c9d7e4;
      border-radius:10px;padding:11px 12px;background:white;color:var(--ink)}
    details{grid-column:1/-1;border:1px dashed #b9cad9;border-radius:12px;padding:13px}
    summary{cursor:pointer;font-weight:800}button{grid-column:1/-1;background:var(--blue);color:white;
      border:0;border-radius:12px;padding:14px;font-weight:800;font-size:16px;cursor:pointer}
    button:disabled{opacity:.55}.result{margin-top:16px;padding:18px;border-radius:14px;background:#eef9f5;
      border-left:5px solid var(--good);display:none}.result b{font-size:30px;color:var(--good)}
    .note{color:var(--muted);font-size:13px;line-height:1.55}.table-wrap{overflow:auto}table{
      border-collapse:collapse;width:100%;font-size:14px}th,td{padding:11px 12px;text-align:left;
      border-bottom:1px solid #e2ebf2}th{color:var(--muted);font-size:12px;text-transform:uppercase}
    .gallery{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.gallery img{
      width:100%;border:1px solid #dce7f0;border-radius:12px;background:#fff}.pill{display:inline-flex;
      align-items:center;gap:6px;background:#e7f7f1;color:var(--good);font-weight:800;padding:7px 10px;
      border-radius:999px;font-size:12px}.dot{width:8px;height:8px;background:var(--good);border-radius:50%}
    footer{text-align:center;color:var(--muted);padding:25px}.error{background:#fff0f0;border-left-color:#d33;
      color:#8c1c1c}.loading{opacity:.65}@media(max-width:850px){.metric{grid-column:span 6}
      .half{grid-column:span 12}}@media(max-width:560px){.metric{grid-column:span 12}
      form,.gallery{grid-template-columns:1fr}}
  </style>
</head>
<body>
<header>
  <div class="eyebrow">Apache Spark × H2O Sparkling Water</div>
  <h1>NYC Yellow Taxi<br>Trip Duration</h1>
  <p class="lead">Dashboard kỹ thuật cho pipeline 12 tháng: xem quy mô dữ liệu, so sánh mô hình,
  phân tích sai số và dự đoán thời gian chuyến đi bằng DRF MOJO thật.</p>
</header>
<main class="shell">
  <section class="grid" id="metrics">
    <article class="card metric"><span>Dữ liệu thô</span><b id="rawRows">…</b></article>
    <article class="card metric"><span>Dữ liệu sạch</span><b id="cleanRows">…</b></article>
    <article class="card metric"><span>R² DRF test</span><b id="bestR2">…</b></article>
    <article class="card metric"><span>MOJO full test</span><b id="mojoRows">…</b></article>

    <article class="card half">
      <span class="pill"><span class="dot"></span>DRF MOJO local</span>
      <h2 style="margin-top:14px">Dự đoán một chuyến đi</h2>
      <form id="predictForm">
        <label>Điểm đón<select id="pickup_location" required></select></label>
        <label>Điểm trả<select id="dropoff_location" required></select></label>
        <label>Số hành khách<input id="passenger_count" type="number" min="0" max="9"
          step="1" value="1" required></label>
        <label>Thời điểm đón<input id="pickup_datetime" type="datetime-local"
          value="2025-11-05T09:00" required></label>
        <details>
          <summary>Đặc trưng lịch sử tuyến</summary>
          <p class="note">Ba giá trị này được tính từ tập train trong pipeline. Khi demo nhập tay,
          có thể dùng giá trị điển hình bên dưới hoặc thay bằng thống kê tuyến đã biết.</p>
          <label>Số chuyến lịch sử<input id="route_trip_count" type="number" min="0"
            value="10000" required></label>
          <label>Thời gian trung bình tuyến (phút)<input id="route_avg_duration" type="number"
            min="1" max="180" step="0.1" value="15" required></label>
          <label>Thời gian trung bình tuyến-giờ (phút)<input id="hour_route_avg_duration"
            type="number" min="1" max="180" step="0.1" value="15" required></label>
        </details>
        <button id="predictButton" type="submit">Dự đoán bằng MOJO</button>
      </form>
      <div class="result" id="predictionResult"></div>
      <p class="note">Prediction mang tính minh họa học thuật. Thời gian thực tế còn phụ thuộc giao
      thông, thời tiết, sự kiện và điều kiện đường.</p>
    </article>

    <article class="card half">
      <h2>So sánh mô hình</h2>
      <div class="table-wrap"><table>
        <thead><tr><th>Mô hình</th><th>RMSE</th><th>MAE</th><th>R²</th><th>Test</th></tr></thead>
        <tbody id="modelRows"></tbody>
      </table></div>
      <p class="note">Dữ liệu test được chia theo thời gian. RMSE và MAE có đơn vị phút;
      thấp hơn là tốt hơn. R² cao hơn là tốt hơn.</p>
    </article>

    <article class="card wide">
      <h2>Trực quan kết quả và phân tích sai số</h2>
      <div class="gallery">
        <img src="/charts/rmse_comparison.png" alt="So sánh RMSE">
        <img src="/charts/r2_comparison.png" alt="So sánh R2">
        <img src="/charts/feature_importance.png" alt="Feature importance">
        <img src="/charts/error_by_hour.png" alt="Sai số theo giờ">
        <img src="/charts/error_by_borough.png" alt="Sai số theo borough">
        <img src="/charts/error_by_duration_range.png" alt="Sai số theo thời lượng">
      </div>
    </article>
  </section>
</main>
<footer>NYC Taxi Sparkling Water · Local academic demonstration</footer>
<script>
const fmt = n => new Intl.NumberFormat('vi-VN').format(n);
async function boot(){
  const [summary,zones] = await Promise.all([
    fetch('/api/summary').then(r=>r.json()),
    fetch('/api/zones').then(r=>r.json())
  ]);
  rawRows.textContent=fmt(summary.dataset.raw_rows);
  cleanRows.textContent=fmt(summary.dataset.clean_rows);
  const best=summary.models.find(m=>m.model==='DRF')||summary.models[0];
  bestR2.textContent=best.r2.toFixed(4);
  mojoRows.textContent=fmt(summary.mojo.rows);
  modelRows.innerHTML=summary.models.map(m=>`<tr><td><b>${m.model}</b></td>
    <td>${m.rmse.toFixed(4)}</td><td>${m.mae.toFixed(4)}</td>
    <td>${m.r2.toFixed(4)}</td><td>${fmt(m.test_rows)}</td></tr>`).join('');
  const options=zones.map(z=>`<option value="${z.id}">${z.id} · ${z.zone} (${z.borough})</option>`).join('');
  pickup_location.innerHTML=options; dropoff_location.innerHTML=options;
  pickup_location.value='161'; dropoff_location.value='236';
}
predictForm.addEventListener('submit',async e=>{
  e.preventDefault(); predictButton.disabled=true; predictButton.textContent='Đang chạy MOJO…';
  predictionResult.style.display='block'; predictionResult.className='result loading';
  predictionResult.textContent='Đang nạp model và chấm điểm…';
  const keys=['pickup_location','dropoff_location','passenger_count','pickup_datetime',
    'route_trip_count','route_avg_duration','hour_route_avg_duration'];
  const payload=Object.fromEntries(keys.map(k=>[k,document.getElementById(k).value]));
  try{
    const response=await fetch('/api/predict',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify(payload)});
    const data=await response.json();
    if(!response.ok) throw new Error(data.error||'Prediction failed');
    predictionResult.className='result';
    predictionResult.innerHTML=`Thời gian dự đoán<br><b>${data.prediction_minutes.toFixed(2)} phút</b>
      <div class="note">Pickup hour ${data.features.pickup_hour}, Spark day-of-week
      ${data.features.pickup_day_of_week}; model ${data.model}</div>`;
  }catch(error){
    predictionResult.className='result error';
    predictionResult.textContent=error.message;
  }finally{predictButton.disabled=false;predictButton.textContent='Dự đoán bằng MOJO'}
});
boot().catch(error=>{document.querySelector('.shell').innerHTML=`<div class="card error">${error}</div>`});
</script>
</body></html>"""


def build_index_html() -> str:
    """Load the standalone dashboard UI.

    Keeping presentation code in its own file makes the local application
    easier to redesign without touching MOJO scoring or API behavior.
    """

    return project_path("src/app/index.html").read_text(encoding="utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "NYCTaxiDashboard/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[dashboard] {self.address_string()} - {fmt % args}")

    def send_bytes(
        self, body: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def send_json(
        self, payload: Any, status: HTTPStatus = HTTPStatus.OK
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_bytes(body, "application/json; charset=utf-8", status)

    def do_GET(self) -> None:  # noqa: N802
        path = unquote(urlparse(self.path).path)
        try:
            if path == "/":
                self.send_bytes(
                    build_index_html().encode("utf-8"), "text/html; charset=utf-8"
                )
            elif path == "/api/health":
                self.send_json({"status": "ok", "artifacts": artifact_status()})
            elif path == "/api/summary":
                self.send_json(load_summary())
            elif path == "/api/analytics":
                self.send_json(load_analytics())
            elif path == "/api/models":
                self.send_json(load_prediction_models())
            elif path == "/api/zones":
                self.send_json(load_zones())
            elif path.startswith("/charts/"):
                name = Path(path).name
                chart = (CHARTS_DIR / name).resolve()
                if chart.parent != CHARTS_DIR.resolve() or chart.suffix.lower() != ".png":
                    self.send_json({"error": "Đường dẫn biểu đồ không hợp lệ."}, HTTPStatus.BAD_REQUEST)
                elif not chart.is_file():
                    self.send_json({"error": "Không tìm thấy biểu đồ."}, HTTPStatus.NOT_FOUND)
                else:
                    self.send_bytes(chart.read_bytes(), "image/png")
            else:
                self.send_json({"error": "Không tìm thấy endpoint."}, HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - HTTP integration safety net
            self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/predict":
            self.send_json({"error": "Không tìm thấy endpoint."}, HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 64 * 1024:
                raise InputError("Request body rỗng hoặc quá lớn.")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise InputError("JSON body phải là một object.")
            model_key = str(payload.get("model", "drf"))
            features = normalize_prediction_input(payload)
            prediction = predict_with_mojo(features, model_key)
            model = MODEL_REGISTRY[model_key]
            self.send_json(
                {
                    "prediction_minutes": prediction,
                    "model": model["label"],
                    "model_key": model_key,
                    "features": features,
                }
            )
        except (InputError, json.JSONDecodeError) as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except subprocess.TimeoutExpired:
            self.send_json(
                {"error": "MOJO scoring quá thời gian 90 giây."},
                HTTPStatus.GATEWAY_TIMEOUT,
            )
        except Exception as exc:  # pragma: no cover - external Java failure
            self.send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)


def main() -> int:
    parser = argparse.ArgumentParser(description="NYC Taxi local MOJO dashboard")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the dashboard after the HTTP server has bound its port.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run one real MOJO prediction and exit.",
    )
    args = parser.parse_args()

    if args.self_test:
        result = self_test()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["status"] == "PASS" else 1

    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print("=" * 64)
    print("NYC TAXI LOCAL DASHBOARD")
    print(f"http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    print("=" * 64)
    if args.open_browser:
        webbrowser.open(f"http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
