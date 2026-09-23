# NYC Taxi Trip Duration Prediction with Apache Spark & H2O Sparkling Water

## 1. Giới thiệu

Đồ án xây dựng pipeline Big Data để phân tích dữ liệu Yellow Taxi tại New York và dự đoán **thời gian chuyến đi (trip duration)** bằng **Apache Spark 3.5.0** kết hợp **H2O Sparkling Water 3.46.0.6**.

```text
NYC TLC Yellow Taxi Parquet
        │
        ▼
Apache Spark
  ├─ Load dữ liệu
  ├─ Làm sạch
  ├─ Feature Engineering
  ├─ SQL Analysis
  └─ Multi-source join với PostgreSQL
        │
        ▼
H2O Sparkling Water
  ├─ GBM
  ├─ DRF
  ├─ Deep Learning
  ├─ Grid Search
  └─ AutoML
        │
        ▼
Model Evaluation
  ├─ RMSE
  ├─ MAE
  ├─ R²
  └─ Feature Importance
        │
        ▼
Deployment / Demo
  ├─ MOJO
  ├─ Batch Prediction
  ├─ Structured Streaming
  ├─ Docker
  ├─ Kubernetes
  ├─ External H2O Backend
  └─ H2O Flow Web UI
```

## 2. Bài toán

Target:

```text
trip_duration_minutes
```

được tính từ:

```text
tpep_dropoff_datetime - tpep_pickup_datetime
```

Feature baseline:

- `pickup_location`
- `dropoff_location`
- `passenger_count`
- `pickup_hour`
- `pickup_day_of_week`
- `is_weekend`

Không dùng các cột có nguy cơ **data leakage** như dropoff time, fare, tip, tolls, total amount. `trip_distance` cũng không dùng trong baseline vì là khoảng cách thực tế ghi nhận sau chuyến đi.

`priority_score` từ PostgreSQL chỉ là metadata mô phỏng phục vụ minh họa multi-source, không dùng trong baseline khoa học.

## 3. Nguồn dữ liệu

NYC TLC Trip Record Data:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Yellow Taxi January 2025:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

Taxi Zone Lookup:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Quy mô:

```text
Raw January 2025 : 3,475,226 rows
Clean data       : 3,409,834 rows
ML demo dataset  : 500,000 rows
```

## 4. Công nghệ

| Thành phần | Phiên bản / Công nghệ |
|---|---|
| Python | 3.11 |
| Java | Eclipse Temurin JDK 17 |
| Apache Spark | 3.5.0 |
| PySpark | 3.5.0 |
| Scala | 2.12.18 |
| H2O | 3.46.0.6 |
| Sparkling Water | 3.46.0.6-1-3.5 |
| PostgreSQL | 18.x |
| Docker Desktop | Linux containers / WSL2 |
| Kubernetes | Docker Desktop Kubernetes |
| Dataset | NYC TLC Yellow Taxi Jan 2025 |

## 5. Cấu trúc project

```text
NYC-Taxi-Sparkling-Water/
├── data/
│   ├── raw/
│   │   ├── yellow/
│   │   └── zones/
│   ├── processed/
│   ├── stream/
│   └── spark-temp/
├── src/
├── database/
├── models/
├── outputs/
│   ├── metrics/
│   ├── predictions/
│   ├── streaming/
│   └── logs/
├── config/
├── docker/
├── kubernetes/
├── scripts/
│   ├── activate_project.cmd
│   ├── run_demo.cmd
│   └── start_h2o_flow.cmd
├── requirements.txt
└── README.md
```

## 6. Khởi tạo môi trường

Đường dẫn đang dùng:

```text
JAVA_HOME=D:\BigData\Tools\Java\jdk-17
SPARK_HOME=D:\BigData\Tools\Spark\spark-3.5.0-bin-hadoop3
HADOOP_HOME=%SPARK_HOME%\hadoop
```

Mở CMD tại root:

```cmd
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
scripts\activate_project.cmd
```

## 7. Chạy demo tổng hợp bằng một lệnh

```cmd
scripts\run_demo.cmd
```

Pipeline:

```text
Processed Taxi Data
→ Apache Spark
→ Model Comparison
→ H2O MOJO
→ Batch Prediction
```

Kết quả đã kiểm thử:

```text
Spark Version: 3.5.0
Processed Rows: 500,000
Batch Input Rows: 10,000
Prediction Rows: 10,000
Null Predictions: 0
```

## 8. Kết quả mô hình

Cùng seeded split:

```text
Train      : 350,089
Validation : 74,824
Test       : 75,087
```

| Model | Test RMSE | Test MAE | Test R² |
|---|---:|---:|---:|
| AutoML | 5.6703 | 3.3984 | 0.7742 |
| GBM Grid Search | 5.7925 | 3.5523 | 0.7644 |
| Deep Learning | 5.9324 | 3.5685 | 0.7529 |
| GBM Baseline | 6.4630 | 4.2812 | 0.7067 |
| DRF | 6.6729 | 4.4522 | 0.6873 |

Trong các lần chạy hiện tại trên cùng split, AutoML có RMSE/MAE thấp nhất và R² cao nhất.

> R² là hệ số xác định, không phải accuracy.

## 9. H2O MOJO

Model deployment:

```text
models/nyc_taxi_gbm_mojo
```

Batch scoring 10,000 dòng:

```text
Average prediction : 23.8321 phút
Min prediction     : 3.0782 phút
Max prediction     : 84.3806 phút
```

## 10. H2O Flow Web UI

Khởi động:

```cmd
scripts\start_h2o_flow.cmd
```

Mở:

```text
http://127.0.0.1:54321
```

### Import dataset

Trong Flow:

```text
Data
→ Import Files
```

Search:

```text
D:/BigData/NYC-Taxi-Sparkling-Water/data/processed/taxi_ml_ready.parquet
```

Chỉ chọn các file `.snappy.parquet` thật. Không chọn `.crc`, `_SUCCESS`, `_SUCCESS.crc`.

Sau đó:

```text
Import
→ Parse these files
```

Destination Frame:

```text
taxi_ml_ready.hex
```

Parser:

```text
PARQUET
```

Kiểm tra sau parse:

```text
Rows    : khoảng 500,000
Columns : 8
```

### Inspect Frame

Kiểm tra các cột:

```text
pickup_location
dropoff_location
passenger_count
pickup_hour
pickup_day_of_week
is_weekend
priority_score
trip_duration_minutes
```

Nếu `pickup_location` và `dropoff_location` là `String`, đổi sang `Enum` trước khi build model.

`trip_duration_minutes` phải giữ numeric vì đây là regression target.

### Split dữ liệu trong Flow

```text
Data
→ Split Frame...
```

Frame:

```text
taxi_ml_ready.hex
```

Tạo:

```text
taxi_flow_train.hex = 0.80
taxi_flow_valid.hex = 0.20
```

Đây là split phục vụ GUI demo, không thay thế split chính thức của pipeline.

### Build GBM trên Flow

```text
Model
→ Gradient Boosting Machine
```

Thiết lập:

```text
model_id          = taxi_flow_gbm_demo
training_frame    = taxi_flow_train.hex
validation_frame  = taxi_flow_valid.hex
response_column   = trip_duration_minutes
ignored_columns   = priority_score
ntrees            = 50
max_depth         = 6
learn_rate        = 0.05
sample_rate       = 0.8
seed              = 42
```

Sau đó bấm:

```text
Build Model
```

Có thể xem:

- Model Summary
- Scoring History
- RMSE
- MAE
- R²
- Variable Importance

> Model Flow chỉ dùng cho demo trực quan. Các số liệu chính thức của báo cáo lấy từ pipeline Sparkling Water đã kiểm thử.

### Predict trên Flow

```text
Model
→ List All Models
→ taxi_flow_gbm_demo
→ Predict
```

Frame:

```text
taxi_flow_valid.hex
```

Sau khi chạy:

```text
Data
→ List All Frames
```

Mở prediction frame để xem cột `predict`.

## 11. MOJO và H2O Flow

H2O Flow có hỗ trợ import **MOJO tiêu chuẩn dạng file** dưới dạng Generic Model:

```text
Data
→ Import Files / Upload File
→ chọn file MOJO
→ Model
→ Import MOJO Model
```

Tuy nhiên artifact Sparkling Water hiện tại được lưu theo cấu trúc Spark ML tại:

```text
models/nyc_taxi_gbm_mojo
```

nên không coi việc import artifact này vào Flow là bước bắt buộc của demo. Phần MOJO chính thức của project đã được kiểm thử bằng:

```text
src/20_load_mojo_predict.py
src/22_integrated_demo.py
```

Để demo trực quan trên Flow, nên dùng model GBM được build trực tiếp trong Flow; để demo deployment, dùng MOJO qua script/Docker/Kubernetes.

## 12. Structured Streaming

```text
500 records
5 JSON files
100 records / micro-batch
```

Pipeline:

```text
staging
→ incoming
→ readStream
→ foreachBatch
→ Parquet output
```

## 13. PostgreSQL JDBC

Database:

```text
nyc_taxi_db
```

Bảng:

```text
zone_statistics
```

Các field:

```text
zone_id
zone_type
congestion_level
priority_score
```

## 14. Docker

Build:

```cmd
docker build -f docker\Dockerfile -t nyc-taxi-mojo:1.0 .
```

Run:

```cmd
docker run --name nyc-taxi-mojo-demo nyc-taxi-mojo:1.0
```

## 15. Kubernetes

Kiểm tra:

```cmd
kubectl config current-context
kubectl get nodes
```

Apply:

```cmd
kubectl apply -f kubernetes\mojo-job.yaml
```

Xem:

```cmd
kubectl get jobs
kubectl get pods -l job-name=nyc-taxi-mojo-job
kubectl logs job/nyc-taxi-mojo-job
```

## 16. External H2O Backend

Test:

```cmd
python src\21_external_backend_test.py
```

Kết quả đã kiểm thử:

```text
H2O Cluster       : 127.0.0.1:54321
Cloud Name        : nyc_taxi_external
Cloud Size        : 1
Spark Rows        : 10
H2O -> Spark Rows : 10
```

## 17. File quan trọng

| File | Chức năng |
|---|---|
| `src/00_test_spark.py` | Kiểm tra Spark |
| `src/01_load_data.py` | Load raw data |
| `src/02_preprocessing.py` | Làm sạch + feature engineering |
| `src/03_jdbc_multisource.py` | PostgreSQL JDBC |
| `src/05_h2o_frame_conversion.py` | Spark ↔ H2OFrame |
| `src/06_prepare_ml_data.py` | Chuẩn bị ML |
| `src/07_train_gbm.py` | GBM baseline |
| `src/08_train_drf.py` | DRF |
| `src/09_train_deep_learning.py` | Deep Learning |
| `src/10_gbm_grid_search.py` | Manual GBM grid search |
| `src/11_automl.py` | AutoML |
| `src/12_compare_models.py` | So sánh model |
| `src/13_visualize_metrics.py` | Biểu đồ metrics |
| `src/14_feature_importance.py` | Feature importance |
| `src/16_batch_prediction.py` | Batch prediction |
| `src/18_structured_streaming.py` | Structured Streaming |
| `src/19_export_mojo.py` | Export MOJO |
| `src/20_load_mojo_predict.py` | Load + score MOJO |
| `src/21_external_backend_test.py` | External Backend |
| `src/22_integrated_demo.py` | Demo tổng hợp |
| `scripts/run_demo.cmd` | Demo 1 lệnh |
| `scripts/start_h2o_flow.cmd` | Mở H2O Flow |

## 18. Kịch bản demo cho giảng viên

### Demo nhanh

```cmd
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
scripts\run_demo.cmd
```

Cho thấy:

1. Spark version
2. Dataset 500,000 rows
3. So sánh 5 mô hình
4. Load MOJO
5. Predict 10,000 records
6. Prediction summary
7. `INTEGRATED DEMO SUCCESS`

### Demo trực quan

```cmd
scripts\start_h2o_flow.cmd
```

Mở:

```text
http://127.0.0.1:54321
```

Cho xem:

1. H2O cluster
2. Import Parquet
3. Frame 500,000 rows
4. Inspect columns
5. Build GBM
6. Model metrics
7. Variable importance
8. Predict validation frame

## 19. Lưu ý

Sau khi job đã `SUCCESS`, Spark trên Windows có thể báo:

```text
Failed to delete Spark temp dir
NoSuchFileException
```

Đây là cleanup warning sau khi job hoàn tất.

XGBoost native backend không dùng trong project vì môi trường Windows hiện tại không có native backend phù hợp.

## 20. GitHub

Không nên commit:

```text
.venv/
__pycache__/
data/raw/
data/spark-temp/
outputs/logs/
*.log
```

Nên kiểm tra file lớn trước khi push.

Quy trình push an toàn:

```cmd
git status
git remote -v
git branch --show-current

git checkout -b feature/final-demo-docs

git add README.md
git add scripts
git add src
git add config
git add docker
git add kubernetes
git add requirements.txt
git add .dockerignore

git status

git commit -m "docs: finalize NYC Taxi Sparkling Water demo"

git push -u origin feature/final-demo-docs
```

Không merge vào `main` trong bước này.

## 21. Tóm tắt

Project đã hoàn thành:

```text
Spark Environment
Data Loading
Preprocessing
PostgreSQL JDBC
H2O Sparkling Water
GBM / DRF / Deep Learning
Grid Search
AutoML
Model Evaluation
Feature Importance
Batch Prediction
Structured Streaming
spark-submit
MOJO
Docker
Kubernetes
External H2O Backend
H2O Flow
Integrated Demo
```

Lệnh demo chính:

```cmd
scripts\run_demo.cmd
```

Lệnh mở H2O Flow:

```cmd
scripts\start_h2o_flow.cmd
```
