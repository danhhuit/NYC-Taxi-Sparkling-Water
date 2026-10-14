# NYC Taxi Sparkling Water

Phân tích dữ liệu lớn và dự đoán thời gian chuyến đi Yellow Taxi tại New York bằng Apache Spark, H2O Sparkling Water, PostgreSQL, MOJO, Structured Streaming, Docker và Kubernetes.

Pipeline sử dụng dữ liệu 12 tháng từ `2025-01` đến `2025-12`, làm sạch và tạo đặc trưng bằng Spark, chia dữ liệu theo thời gian để tránh leakage, huấn luyện nhiều mô hình H2O và triển khai model dưới dạng MOJO.

## 1. Mục tiêu

- Xử lý dữ liệu taxi nhiều tháng bằng Spark.
- Kết hợp Taxi Zone Lookup và PostgreSQL qua JDBC.
- Tạo đặc trưng lịch sử theo tuyến mà không dùng thông tin tương lai.
- So sánh GBM, DRF, Deep Learning, GBM Grid Search và AutoML.
- Phân tích sai số theo giờ, quận, khoảng thời gian và tuyến.
- Export model MOJO để chấm điểm độc lập.
- Minh họa batch prediction và Structured Streaming.
- Đóng gói Docker và triển khai Kubernetes Job.
- Trình diễn model và dữ liệu trên H2O Flow.

## 2. Kiến trúc

```text
NYC TLC Parquet + Taxi Zone CSV + PostgreSQL
                        |
                        v
                 Apache Spark ETL
        clean -> feature engineering -> JDBC join
                        |
                        v
            Chronological train/valid/test
                        |
                        v
              H2O Sparkling Water
     GBM | DRF | Deep Learning | Grid | AutoML
                        |
                        v
        Comparison + Error Analysis + Importance
                        |
                        v
                     MOJO
          Batch | Streaming | Docker | K8s
```

## 3. Dataset và cách chia

Dữ liệu nguồn là Yellow Taxi Trip Records năm 2025.

| Thành phần | Giá trị |
|---|---:|
| Dữ liệu raw | 48.722.602 dòng |
| Sau làm sạch | 46.714.819 dòng |
| Bị loại | 2.007.783 dòng, tương đương 4,12% |
| Train | 34.327.814 dòng, tháng 01–09 |
| Validation | 4.231.702 dòng, tháng 10 |
| Test | 8.155.303 dòng, tháng 11–12 |

Việc chia theo thời gian được cấu hình trong `config/project_config.yaml`. Không dùng `randomSplit` cho kết quả cuối kỳ.

GBM, DRF, Deep Learning và Grid Search local mặc định dùng mẫu tái lập 1.200.000/400.000/400.000 dòng. AutoML dùng 600.000/200.000/200.000 dòng và tối đa bốn model để phù hợp heap local. Toàn bộ dữ liệu vẫn được Spark xử lý ở ETL; MOJO batch scoring mặc định chạy toàn bộ 8.155.303 dòng test. Có thể truyền giới hạn khác qua tham số dòng lệnh.

## 4. Công nghệ

| Công nghệ | Phiên bản dùng trong dự án |
|---|---|
| Python | 3.11 |
| Java | 17 |
| Apache Spark / PySpark | 3.5.0 |
| H2O | 3.46.0.6 |
| Sparkling Water | 3.46.0.6-1-3.5 |
| PostgreSQL | 18 |
| Docker Desktop | Linux containers |
| Kubernetes | Docker Desktop Kubernetes |

Các package Python chính nằm trong `requirements.txt`.

## 5. Cấu trúc thư mục

```text
NYC-Taxi-Sparkling-Water/
├── config/                 # Cấu hình dự án, Spark và mẫu biến database
├── data/
│   ├── raw/                # 12 file Parquet và Taxi Zone lookup
│   ├── processed/          # Dữ liệu clean/features/split/batch input
│   ├── flow/               # Hardlink dữ liệu dùng cho H2O Flow
│   └── streaming_input/    # Micro-batch input
├── database/               # SQL khởi tạo PostgreSQL
├── docker/                 # Dockerfile
├── kubernetes/             # Kubernetes Job manifests
├── models/                 # MOJO model artifacts
├── outputs/
│   ├── charts/
│   ├── logs/
│   ├── metrics/
│   ├── predictions/
│   └── streaming/
├── scripts/                # Script thiết lập và chạy pipeline
└── src/
    ├── common/             # Cấu hình, đường dẫn, helper ML
    ├── data/               # Download, cleaning, feature, JDBC, split
    ├── demo/               # Integrated demo
    ├── evaluation/         # Comparison, charts, importance, error analysis
    ├── h2o/                # H2O Context và external backend
    ├── models/             # GBM, DRF, DL, Grid Search, AutoML
    ├── prediction/         # MOJO và batch scoring
    ├── streaming/          # Structured Streaming
    └── tests/              # Kiểm tra Spark
```

Tên source được tổ chức theo tác vụ và không còn tiền tố số.

## 6. Cấu hình máy

Các đường dẫn mặc định:

```text
Project    D:\BigData\NYC-Taxi-Sparkling-Water
JAVA_HOME  D:\BigData\Tools\Java\jdk-17
SPARK_HOME D:\BigData\Tools\Spark\spark-3.5.0-bin-hadoop3
```

Nếu cài công cụ ở vị trí khác, sửa `scripts/activate_project.cmd` và `scripts/check_environment.ps1`.

Kiểm tra môi trường:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
powershell -ExecutionPolicy Bypass -File scripts\check_environment.ps1
```

Kết quả được ghi vào:

```text
outputs/logs/environment_check.log
```

## 7. Cài đặt Python

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
py -3.11 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Kích hoạt môi trường dự án:

```bat
scripts\activate_project.cmd
```

Script này cấu hình Java, Spark, Hadoop helper, thư mục tạm và các tùy chọn Java 17 cần cho Sparkling Water.

## 8. PostgreSQL

Tạo database và bảng:

```bat
psql -U postgres -f database\init.sql
```

Đặt mật khẩu cho đúng cửa sổ terminal đang chạy pipeline:

```bat
set "NYC_TAXI_DB_PASSWORD=your_password"
```

PowerShell:

```powershell
$env:NYC_TAXI_DB_PASSWORD = "your_password"
```

Không ghi mật khẩu thật vào Git. Có thể tham khảo `config/database.env.example`.

Cấu hình kết nối nằm tại `postgres` trong `config/project_config.yaml`:

```yaml
postgres:
  host: localhost
  port: 5432
  database: nyc_taxi_db
  user: postgres
  table: public.zone_statistics
  password_env: NYC_TAXI_DB_PASSWORD
```

## 9. Cấu hình dữ liệu và Spark

File chính: `config/project_config.yaml`.

```yaml
dataset:
  start_month: "2025-01"
  end_month: "2025-12"

split:
  train_end: "2025-09-30 23:59:59"
  validation_end: "2025-10-31 23:59:59"
  test_end: "2025-12-31 23:59:59"

spark:
  master: "local[4]"
  driver_memory: "4g"
  executor_memory: "4g"
  shuffle_partitions: 96
```

Cấu hình dùng cho `spark-submit` nằm trong `config/spark-defaults.conf`.

## 10. Chạy toàn bộ pipeline 12 tháng

Mở Command Prompt:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
set "NYC_TAXI_DB_PASSWORD=your_password"
scripts\run_12_month_pipeline.cmd
```

Chỉ tải và kiểm tra 12 file:

```bat
scripts\run_12_month_pipeline.cmd --download-only
```

Pipeline chính lần lượt chạy:

1. Download và xác thực 12 tháng.
2. Cleaning, feature engineering, PostgreSQL join và chronological split.
3. GBM, DRF và Deep Learning.
4. GBM Grid Search và AutoML.
5. Model Comparison, biểu đồ và Feature Importance.
6. Error Analysis.
7. MOJO scoring trên toàn bộ test split.
8. Structured Streaming.
9. Integrated Demo.

Nếu ETL và ba baseline đã có, chỉ chạy các phần bổ sung:

```bat
scripts\run_remaining_pipeline.cmd
```

## 11. Chạy từng tác vụ

```bat
scripts\activate_project.cmd

python -m src.data.download_multimonth
python -m src.data.final_data_pipeline
python -m src.models.train_final_gbm
python -m src.models.train_final_additional_models
python -m src.models.gbm_grid_search
python -m src.models.automl
python -m src.evaluation.compare_models
python -m src.evaluation.visualize_metrics
python -m src.evaluation.feature_importance
python -m src.evaluation.final_error_analysis
python -m src.prediction.final_mojo_score --rows 0
python -m src.streaming.final_streaming
python -m src.demo.final_demo
```

### Điều chỉnh số dòng huấn luyện

Mặc định local:

```bat
python -m src.models.train_final_gbm ^
  --max-train-rows 1200000 ^
  --max-validation-rows 400000 ^
  --max-test-rows 400000
```

Grid Search và AutoML hỗ trợ cùng ba tham số. Truyền `0` để bỏ giới hạn, nhưng cần RAM rất lớn:

```bat
python -m src.models.gbm_grid_search ^
  --max-train-rows 0 ^
  --max-validation-rows 0 ^
  --max-test-rows 0
```

Không nên chạy Grid Search/AutoML toàn bộ 46,7 triệu dòng trên máy 16 GB RAM. Với môi trường local, mẫu cố định và cùng seed giúp so sánh công bằng, còn toàn bộ dữ liệu được dùng cho ETL và batch scoring.

### MOJO scoring toàn bộ dòng

`--rows 0` nghĩa là tất cả dòng:

```bat
python -m src.prediction.final_mojo_score --rows 0
```

Chạy thử 100.000 dòng:

```bat
python -m src.prediction.final_mojo_score --rows 100000
```

## 12. Đặc trưng mô hình

Các feature cuối kỳ:

```text
pickup_location
dropoff_location
passenger_count
pickup_hour
pickup_day_of_week
pickup_month
is_weekend
route_trip_count
route_avg_duration
hour_route_avg_duration
```

Nhãn:

```text
trip_duration_minutes
```

Các feature thống kê tuyến được tạo từ dữ liệu lịch sử phù hợp với mốc thời gian, không tính trực tiếp từ test trong tương lai. Chi tiết cột được xuất tại `outputs/metrics/feature_dictionary.csv`.

## 13. Kết quả mô hình

Nguồn kết quả chính thức:

```text
outputs/metrics/gbm_metrics.json
outputs/metrics/drf_metrics.json
outputs/metrics/deep_learning_metrics.json
outputs/metrics/gbm_grid_results.csv
outputs/metrics/gbm_best_params.json
outputs/metrics/automl_leaderboard.csv
outputs/metrics/automl_test_metrics.json
outputs/metrics/model_comparison.csv
outputs/metrics/model_comparison.json
```

Các file so sánh chỉ tổng hợp kết quả chronological 12 tháng; không trộn metrics random split giữa kỳ.

Kết quả kiểm thử hiện tại:

| Model | RMSE | MAE | R² | Test sample |
|---|---:|---:|---:|---:|
| DRF | 7,4383 | 4,5895 | 0,7488 | 400.000 |
| GBM Grid Search | 7,4631 | 4,6329 | 0,7472 | 400.000 |
| GBM | 7,4664 | 4,6192 | 0,7469 | 400.000 |
| AutoML | 7,4781 | 4,6491 | 0,7479 | 200.000 |
| Deep Learning | 7,5542 | 4,7272 | 0,7409 | 400.000 |

DRF có RMSE thấp nhất và được export làm `models/final_mojo`. MOJO độc lập đã chấm đủ 8.155.303 dòng test, không có prediction null.

Biểu đồ:

```text
outputs/charts/rmse_comparison.png
outputs/charts/mae_comparison.png
outputs/charts/r2_comparison.png
outputs/charts/feature_importance.png
outputs/charts/error_by_hour.png
outputs/charts/error_by_day_of_week.png
outputs/charts/error_by_borough.png
outputs/charts/error_by_dropoff_borough.png
outputs/charts/error_by_duration_range.png
```

## 14. H2O Flow local

Chuẩn bị hardlink đến toàn bộ Parquet 12 tháng và mở cluster với heap 8 GB (cần tối thiểu 10 GB RAM trống):

```bat
scripts\start_h2o_flow_full.cmd
```

Launcher chạy kiểm tra RAM trước và chỉ khởi động khi còn ít nhất heap yêu cầu cộng 2 GB dự phòng cho Windows. Trên máy Windows hiện tại, launcher tự dùng Java 8 riêng cho H2O standalone để tránh lỗi loopback của Java 17; Spark và phần còn lại của pipeline vẫn dùng Java 17. Script chỉ báo thành công sau khi REST API xác nhận đúng cloud, healthy và có ít nhất một node.

Log khởi động và PID được ghi trong `outputs/logs/`. Dừng cluster bằng:

```bat
scripts\stop_h2o_flow.cmd
```

Sau khi cluster đã lên, chạy workflow import/model/screenshot tự động:

```bat
.venv\Scripts\python.exe scripts\capture_h2o_flow_full.py
```

Workflow chỉ tạo screenshot sau khi H2O xác nhận frame có đúng `46.714.819` dòng; không tự thay thế bằng mẫu nhỏ.

Mở:

```text
http://127.0.0.1:54321
```

Trong Flow:

1. Chọn **Data → Import Files**.
2. Nhập:

   ```text
   D:/BigData/NYC-Taxi-Sparkling-Water/data/flow/full_12_months
   ```

3. Chỉ chọn các file `*.parquet`; không chọn `.crc` hoặc `_SUCCESS`.
4. Parse thành một frame.
5. Xác nhận frame có `46.714.819` dòng.
6. Build GBM với response `trip_duration_minutes`.
7. Xem Model Metrics, Variable Importance và Predict.

Các ảnh cần lưu tại `docs/screenshots/`:

```text
h2o_flow_cluster.png
h2o_flow_frame.png
h2o_flow_gbm.png
h2o_flow_metrics.png
h2o_flow_importance.png
h2o_flow_prediction.png
```

Nếu import toàn bộ frame báo thiếu heap, đóng cửa sổ H2O cũ trước khi chạy lại và tăng `-Xmx` trong `scripts/start_h2o_flow_full.cmd` nếu máy còn đủ RAM.

Kiểm tra external backend sau khi chạy `scripts\start_h2o_flow.cmd`:

```bat
scripts\run_h2o_external_backend_check.cmd
```

Log được lưu tại `outputs/logs/h2o_external_backend.log`.

Các dòng `Failed to delete ... sparkling_water_assembly.jar` lúc Spark thoát là cảnh báo dọn thư mục tạm do Windows còn giữ file JAR, không phải lỗi kiểm tra. Kết quả được xem là đạt khi process trả exit code `0` và log chứa `EXTERNAL BACKEND TEST SUCCESS`.

## 15. spark-submit

Chạy integrated demo bằng Spark launcher và ghi log:

```bat
scripts\run_spark_submit.cmd
```

Log:

```text
outputs/logs/spark_submit.log
```

## 16. Docker

Docker Desktop phải chạy ở chế độ Linux containers.

```bat
docker build -f docker\Dockerfile -t nyc-taxi-final:1.0 .
docker run --name nyc-taxi-final-demo nyc-taxi-final:1.0
docker logs nyc-taxi-final-demo
```

Nếu container cũ đã tồn tại:

```bat
docker rm nyc-taxi-final-demo
```

Image chứa MOJO và batch input được chuẩn bị tại thời điểm build. Vì vậy cần chạy MOJO scoring/chuẩn bị batch input trước khi build image.

Container chạy chế độ `--deployment-validation`: vẫn chấm và kiểm tra toàn bộ 8.155.303 dòng nhưng không ghi lại các Parquet lớn vào filesystem tạm sẽ bị xóa khi container kết thúc. Summary, mẫu dự đoán và dấu hiệu thành công vẫn được tạo/in ra log. Việc chạy trực tiếp `scripts\run_final_mojo_scoring.cmd` trên máy vẫn xuất đầy đủ các bộ predictions.

## 17. Kubernetes

Trong Docker Desktop, bật **Settings → Kubernetes → Enable Kubernetes** và chờ node `Ready`.

```bat
kubectl config use-context docker-desktop
kubectl get nodes
kubectl apply -f kubernetes\final-job.yaml
kubectl wait --for=condition=complete job/nyc-taxi-final-job --timeout=20m
kubectl logs job/nyc-taxi-final-job
```

Sau khi vừa bật Kubernetes hoặc sau khi Docker Desktop được khởi động lại, có thể dùng script kiểm tra tự động:

```bat
scripts\validate_deployment_after_reboot.cmd
```

Script kiểm tra daemon, chạy container, apply lại Job, chờ `Complete` và ghi `docker_final.log`/`kubernetes_final.log`.

Nếu `docker run` báo `metadata.db: read-only file system`, đây là lỗi filesystem của Docker Desktop/containerd chứ không phải lỗi image. Thoát Docker Desktop, chạy `wsl --shutdown` trong PowerShell, khởi động lại Windows rồi mở Docker Desktop trước khi chạy lại script. Không dùng **Clean/Purge data** hoặc **Factory Reset** nếu chưa chấp nhận mất image, container và cluster cục bộ.

Chạy lại Job:

```bat
kubectl delete job nyc-taxi-final-job
kubectl apply -f kubernetes\final-job.yaml
```

Manifest dùng `imagePullPolicy: Never`, do đó image `nyc-taxi-final:1.0` phải tồn tại trong Docker Desktop trước khi apply.

Docker Desktop dùng Kubernetes chế độ `kind`, có containerd image store riêng với Docker Engine. Script validation tự stream image vào node bằng `scripts\load_image_into_docker_desktop_kubernetes.cmd` trước khi tạo Job; vì image khoảng 2,5 GB, bước này có thể mất vài phút.

## 18. Integrated Demo

```bat
scripts\run_final_demo.cmd
```

Demo kiểm tra:

- Processed datasets.
- Dataset summaries.
- Model comparison.
- Final model và MOJO.
- Batch predictions.
- Streaming results.
- Deployment files.

Dòng kết thúc mong đợi:

```text
FINAL INTEGRATED DEMO SUCCESS
```

## 19. Output bàn giao kỹ thuật

```text
outputs/metrics/raw_dataset_summary.csv
outputs/metrics/cleaning_summary.csv
outputs/metrics/feature_dictionary.csv
outputs/metrics/multisource_join_summary.csv
outputs/metrics/split_summary.csv
outputs/metrics/gbm_metrics.json
outputs/metrics/drf_metrics.json
outputs/metrics/deep_learning_metrics.json
outputs/metrics/gbm_grid_results.csv
outputs/metrics/gbm_best_params.json
outputs/metrics/automl_leaderboard.csv
outputs/metrics/automl_test_metrics.json
outputs/metrics/model_comparison.csv
outputs/metrics/model_comparison.json
outputs/metrics/feature_importance.csv
outputs/metrics/error_by_hour.csv
outputs/metrics/error_by_day_of_week.csv
outputs/metrics/error_by_borough.csv
outputs/metrics/error_by_dropoff_borough.csv
outputs/metrics/error_by_duration_range.csv
models/final_mojo/
outputs/predictions/batch_predictions/
outputs/predictions/mojo_predictions/
outputs/streaming/final_predictions/
docker/Dockerfile
kubernetes/final-job.yaml
```

## 20. Xử lý lỗi thường gặp

### `JAVA_HOME is not set`

Chạy `scripts\activate_project.cmd` hoặc sửa đường dẫn Java trong script.

### `The system cannot find the path specified`

Chạy script từ thư mục project hoặc dùng script trong `scripts/`; các script đã tự chuyển về project root.

### Spark/H2O mất kết nối hoặc `WinError 10054`

- Đóng các Java/Spark/H2O process cũ.
- Kiểm tra không có cluster khác chiếm cổng `54321`.
- Giảm `--max-train-rows`.
- Không chạy đồng thời AutoML, Grid Search và H2O Flow trên máy ít RAM.

### Lỗi `TimestampNTZType`

Các module final đã chỉ chuyển feature và label sang H2O. Không truyền nguyên DataFrame có timestamp vào `leaderboardDataFrame`.

### Lỗi xóa thư mục Spark temp sau khi đã SUCCESS

Các dòng `ShutdownHookManager: Exception while deleting Spark temp dir` xuất hiện sau marker thành công là cảnh báo cleanup trên Windows, không làm sai metrics. Chỉ xóa thư mục tạm khi chắc chắn không còn Spark process.

### Kubernetes không có context

Bật Kubernetes trong Docker Desktop, chờ hoàn tất và chạy:

```bat
kubectl config use-context docker-desktop
```

## 21. Tái lập và an toàn

- Seed mặc định: `42`.
- Mọi đường dẫn nghiệp vụ nằm trong `config/project_config.yaml`.
- Split cuối kỳ luôn theo thời gian.
- Metrics JSON lưu số dòng full và số dòng thực dùng huấn luyện.
- Không commit mật khẩu, `.env`, raw data lớn hoặc output prediction lớn.
- Nên đổi mật khẩu PostgreSQL nếu từng dán mật khẩu vào terminal/chat công khai.

## 22. Kiểm tra source trước khi bàn giao

```bat
.venv\Scripts\python.exe -m compileall -q src
scripts\run_spark_submit.cmd
scripts\run_final_demo.cmd
git status
```

Sau khi kiểm tra output và loại dữ liệu lớn khỏi Git:

```bat
git add README.md config database docker kubernetes scripts src
git add outputs\metrics outputs\charts
git commit -m "feat: complete final 12-month Sparkling Water pipeline"
git push origin main
```

Không chạy `git add data` hoặc thêm toàn bộ `outputs/predictions` nếu repository không dùng Git LFS.
