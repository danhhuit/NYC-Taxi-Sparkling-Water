# Hướng dẫn chạy pipeline NYC Taxi 12 tháng

## Phạm vi dữ liệu

- Dữ liệu: Yellow Taxi từ `2025-01` đến `2025-12`.
- Train: tháng 01–09.
- Validation: tháng 10.
- Test: tháng 11–12.
- Route statistics chỉ được tính từ train để tránh leakage.

## 1. Mở CMD và kích hoạt môi trường

```cmd
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
scripts\activate_project.cmd
```

## 2. Kiểm tra môi trường

```powershell
powershell -ExecutionPolicy Bypass -File scripts\check_environment.ps1
```

## 3. PostgreSQL JDBC

Nếu muốn pipeline join bảng `zone_statistics`, thiết lập mật khẩu trong đúng cửa sổ CMD:

```cmd
set "NYC_TAXI_DB_PASSWORD=MAT_KHAU_POSTGRES_CUA_BAN"
```

Không commit mật khẩu vào Git. Nếu không thiết lập, pipeline vẫn chạy nhưng
`multisource_join_summary.csv` sẽ ghi `SKIPPED_PASSWORD_NOT_SET`.

## 4. Chạy từng bước

### Tải và xác thực 12 tháng

```cmd
python -m src.data.download_multimonth
```

### Cleaning, feature engineering và chronological split

```cmd
python -m src.data.final_data_pipeline
```

Job này xử lý toàn bộ dữ liệu nên có thể mất 25–45 phút trên máy local.

### Train GBM cuối kỳ

```cmd
python -m src.models.train_final_gbm
```

### Train DRF và Deep Learning

```cmd
python -m src.models.train_final_additional_models
```

### Error Analysis

```cmd
python -m src.evaluation.final_error_analysis
```

### Load MOJO và batch scoring

```cmd
python -m src.prediction.final_mojo_score
```

### Structured Streaming với MOJO

```cmd
python -m src.streaming.final_streaming
```

### Integrated Demo

```cmd
python -m src.demo.final_demo
```

## 5. Chạy tự động toàn bộ

Kiểm tra riêng bước download trước:

```cmd
scripts\run_12_month_pipeline.cmd --download-only
```

Sau khi thấy `12-MONTH DOWNLOAD CHECK SUCCESS`, chạy toàn bộ:

```cmd
scripts\run_12_month_pipeline.cmd
```

Script dừng ngay tại bước đầu tiên thất bại. Nên chạy từng bước trong lần đầu để
dễ theo dõi RAM, dung lượng đĩa và log.

## 6. Docker

```cmd
docker build -f docker\Dockerfile -t nyc-taxi-final:1.0 .
docker run --name nyc-taxi-final-demo nyc-taxi-final:1.0
```

## H2O Flow với toàn bộ 12 tháng

Đóng H2O cluster cũ, sau đó chạy:

```cmd
scripts\start_h2o_flow_full.cmd
```

Trong Flow import:

```text
D:/BigData/NYC-Taxi-Sparkling-Water/data/flow/full_12_months
```

Thư mục này chỉ chứa hard link tới 1.152 file Parquet thật, không có `.crc`
hoặc `_SUCCESS`, nên có thể chọn `Add all`.

## 7. Cấu trúc source mới

```text
src/
├── common/       cấu hình và path helper
├── data/         download, cleaning, JDBC, feature pipeline
├── h2o/          H2OContext, H2OFrame, external backend
├── models/       GBM, DRF, Deep Learning, Grid Search, AutoML
├── evaluation/   comparison, charts, importance, error analysis
├── prediction/   batch scoring và MOJO
├── streaming/    Structured Streaming
├── demo/         integrated demo
└── tests/        smoke test Spark
```

Tất cả file đã bỏ số ở đầu. Khi chạy, đứng tại project root và dùng
`python -m src.<package>.<module>`.
