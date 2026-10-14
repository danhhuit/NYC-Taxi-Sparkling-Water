# Hướng dẫn hoàn tất thủ công

Tài liệu này chỉ gồm các bước không thể hoàn tất an toàn trong phiên hiện tại vì cần khởi động lại Windows, thao tác Docker Desktop hoặc giải phóng RAM.

## 1. Trước khi bắt đầu

1. Lưu mọi tài liệu đang mở.
2. Đóng Spark, Java, H2O Flow, Docker Desktop và các chương trình dùng nhiều RAM.
3. Khởi động lại Windows. Không chọn Docker Factory Reset vì có thể làm mất image và cluster.
4. Sau khi đăng nhập lại, không mở H2O Flow trước khi hoàn tất Docker/Kubernetes.

## 2. Hoàn tất Docker và Kubernetes

### 2.1 Khởi động Docker Desktop

1. Mở Docker Desktop.
2. Chờ góc dưới bên trái hiển thị Docker Engine đang chạy.
3. Vào **Settings → Kubernetes**.
4. Bật **Enable Kubernetes** nếu đang tắt.
5. Chọn **Apply & Restart** và chờ trạng thái Kubernetes chuyển sang Running.

Không chạy H2O Flow trong lúc này để tránh thiếu RAM.

### 2.2 Chạy kiểm tra tự động

Mở Command Prompt:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
scripts\validate_deployment_after_reboot.cmd
```

Script sẽ:

1. Kiểm tra Docker daemon.
2. Build lại image `nyc-taxi-final:1.0` từ source mới nhất.
3. Chạy Docker scoring container.
4. Kiểm tra Kubernetes context và node.
5. Xóa Job cũ nếu có.
6. Apply `kubernetes\final-job.yaml`.
7. Chờ Job `nyc-taxi-final-job` hoàn thành tối đa 30 phút.
8. Ghi log Docker và Kubernetes.

Không đóng cửa sổ khi container đang chấm điểm. Batch trong image chứa toàn bộ 8.155.303 dòng test nên có thể mất nhiều phút.

### 2.3 Tiêu chí thành công

Cuối cửa sổ phải xuất hiện:

```text
Docker and Kubernetes validation succeeded.
```

Kiểm tra thêm:

```bat
docker ps -a --filter ancestor=nyc-taxi-final:1.0
kubectl get nodes
kubectl get job nyc-taxi-final-job
kubectl get pods -l app=nyc-taxi-final
kubectl logs job/nyc-taxi-final-job
```

Kết quả mong đợi:

- Kubernetes node: `Ready`.
- Job: `Complete` với `1/1`.
- Pod: `Completed`, số restart bằng `0`.
- Log chứa `FINAL MOJO SCORING SUCCESS`.

Bằng chứng được lưu tại:

```text
outputs/logs/docker_final.log
outputs/logs/kubernetes_final.log
```

### 2.4 Nếu Docker vẫn không chạy

Mở PowerShell bằng **Run as administrator**:

```powershell
wsl --shutdown
Restart-Service WslService
```

Sau đó mở lại Docker Desktop. Không xóa thư mục Docker và không dùng Factory Reset. Nếu vẫn lỗi, kiểm tra:

```bat
docker desktop status
docker desktop logs --since 10m --priority 2
```

Lỗi trước đó là socket bị kẹt:

```text
C:\Users\NGUYEN THANH DANH\AppData\Local\docker-secrets-engine\engine.sock
Windows error 1920
```

Windows reboot hoặc restart `WslService` bằng quyền Administrator là cách phục hồi an toàn.

## 3. Hoàn tất external H2O backend

Sau khi Docker/Kubernetes đã xong:

1. Thoát Docker Desktop để giải phóng RAM.
2. Mở Command Prompt thứ nhất:

   ```bat
   cd /d D:\BigData\NYC-Taxi-Sparkling-Water
   scripts\start_h2o_flow.cmd
   ```

3. Giữ cửa sổ H2O mở.
4. Mở Command Prompt thứ hai:

   ```bat
   cd /d D:\BigData\NYC-Taxi-Sparkling-Water
   scripts\run_h2o_external_backend_check.cmd
   ```

Kết quả mong đợi:

```text
EXTERNAL H2O CONTEXT CONNECTED SUCCESSFULLY
SPARK ROW COUNT: 10
H2O -> SPARK ROW COUNT: 10
EXTERNAL BACKEND TEST SUCCESS
```

Log được ghi tại:

```text
outputs/logs/h2o_external_backend.log
```

Sau khi thành công, quay lại cửa sổ H2O và nhấn `Ctrl+C` để dừng cluster.

## 4. Hoàn tất H2O Flow với toàn bộ 46.714.819 dòng

### 4.1 Giải phóng RAM

H2O Flow full cần ít nhất 10 GB RAM trống cho heap 8 GB và 2 GB dự phòng.

1. Thoát Docker Desktop.
2. Đóng Chrome tab nặng, IDE, Spark, Java và Python không dùng.
3. Mở Task Manager → Performance → Memory.
4. Chỉ tiếp tục khi Available memory ít nhất 10 GB.

Kiểm tra bằng lệnh:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\test_h2o_flow_resources.ps1 -HeapGb 8
```

Kết quả bắt buộc:

```text
H2O Flow resource preflight passed.
```

Nếu máy vẫn dưới 10 GB trống, khởi động lại Windows rồi chạy H2O trước khi mở Docker/IDE.

### 4.2 Khởi động cluster và chạy workflow

Command Prompt thứ nhất:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
set "H2O_HEAP_GB=8"
scripts\start_h2o_flow_full.cmd
```

Giữ cửa sổ cluster mở. Khi `http://127.0.0.1:54321` truy cập được, mở Command Prompt thứ hai:

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
.venv\Scripts\python.exe scripts\capture_h2o_flow_full.py
```

Workflow sẽ:

1. Import 1.152 file Parquet hardlink.
2. Parse thành frame `nyc_taxi_full_12_months`.
3. Từ chối tiếp tục nếu số dòng khác `46.714.819`.
4. Train GBM trong H2O.
5. Tạo metrics, feature importance và 10.000 prediction demo.
6. Chụp sáu ảnh thật bằng Chrome headless.

Không đóng hai cửa sổ trong khi import/train. Tùy RAM và ổ đĩa, bước này có thể kéo dài khá lâu.

### 4.3 Kiểm tra sáu screenshot

```bat
dir docs\screenshots
```

Phải có:

```text
h2o_flow_cluster.png
h2o_flow_frame.png
h2o_flow_gbm.png
h2o_flow_metrics.png
h2o_flow_importance.png
h2o_flow_prediction.png
```

Mở từng ảnh và xác nhận:

- Cluster healthy, một node.
- Frame có đúng 46.714.819 dòng.
- Model ID là `nyc_taxi_flow_gbm`.
- Metrics và Variable Importance hiển thị.
- Frame prediction có cột `predict`.

Sau đó nhấn `Ctrl+C` trong cửa sổ H2O.

## 5. Kiểm tra cuối cùng

```bat
cd /d D:\BigData\NYC-Taxi-Sparkling-Water
scripts\run_spark_submit.cmd
scripts\run_final_demo.cmd
```

Hai lệnh phải kết thúc bằng:

```text
FINAL INTEGRATED DEMO SUCCESS
```

Sau khi các bước thủ công hoàn thành, kiểm tra:

```bat
git status
```

Các log runtime và prediction lớn được `.gitignore` bỏ qua. Nếu cần nộp minh chứng, nén riêng `outputs/logs` và `docs/screenshots`; không đưa raw dataset hoặc MOJO 140 MB vào GitHub thường.
