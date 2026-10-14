# Model artifacts

Model triển khai hiện tại là H2O DRF MOJO tại thư mục local:

```text
models/final_mojo/
```

Artifact này khoảng 140 MB và được `.gitignore` loại khỏi GitHub thông thường. Có thể:

- tái tạo bằng `python -m src.models.train_final_additional_models --models drf --promote-drf`;
- phân phối qua Git LFS hoặc GitHub Release nếu cần nộp model;
- kiểm tra độc lập bằng `python -m src.prediction.final_mojo_score --rows 0`.

Model GBM Grid Search nhỏ hơn được giữ trong repository để làm bằng chứng cấu trúc MOJO:

```text
models/gbm_grid_best_mojo/
```
