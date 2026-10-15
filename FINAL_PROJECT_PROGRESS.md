# Final Project Progress

Updated: 2026-10-15 (Asia/Ho_Chi_Minh) — evidence-based 12-month audit

| Phase | Status | Evidence / qualification |
|---|---|---|
| 01 Environment | Partial | Spark test succeeded and the final H2O/Spark runs succeeded. Docker Desktop daemon and a reachable Kubernetes cluster are not currently available. |
| 02 Multi-month data | Complete | 12 raw Yellow Taxi Parquet files for January–December 2025; `raw_dataset_summary.csv` totals 48,722,602 rows. |
| 03 Cleaning | Complete | `cleaning_summary.csv` reports 46,714,819 clean trips and `taxi_clean.parquet` has `_SUCCESS`. |
| 04 Feature engineering | Complete with caveat | Required route, borough, and leakage-control fields are materialized. Route statistics are derived only from the train interval, but not strictly prior-in-time within that interval. |
| 05 Multi-source | Complete | Taxi Zone and PostgreSQL join completed: 8 PostgreSQL rows, 10,702,477 matches, `jdbc_status=CONNECTED`. |
| 06 Chronological split | Complete | Jan–Sep train 34,327,814; Oct validation 4,231,702; Nov–Dec test 8,155,303. |
| 07 Sparkling Water | Complete | Internal backend model runs succeeded. `outputs/logs/h2o_external_backend.log` contains `EXTERNAL H2O CONTEXT CONNECTED SUCCESSFULLY` and `EXTERNAL BACKEND TEST SUCCESS`. |
| 08 Baseline models | Complete | Current chronological GBM, DRF, and Deep Learning metrics are present. |
| 09 Grid Search | Complete | Current six-run chronological GBM grid results and selected parameters are present. |
| 10 AutoML | Complete | Current chronological AutoML leaderboard and test metrics are present. |
| 11 Model comparison | Complete | All five current chronological model results and the three required charts are present. |
| 12 Feature importance | Complete | Current `feature_importance.csv` and PNG are present. |
| 13 Error analysis | Complete | Current DRF results cover pickup hour, day of week, pickup borough, dropoff borough, trip-duration range, and route, with CSV/PNG evidence. |
| 14 Final model + MOJO | Complete | Final DRF artifact is in `models/final_mojo`; independent scoring completed all 8,155,303 test rows with zero null predictions. |
| 15 Batch prediction | Complete | Batch and MOJO prediction outputs have `_SUCCESS`; `batch_predictions_sample.csv` is now a normal UTF-8 CSV file. |
| 16 Structured Streaming | Complete | 10 micro-batches / 1,000 rows / zero null predictions completed; `outputs/logs/streaming.log` is retained. |
| 17 spark-submit | Complete | `spark_submit.log` records all ten demo sections and `FINAL INTEGRATED DEMO SUCCESS`. |
| 18 Docker | Partial | Dockerfile and automated validation exist, but the current Docker daemon is unavailable and the system drive has insufficient free space. No current `DOCKER FINAL VALIDATION SUCCESS` proof exists. |
| 19 Kubernetes | Partial | `kubernetes/final-job.yaml` and automated image-import/Job validation exist, but the Docker Desktop node is unreachable and no `KUBERNETES FINAL VALIDATION SUCCESS` proof exists. |
| 20 H2O Flow | Complete for local GUI demonstration | The medium local Flow demo completed with 200,000 train rows, 50,000 test rows and 50,000 predictions (`h2o_flow_demo_metrics.json`). Full 46.7-million-row Flow import remains optional and resource-limited; full-data processing evidence is provided by Spark ETL and MOJO scoring. |
| 20A Local dashboard | Complete | `src/app/server.py` serves metrics, charts, Taxi Zones and real one-row prediction through the locally available DRF, GBM Grid Search, GBM, AutoML and Deep Learning MOJOs. Unit tests, HTTP endpoints and all five MOJO self-tests succeeded. |
| 21 Integrated demo | Complete with deployment caveat | `spark_submit.log` contains `FINAL INTEGRATED DEMO SUCCESS`; its deployment section verifies artifact presence, not successful Docker/Kubernetes runtime deployment. |

## Current 12-month final result

- Dataset: 46,714,819 clean trips from January–December 2025.
- Chronological split: Jan–Sep train, Oct validation, Nov–Dec test.
- Deployment candidate: DRF (`models/final_mojo`).
- DRF test sample: RMSE 7.4383 minutes, MAE 4.5895 minutes, R² 0.7488 (400,000-row chronological test sample).
- Independent MOJO scoring: 8,155,303 test rows, zero null predictions, 24,066.78 rows/second.
- Streaming: 10 micro-batches, 1,000 rows, zero null predictions.

## Remaining technical closure items

1. Free at least 10 GB on the Windows system drive (or move Docker Desktop's disk image), start Docker Desktop, then run `scripts/validate_deployment_after_reboot.cmd`.
2. Retain `DOCKER FINAL VALIDATION SUCCESS` and `KUBERNETES FINAL VALIDATION SUCCESS` in their respective logs.
3. Dashboard light/dark screenshots are retained; capture the remaining H2O Flow screens needed for the live demonstration.
4. Consider strictly time-prior route statistics inside the training period if the leakage-safe claim is intended to be strict.
