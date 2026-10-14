@echo off
chcp 65001 >nul
setlocal

call "%~dp0activate_project.cmd"

echo.
echo ============================================================
echo NYC TAXI FINAL PIPELINE - 12 MONTHS (2025-01 TO 2025-12)
echo ============================================================

call :run "Download / validate 12 monthly files" python -m src.data.download_multimonth
if errorlevel 1 exit /b 1

if /i "%~1"=="--download-only" goto :success

call :run "Clean, engineer features and chronological split" python -m src.data.final_data_pipeline
if errorlevel 1 exit /b 1

call :run "Train final GBM" python -m src.models.train_final_gbm
if errorlevel 1 exit /b 1

call :run "Train DRF / Deep Learning and promote DRF" python -m src.models.train_final_additional_models --promote-drf
if errorlevel 1 exit /b 1

call :run "Final chronological GBM grid search" python -m src.models.gbm_grid_search
if errorlevel 1 exit /b 1

call :run "Final chronological H2O AutoML" python -m src.models.automl
if errorlevel 1 exit /b 1

call :run "Compare final models" python -m src.evaluation.compare_models
if errorlevel 1 exit /b 1

call :run "Create final comparison charts" python -m src.evaluation.visualize_metrics
if errorlevel 1 exit /b 1

call :run "Extract final feature importance" python -m src.evaluation.feature_importance
if errorlevel 1 exit /b 1

call :run "Final error analysis" python -m src.evaluation.final_error_analysis
if errorlevel 1 exit /b 1

call :run "Independent MOJO scoring - all test rows" python -m src.prediction.final_mojo_score --rows 0
if errorlevel 1 exit /b 1

call :run "Final MOJO structured streaming" python -m src.streaming.final_streaming
if errorlevel 1 exit /b 1

call :run "Final integrated demo" python -m src.demo.final_demo
if errorlevel 1 exit /b 1

:success
echo.
echo ============================================================
if /i "%~1"=="--download-only" (
    echo 12-MONTH DOWNLOAD CHECK SUCCESS
) else (
    echo 12-MONTH FINAL PIPELINE SUCCESS
)
echo ============================================================
exit /b 0

:run
set "STEP_NAME=%~1"
echo.
echo ------------------------------------------------------------
echo %STEP_NAME%
echo ------------------------------------------------------------
%2 %3 %4 %5 %6 %7 %8 %9
set "STEP_EXIT=%ERRORLEVEL%"
if not "%STEP_EXIT%"=="0" (
    echo.
    echo FAILED: %STEP_NAME% ^(exit code %STEP_EXIT%^)
    exit /b %STEP_EXIT%
)
exit /b 0
