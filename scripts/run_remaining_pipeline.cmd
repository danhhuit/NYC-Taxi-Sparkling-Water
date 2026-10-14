@echo off
chcp 65001 >nul
setlocal
call "%~dp0activate_project.cmd"

call :run "Train and promote final DRF" python -m src.models.train_final_additional_models --models drf --promote-drf
if errorlevel 1 exit /b 1
call :run "GBM grid search" python -m src.models.gbm_grid_search
if errorlevel 1 exit /b 1
call :run "H2O AutoML" python -m src.models.automl
if errorlevel 1 exit /b 1
call :run "Model comparison" python -m src.evaluation.compare_models
if errorlevel 1 exit /b 1
call :run "Comparison charts" python -m src.evaluation.visualize_metrics
if errorlevel 1 exit /b 1
call :run "Feature importance" python -m src.evaluation.feature_importance
if errorlevel 1 exit /b 1
call :run "MOJO scoring on all test rows" python -m src.prediction.final_mojo_score --rows 0
if errorlevel 1 exit /b 1
call :run "Structured Streaming" python -m src.streaming.final_streaming
if errorlevel 1 exit /b 1
call scripts\run_spark_submit.cmd
if errorlevel 1 exit /b 1
call scripts\run_final_demo.cmd
if errorlevel 1 exit /b 1
echo REMAINING FINAL PIPELINE SUCCESS
exit /b 0

:run
set "STEP_NAME=%~1"
echo.
echo ============================================================
echo %STEP_NAME%
echo ============================================================
%2 %3 %4 %5 %6 %7 %8 %9
set "STEP_EXIT=%ERRORLEVEL%"
if not "%STEP_EXIT%"=="0" (
  echo FAILED: %STEP_NAME% ^(exit code %STEP_EXIT%^)
  exit /b %STEP_EXIT%
)
exit /b 0
