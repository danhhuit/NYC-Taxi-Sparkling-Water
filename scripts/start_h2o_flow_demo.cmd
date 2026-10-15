@echo off
setlocal
chcp 65001 >nul

cd /d "%~dp0.."

if not exist "data\flow\local_demo\h2o_train_demo_200000.parquet" (
    call scripts\prepare_h2o_flow_demo.cmd
    if errorlevel 1 exit /b 1
)

if not exist "data\flow\local_demo\h2o_test_demo_50000.parquet" (
    call scripts\prepare_h2o_flow_demo.cmd --force
    if errorlevel 1 exit /b 1
)

call scripts\start_h2o_flow.cmd
exit /b %ERRORLEVEL%
