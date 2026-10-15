@echo off
setlocal
chcp 65001 >nul

call "%~dp0activate_project.cmd"

powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $c=Invoke-RestMethod -Uri 'http://127.0.0.1:54321/3/Cloud' -TimeoutSec 5; if (-not $c.cloud_healthy -or $c.cloud_name -ne 'nyc_taxi_external') { exit 1 }" >nul 2>&1
if errorlevel 1 (
    echo H2O cloud nyc_taxi_external is not healthy on port 54321.
    echo Run scripts\start_h2o_flow_demo.cmd first.
    exit /b 1
)

.venv\Scripts\python.exe -m src.h2o.load_local_demo %*
exit /b %ERRORLEVEL%
