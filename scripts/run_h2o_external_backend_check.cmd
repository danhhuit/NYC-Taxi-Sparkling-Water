@echo off
setlocal
chcp 65001 >nul

call "%~dp0activate_project.cmd"

powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $c=Invoke-RestMethod -Uri 'http://127.0.0.1:54321/3/Cloud' -TimeoutSec 5; if (-not $c.cloud_healthy -or $c.cloud_name -ne 'nyc_taxi_external') { exit 1 }" >nul 2>&1
if errorlevel 1 (
    echo H2O cloud nyc_taxi_external is not healthy on port 54321.
    echo Run scripts\start_h2o_flow.cmd first.
    echo If startup fails, inspect outputs\logs\h2o_nyc_taxi_external.*.log.
    exit /b 1
)

if not exist "outputs\logs" mkdir "outputs\logs"

echo Running external Sparkling Water backend check. Output: outputs\logs\h2o_external_backend.log
.venv\Scripts\python.exe -m src.h2o.external_backend_test > "outputs\logs\h2o_external_backend.log" 2>&1
set "CHECK_EXIT=%ERRORLEVEL%"

type "outputs\logs\h2o_external_backend.log"
if "%CHECK_EXIT%"=="0" (
    echo.
    echo EXTERNAL BACKEND VALIDATION PASSED
    echo Windows JAR cleanup warnings, if shown above, are non-fatal.
)
exit /b %CHECK_EXIT%
