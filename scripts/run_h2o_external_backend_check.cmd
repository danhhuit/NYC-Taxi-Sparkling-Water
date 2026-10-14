@echo off
setlocal
chcp 65001 >nul

call "%~dp0activate_project.cmd"

netstat -ano | findstr /R /C:":54321 .*LISTENING" >nul
if errorlevel 1 (
    echo H2O is not listening on port 54321.
    echo Start scripts\start_h2o_flow.cmd first; it creates cloud nyc_taxi_external.
    exit /b 1
)

if not exist "outputs\logs" mkdir "outputs\logs"

echo Running external Sparkling Water backend check. Output: outputs\logs\h2o_external_backend.log
.venv\Scripts\python.exe -m src.h2o.external_backend_test > "outputs\logs\h2o_external_backend.log" 2>&1
set "CHECK_EXIT=%ERRORLEVEL%"

type "outputs\logs\h2o_external_backend.log"
exit /b %CHECK_EXIT%
