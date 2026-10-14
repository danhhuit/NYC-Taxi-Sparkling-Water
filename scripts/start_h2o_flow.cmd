@echo off
setlocal
chcp 65001 >nul

call "%~dp0activate_project.cmd"

echo.
echo ============================================
echo STARTING H2O EXTERNAL CLUSTER
echo ============================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_h2o_cluster.ps1" ^
    -CloudName "nyc_taxi_external" ^
    -HeapGb 2 ^
    -StartupTimeoutSeconds 60 ^
    -OpenBrowser
exit /b %ERRORLEVEL%
