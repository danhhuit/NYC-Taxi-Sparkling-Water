@echo off
chcp 65001 >nul

call "%~dp0activate_project.cmd"

if "%H2O_HEAP_GB%"=="" set "H2O_HEAP_GB=8"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0test_h2o_flow_resources.ps1" -HeapGb %H2O_HEAP_GB%
if errorlevel 1 exit /b 1

netstat -ano | findstr /R /C:":54321 .*LISTENING" >nul
if not errorlevel 1 (
    echo.
    echo Port 54321 is already in use.
    echo Stop the existing H2O window with CTRL+C, then run this script again.
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0prepare_h2o_flow_full.ps1"
if errorlevel 1 exit /b 1

set "H2O_JAR=%CD%\.venv\Lib\site-packages\h2o\backend\bin\h2o.jar"
set "SW_EXT_JAR=D:\BigData\Tools\SparklingWater\sparkling-water-3.46.0.6-1-3.5\jars\sparkling-water-assembly-extensions_2.12-3.46.0.6-1-3.5-all.jar"

echo.
echo ============================================
echo STARTING H2O FLOW - FULL 12-MONTH DATA MODE
echo Java heap: %H2O_HEAP_GB% GB (maximum allowed: 10 GB)
echo ============================================

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_h2o_cluster.ps1" ^
    -CloudName "nyc_taxi_full_12_months" ^
    -HeapGb %H2O_HEAP_GB% ^
    -StartupTimeoutSeconds 90 ^
    -OpenBrowser
if errorlevel 1 exit /b 1

echo.
echo H2O Flow: http://127.0.0.1:54321
echo Import:
echo D:/BigData/NYC-Taxi-Sparkling-Water/data/flow/full_12_months
echo.
echo After H2O is ready, run:
echo .venv\Scripts\python.exe scripts\capture_h2o_flow_full.py
exit /b 0
