@echo off
chcp 65001 >nul

call "%~dp0activate_project.cmd"

echo.
echo ============================================
echo STARTING H2O EXTERNAL CLUSTER
echo ============================================
echo.

set "H2O_JAR=%CD%\.venv\Lib\site-packages\h2o\backend\bin\h2o.jar"

set "SW_EXT_JAR=D:\BigData\Tools\SparklingWater\sparkling-water-3.46.0.6-1-3.5\jars\sparkling-water-assembly-extensions_2.12-3.46.0.6-1-3.5-all.jar"

start "NYC TAXI - H2O CLUSTER" cmd /k ^
java -cp "%H2O_JAR%;%SW_EXT_JAR%" water.H2OApp ^
-allow_clients ^
-name nyc_taxi_external ^
-ip 127.0.0.1 ^
-web_ip 127.0.0.1 ^
-port 54321

echo Waiting for H2O...
timeout /t 5 /nobreak >nul

echo.
echo Opening H2O Flow...
start "" http://127.0.0.1:54321

echo.
echo ============================================
echo H2O FLOW
echo http://127.0.0.1:54321
echo ============================================