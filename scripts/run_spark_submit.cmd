@echo off
chcp 65001 >nul
setlocal
call "%~dp0activate_project.cmd"

if not exist "outputs\logs" mkdir "outputs\logs"

echo Running final demo with spark-submit...
"%SPARK_HOME%\bin\spark-submit.cmd" ^
  --properties-file "config\spark-defaults.conf" ^
  "src\demo\spark_submit_entry.py" ^
  > "outputs\logs\spark_submit.log" 2>&1

set "EXIT_CODE=%ERRORLEVEL%"
type "outputs\logs\spark_submit.log"
if not "%EXIT_CODE%"=="0" (
  echo SPARK-SUBMIT FINAL DEMO FAILED
  exit /b %EXIT_CODE%
)

echo SPARK-SUBMIT FINAL DEMO SUCCESS
exit /b 0
