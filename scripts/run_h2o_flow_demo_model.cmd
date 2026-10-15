@echo off
setlocal
chcp 65001 >nul

call "%~dp0load_h2o_flow_demo.cmd"
if errorlevel 1 exit /b 1

.venv\Scripts\python.exe -m src.h2o.train_local_demo %*
exit /b %ERRORLEVEL%
