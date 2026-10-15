@echo off
setlocal
chcp 65001 >nul

call "%~dp0activate_project.cmd"

echo.
echo ============================================
echo PREPARING MEDIUM H2O FLOW GUI DATA
echo Train: 200,000 rows
echo Test :  50,000 rows
echo ============================================
echo.

.venv\Scripts\python.exe -m src.data.prepare_h2o_flow_demo %*
exit /b %ERRORLEVEL%
