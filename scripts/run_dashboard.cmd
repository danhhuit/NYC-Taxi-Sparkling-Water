@echo off
setlocal
cd /d "%~dp0.."
call scripts\activate_project.cmd
if errorlevel 1 exit /b 1

echo.
echo ============================================================
echo NYC TAXI LOCAL DASHBOARD
echo ============================================================
echo URL: http://127.0.0.1:8501
echo Press Ctrl+C to stop.
echo.

powershell -NoProfile -Command "if (Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" >nul 2>&1
if not errorlevel 1 (
    echo ERROR: Port 8501 is already in use.
    echo Close the old dashboard terminal with Ctrl+C, then run this script again.
    exit /b 2
)

".venv\Scripts\python.exe" -m src.app.server --host 127.0.0.1 --port 8501 --open-browser
exit /b %errorlevel%
