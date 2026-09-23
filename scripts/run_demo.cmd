@echo off
chcp 65001 >nul

echo ============================================
echo NYC TAXI - INTEGRATED DEMO
echo ============================================

call "%~dp0activate_project.cmd"

echo.
echo ============================================
echo STARTING INTEGRATED DEMO...
echo ============================================
echo.

python src\22_integrated_demo.py

if errorlevel 1 (
    echo.
    echo ============================================
    echo DEMO FAILED - CHECK ERROR ABOVE
    echo ============================================
    exit /b 1
)

echo.
echo ============================================
echo DEMO FINISHED SUCCESSFULLY
echo ============================================