@echo off
chcp 65001 >nul

call "%~dp0activate_project.cmd"

echo.
echo ============================================
echo NYC TAXI - FINAL INTEGRATED DEMO
echo ============================================
echo.

python -m src.demo.final_demo

if errorlevel 1 (
    echo.
    echo ============================================
    echo FINAL DEMO INCOMPLETE OR FAILED
    echo ============================================
    exit /b 1
)

echo.
echo ============================================
echo FINAL INTEGRATED DEMO SUCCESS
echo ============================================
