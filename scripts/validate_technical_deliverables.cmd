@echo off
setlocal
cd /d "%~dp0.."
call scripts\activate_project.cmd
if errorlevel 1 exit /b 1

echo.
echo ============================================================
echo VALIDATING FOUR TECHNICAL GRADING CRITERIA
echo Report/presentation and team-work are intentionally excluded.
echo ============================================================

".venv\Scripts\python.exe" -m src.evaluation.validate_technical_deliverables
exit /b %errorlevel%
