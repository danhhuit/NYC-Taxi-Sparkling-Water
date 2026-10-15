@echo off
setlocal
cd /d "%~dp0.."
call scripts\activate_project.cmd
if errorlevel 1 exit /b 1

".venv\Scripts\python.exe" -m src.app.server --self-test
exit /b %errorlevel%
