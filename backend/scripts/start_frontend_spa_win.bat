@echo off
setlocal
cd /d "%~dp0\..\.."
set "ROOT=%CD%"
set "DIST=%ROOT%\NL2SQL Agent Frontend Development\dist"
if not exist "%DIST%\index.html" (
  echo Frontend dist not found: %DIST%
  exit /b 1
)
start "" /b py -3.13 "%ROOT%\backend\scripts\serve_frontend_spa.py" --directory "%DIST%" --host 0.0.0.0 --port 5173 1>"%ROOT%\frontend_out.log" 2>"%ROOT%\frontend_err.log"
