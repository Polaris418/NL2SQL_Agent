@echo off
setlocal
cd /d "%~dp0\..\.."
set "ROOT=%CD%"
set "PY=%ROOT%\.venv-win\Scripts\python.exe"
if not exist "%PY%" (
  echo Backend Python not found: %PY%
  exit /b 1
)
start "" /b "%PY%" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 1>"%ROOT%\tmp_uvicorn_out.log" 2>"%ROOT%\tmp_uvicorn_err.log"
