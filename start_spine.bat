@echo off
REM Morpheus Data Spine Launcher
REM Starts NATS server and all spine services
REM
REM Prerequisites:
REM   1. NATS server installed: choco install nats-server
REM      OR download from https://nats.io/download/
REM   2. Python dependencies: pip install nats-py msgpack
REM   3. Schwab credentials in .env file

echo ============================================
echo   Morpheus Data Spine
echo ============================================
echo.

REM Check if NATS is running
tasklist /FI "IMAGENAME eq nats-server.exe" 2>NUL | find /I "nats-server.exe" >NUL
if %ERRORLEVEL% NEQ 0 (
    echo [SPINE] Starting NATS server...
    start "NATS Server" nats-server -js -m 8222
    timeout /t 2 /nobreak >NUL
    echo [SPINE] NATS server started on port 4222 (monitor: 8222)
) else (
    echo [SPINE] NATS server already running
)

echo.
echo [SPINE] Starting Morpheus services...
echo.

cd /d %~dp0

REM Start the launcher which manages all services
python -m morpheus.spine.launcher %*

echo.
echo [SPINE] Spine stopped.
pause
