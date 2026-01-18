@echo off
:: Morpheus Single-Point Startup
:: Wrapper for PowerShell script

echo.
echo ============================================================
echo MORPHEUS STARTUP
echo ============================================================
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0start_morpheus.ps1"
