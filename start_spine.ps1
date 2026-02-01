# Morpheus Data Spine Launcher (PowerShell)
# Starts NATS server and all spine services
#
# Prerequisites:
#   1. NATS server installed: choco install nats-server
#      OR download from https://nats.io/download/
#   2. Python dependencies: pip install nats-py msgpack
#   3. Schwab credentials in .env file
#
# Usage:
#   .\start_spine.ps1
#   .\start_spine.ps1 -Services "schwab_feed,ai_core,ui_gateway"

param(
    [string]$Services = "",
    [string]$Config = ""
)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Morpheus Data Spine" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Check if NATS is running
$natsProcess = Get-Process -Name "nats-server" -ErrorAction SilentlyContinue
if (-not $natsProcess) {
    Write-Host "[SPINE] Starting NATS server..." -ForegroundColor Yellow
    Start-Process -FilePath "nats-server" -ArgumentList "-js", "-m", "8222" -WindowStyle Minimized
    Start-Sleep -Seconds 2
    Write-Host "[SPINE] NATS server started on port 4222 (monitor: 8222)" -ForegroundColor Green
} else {
    Write-Host "[SPINE] NATS server already running (PID: $($natsProcess.Id))" -ForegroundColor Green
}

Write-Host ""
Write-Host "[SPINE] Starting Morpheus services..." -ForegroundColor Yellow
Write-Host ""

# Build command arguments
$args = @()
if ($Services) {
    $args += "--services"
    $args += $Services
}
if ($Config) {
    $args += "--config"
    $args += $Config
}

# Start the launcher
Set-Location $PSScriptRoot
try {
    python -m morpheus.spine.launcher @args
} catch {
    Write-Host "[SPINE] Error: $_" -ForegroundColor Red
} finally {
    Write-Host ""
    Write-Host "[SPINE] Spine stopped." -ForegroundColor Yellow
}
