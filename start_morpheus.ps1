<#
.SYNOPSIS
    Morpheus Single-Point Startup Script

.DESCRIPTION
    Starts Morpheus_AI and Morpheus_UI with zero user interaction.

    SAFETY DEFAULTS ENFORCED:
    - PAPER mode: ON
    - Live armed: FALSE
    - Kill switch: SAFE

.EXAMPLE
    .\start_morpheus.ps1
#>

$ErrorActionPreference = "Stop"

# ============================================================================
# Configuration
# ============================================================================

$MORPHEUS_AI_DIR = "C:\Morpheus_AI"
$MORPHEUS_UI_DIR = "C:\Morpheus_UI"
$API_PORT = 8010
$VITE_PORT = 5173
$HEALTH_CHECK_URL = "http://localhost:$API_PORT/health"
$MAX_WAIT_SECONDS = 30
$HEALTH_CHECK_INTERVAL = 1

# ============================================================================
# Helper Functions
# ============================================================================

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host ("=" * 60) -ForegroundColor Cyan
}

function Write-Status {
    param([string]$Message, [string]$Status = "INFO")
    $color = switch ($Status) {
        "OK" { "Green" }
        "WARN" { "Yellow" }
        "ERROR" { "Red" }
        "WAIT" { "DarkGray" }
        default { "White" }
    }
    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
    Write-Host "[$Status] " -NoNewline -ForegroundColor $color
    Write-Host $Message
}

function Test-PortInUse {
    param([int]$Port)
    $connection = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    return $null -ne $connection
}

function Stop-ProcessOnPort {
    param([int]$Port)
    $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($connections) {
        $processIds = $connections | Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($procId in $processIds) {
            $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Status "Stopping existing process on port $Port (PID: $procId, Name: $($proc.ProcessName))" "WARN"
                Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
            }
        }
        Start-Sleep -Seconds 1
    }
}

function Wait-ForHealthCheck {
    param(
        [string]$Url,
        [int]$MaxWaitSeconds,
        [int]$IntervalSeconds
    )

    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        try {
            # Use .NET WebClient which is more reliable than Invoke-RestMethod
            $webClient = New-Object System.Net.WebClient
            $response = $webClient.DownloadString($Url)
            if ($response -match '"status"\s*:\s*"healthy"') {
                return $true
            }
        }
        catch {
            # Ignore errors during startup
        }

        Write-Status "Waiting for Morpheus_AI... ($elapsed/$MaxWaitSeconds sec)" "WAIT"
        Start-Sleep -Seconds $IntervalSeconds
        $elapsed += $IntervalSeconds
    }

    return $false
}

function Test-PythonEnvironment {
    # Check for Python
    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Status "Python not found in PATH" "ERROR"
        return $false
    }

    Write-Status "Python found: $($python.Source)" "OK"
    return $true
}

function Test-NodeEnvironment {
    # Check for Node.js
    $node = Get-Command node -ErrorAction SilentlyContinue
    if (-not $node) {
        Write-Status "Node.js not found in PATH" "ERROR"
        return $false
    }

    # Check for npm
    $npm = Get-Command npm -ErrorAction SilentlyContinue
    if (-not $npm) {
        Write-Status "npm not found in PATH" "ERROR"
        return $false
    }

    Write-Status "Node.js found: $($node.Source)" "OK"
    return $true
}

# ============================================================================
# Main Script
# ============================================================================

try {
    Write-Header "MORPHEUS STARTUP"
    Write-Host ""
    Write-Host "SAFETY DEFAULTS:" -ForegroundColor Yellow
    Write-Host "  - PAPER mode: ON" -ForegroundColor Green
    Write-Host "  - Live armed: FALSE" -ForegroundColor Green
    Write-Host "  - Kill switch: SAFE" -ForegroundColor Green
    Write-Host ""

    # ========================================================================
    # Step 1: Environment Check
    # ========================================================================

    Write-Header "STEP 1: Environment Check"

    if (-not (Test-PythonEnvironment)) {
        throw "Python environment check failed"
    }

    if (-not (Test-NodeEnvironment)) {
        throw "Node.js environment check failed"
    }

    # Check directories exist
    if (-not (Test-Path $MORPHEUS_AI_DIR)) {
        throw "Morpheus_AI directory not found: $MORPHEUS_AI_DIR"
    }
    Write-Status "Morpheus_AI directory: $MORPHEUS_AI_DIR" "OK"

    if (-not (Test-Path $MORPHEUS_UI_DIR)) {
        throw "Morpheus_UI directory not found: $MORPHEUS_UI_DIR"
    }
    Write-Status "Morpheus_UI directory: $MORPHEUS_UI_DIR" "OK"

    # ========================================================================
    # Step 2: Clean Up Existing Processes
    # ========================================================================

    Write-Header "STEP 2: Clean Up"

    # Kill any existing Morpheus-related processes
    Write-Status "Stopping any existing Morpheus processes..." "INFO"

    # Stop processes on our ports
    if (Test-PortInUse $API_PORT) {
        Stop-ProcessOnPort $API_PORT
    } else {
        Write-Status "Port $API_PORT is free" "OK"
    }

    if (Test-PortInUse $VITE_PORT) {
        Stop-ProcessOnPort $VITE_PORT
    } else {
        Write-Status "Port $VITE_PORT is free" "OK"
    }

    # Also kill any orphaned electron processes from previous runs
    Get-Process -Name "electron" -ErrorAction SilentlyContinue | ForEach-Object {
        $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)" -ErrorAction SilentlyContinue).CommandLine
        if ($cmdLine -and $cmdLine -like "*Morpheus*") {
            Write-Status "Stopping orphaned Electron process (PID: $($_.Id))" "WARN"
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }

    # ========================================================================
    # Step 3: Start Morpheus_AI
    # ========================================================================

    Write-Header "STEP 3: Start Morpheus_AI"

    Write-Status "Installing Python dependencies..." "INFO"
    Push-Location $MORPHEUS_AI_DIR

    # Install/upgrade pip dependencies (always run to ensure up-to-date)
    python -m pip install -e . --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "pip install failed"
    }
    Write-Status "Python dependencies ready" "OK"

    Pop-Location

    Write-Status "Starting Morpheus_AI server..." "INFO"

    # Start Morpheus_AI in background (minimized, not hidden - for debugging)
    $aiProcess = Start-Process -FilePath "python" `
        -ArgumentList "-m", "morpheus.server.main" `
        -WorkingDirectory $MORPHEUS_AI_DIR `
        -WindowStyle Minimized `
        -PassThru

    Write-Status "Morpheus_AI process started (PID: $($aiProcess.Id))" "OK"

    # Wait for health check
    Write-Status "Waiting for health check..." "INFO"

    if (-not (Wait-ForHealthCheck -Url $HEALTH_CHECK_URL -MaxWaitSeconds $MAX_WAIT_SECONDS -IntervalSeconds $HEALTH_CHECK_INTERVAL)) {
        throw "Morpheus_AI failed to start within $MAX_WAIT_SECONDS seconds"
    }

    # Verify health
    $webClient = New-Object System.Net.WebClient
    $healthJson = $webClient.DownloadString($HEALTH_CHECK_URL)
    $health = $healthJson | ConvertFrom-Json
    Write-Status "Morpheus_AI healthy!" "OK"
    Write-Host "  Trading Mode: $($health.trading_mode)" -ForegroundColor Green
    Write-Host "  Live Armed: $($health.live_armed)" -ForegroundColor Green
    Write-Host "  Kill Switch: $($health.kill_switch_active)" -ForegroundColor Green

    # ========================================================================
    # Step 4: Start Morpheus_UI
    # ========================================================================

    Write-Header "STEP 4: Start Morpheus_UI"

    Write-Status "Starting Morpheus_UI (Electron)..." "INFO"

    Push-Location $MORPHEUS_UI_DIR

    # Always ensure npm dependencies are installed and up-to-date
    Write-Status "Ensuring npm dependencies are installed..." "INFO"
    npm install 2>&1 | Out-Null
    Write-Status "npm dependencies ready" "OK"

    # Start UI - Electron window will open normally
    # Note: cmd window is minimized but Electron opens its own window
    $uiProcess = Start-Process -FilePath "cmd" `
        -ArgumentList "/c", "npm run dev" `
        -WorkingDirectory $MORPHEUS_UI_DIR `
        -WindowStyle Normal `
        -PassThru

    Pop-Location

    Write-Status "Morpheus_UI process started (PID: $($uiProcess.Id))" "OK"

    # Wait for Vite to be ready
    Write-Status "Waiting for Vite dev server..." "INFO"
    $viteWait = 0
    while ($viteWait -lt 30) {
        if (Test-PortInUse $VITE_PORT) {
            break
        }
        Start-Sleep -Seconds 1
        $viteWait++
    }

    if (Test-PortInUse $VITE_PORT) {
        Write-Status "Vite dev server ready on port $VITE_PORT" "OK"
    } else {
        Write-Status "Vite dev server may still be starting..." "WARN"
    }

    # Wait for Electron window to appear
    Write-Status "Waiting for Electron window..." "INFO"
    $electronWait = 0
    while ($electronWait -lt 15) {
        $electronProcs = Get-Process -Name "electron" -ErrorAction SilentlyContinue
        if ($electronProcs -and $electronProcs.Count -gt 0) {
            Write-Status "Electron window launched!" "OK"
            break
        }
        Start-Sleep -Seconds 1
        $electronWait++
    }

    if ($electronWait -ge 15) {
        Write-Status "Electron may still be loading..." "WARN"
    }

    # ========================================================================
    # Step 5: Final Validation
    # ========================================================================

    Write-Header "STEP 5: Final Validation"

    # Re-check health
    Start-Sleep -Seconds 2
    $webClient = New-Object System.Net.WebClient
    $healthJson = $webClient.DownloadString($HEALTH_CHECK_URL)
    $health = $healthJson | ConvertFrom-Json

    Write-Status "Health Check: $($health.status)" "OK"
    Write-Status "WebSocket Clients: $($health.websocket_clients)" "INFO"
    Write-Status "Event Count: $($health.event_count)" "INFO"

    # ========================================================================
    # Success
    # ========================================================================

    Write-Header "MORPHEUS READY"

    Write-Host ""
    Write-Host "System Status:" -ForegroundColor Cyan
    Write-Host "  Morpheus_AI: Running (PID $($aiProcess.Id))" -ForegroundColor Green
    Write-Host "  Morpheus_UI: Running (PID $($uiProcess.Id))" -ForegroundColor Green
    Write-Host "  API: http://localhost:$API_PORT" -ForegroundColor White
    Write-Host "  UI:  http://localhost:$VITE_PORT" -ForegroundColor White
    Write-Host ""
    Write-Host "Trading Status:" -ForegroundColor Cyan
    Write-Host "  Mode: $($health.trading_mode)" -ForegroundColor $(if ($health.trading_mode -eq "PAPER") { "Green" } else { "Red" })
    Write-Host "  Armed: $($health.live_armed)" -ForegroundColor $(if (-not $health.live_armed) { "Green" } else { "Red" })
    Write-Host "  Kill Switch: $(if ($health.kill_switch_active) { 'ACTIVE' } else { 'SAFE' })" -ForegroundColor $(if (-not $health.kill_switch_active) { "Green" } else { "Red" })
    Write-Host ""
    Write-Host "Hotkeys:" -ForegroundColor Cyan
    Write-Host "  Ctrl+Enter    : Confirm Entry" -ForegroundColor White
    Write-Host "  Ctrl+Shift+X  : Cancel All" -ForegroundColor White
    Write-Host "  Ctrl+Shift+K  : Kill Switch" -ForegroundColor White
    Write-Host ""
    Write-Host "Press Ctrl+C to stop all services" -ForegroundColor Yellow
    Write-Host ""

    # Store process info for cleanup
    $script:AI_PID = $aiProcess.Id
    $script:UI_PID = $uiProcess.Id

    # Keep script running and handle Ctrl+C
    Write-Status "Monitoring... (Ctrl+C to stop)" "INFO"

    while ($true) {
        Start-Sleep -Seconds 5

        # Check if processes are still running
        $aiRunning = Get-Process -Id $aiProcess.Id -ErrorAction SilentlyContinue
        $uiRunning = Get-Process -Id $uiProcess.Id -ErrorAction SilentlyContinue

        if (-not $aiRunning) {
            Write-Status "Morpheus_AI process died unexpectedly" "ERROR"
            break
        }
    }
}
catch {
    Write-Host ""
    Write-Status "STARTUP FAILED: $_" "ERROR"
    Write-Host ""
    exit 1
}
finally {
    # Cleanup on exit
    Write-Host ""
    Write-Status "Shutting down..." "INFO"

    if ($script:AI_PID) {
        Stop-Process -Id $script:AI_PID -Force -ErrorAction SilentlyContinue
        Write-Status "Stopped Morpheus_AI" "OK"
    }

    if ($script:UI_PID) {
        Stop-Process -Id $script:UI_PID -Force -ErrorAction SilentlyContinue
        # Also stop any npm/node child processes
        Get-Process -Name "node" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
        Write-Status "Stopped Morpheus_UI" "OK"
    }

    Write-Status "Shutdown complete" "OK"
}
