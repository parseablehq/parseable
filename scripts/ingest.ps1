#!/usr/bin/env pwsh

param(
    [Parameter(Position=0)]
    [string]$Param1,
    
    [Parameter(Position=1)]
    [string]$Param2,

    [Parameter(Position=2)]
    [string]$Param3,

    [Parameter(Position=3)]
    [string]$Param4
)

$ProgressPreference = 'SilentlyContinue'

$INSTALL_DIR = "$env:LOCALAPPDATA\fluent-bit"
$BIN_DIR = "$INSTALL_DIR\bin"
$FLUENT_BIT_EXE = "$BIN_DIR\fluent-bit.exe"
$CONFIG_FILE = "$PSScriptRoot\fluent-bit.conf"
$PID_FILE = "$PSScriptRoot\fluent-bit.pid"
$LOG_FILE = "$PSScriptRoot\fluent-bit.log"
$ERROR_LOG_FILE = "$PSScriptRoot\fluent-bit.err.log"
$SCRIPT_PATH = $PSCommandPath
if ([string]::IsNullOrWhiteSpace($SCRIPT_PATH)) {
    $SCRIPT_PATH = $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($SCRIPT_PATH)) {
    $SCRIPT_PATH = Join-Path $PSScriptRoot "ingest.ps1"
}
$SCRIPT_CMD = "powershell -NoProfile -ExecutionPolicy Bypass -File '$SCRIPT_PATH'"

$SUPPORTED_ARCH = @("AMD64", "ARM64")

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Write-ErrorMsg {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Test-FluentBitRunning {
    if (Test-Path $PID_FILE) {
        $processId = Get-Content $PID_FILE
        try {
            $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
            if ($process) {
                return $true
            }
        }
        catch {
            return $false
        }
    }
    return $false
}

function Stop-FluentBit {
    if (Test-FluentBitRunning) {
        $processId = Get-Content $PID_FILE
        Write-Info "Stopping Fluent Bit (PID: $processId)..."
        
        try {
            Stop-Process -Id $processId -Force -ErrorAction Stop
            Start-Sleep -Seconds 2
            Write-Info "Fluent Bit stopped successfully"
            Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        }
        catch {
            Write-ErrorMsg "Failed to stop Fluent Bit: $_"
            exit 1
        }
    }
    else {
        Write-Warning "Fluent Bit is not running"
    }
}

function Show-Status {
    if (Test-FluentBitRunning) {
        $processId = Get-Content $PID_FILE
        Write-Info "Fluent Bit is running (PID: $processId)"
        Write-Host ""
        Write-Info "Process details:"
        Get-Process -Id $processId | Format-Table Id, ProcessName, CPU, WS, StartTime -AutoSize
        Write-Host ""
        Write-Info "Config file: $CONFIG_FILE"
        Write-Info "Log file: $LOG_FILE"
        Write-Info "Error log file: $ERROR_LOG_FILE"
        Write-Host ""
        Write-Info "To see logs: $SCRIPT_CMD logs"
        Write-Info "To stop: $SCRIPT_CMD stop"
    }
    else {
        Write-Warning "Fluent Bit is not running"
        if (Test-Path $PID_FILE) {
            Write-Info "Cleaning up stale PID file..."
            Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        }
    }
}

function Show-Logs {
    if (Test-Path $LOG_FILE) {
        Write-Info "Showing last 80 stdout log lines from $LOG_FILE"
        Get-Content -Path $LOG_FILE -Tail 80
    }
    else {
        Write-Warning "Stdout log file not found: $LOG_FILE"
    }

    if (Test-Path $ERROR_LOG_FILE) {
        Write-Host ""
        Write-Info "Showing last 80 stderr log lines from $ERROR_LOG_FILE"
        Get-Content -Path $ERROR_LOG_FILE -Tail 80
    }
    else {
        Write-Warning "Stderr log file not found: $ERROR_LOG_FILE"
    }
}

function Get-Architecture {
    $arch = $env:PROCESSOR_ARCHITECTURE
    if ($arch -eq "AMD64") {
        return "AMD64"
    }
    elseif ($arch -eq "ARM64") {
        return "ARM64"
    }
    else {
        Write-ErrorMsg "Unsupported architecture: $arch"
        exit 1
    }
}

function Install-FluentBit {
    
    $arch = Get-Architecture
    
    if ($SUPPORTED_ARCH -notcontains $arch) {
        Write-ErrorMsg "Unsupported CPU architecture: $arch"
        exit 1
    }
    
    if (Test-Path $FLUENT_BIT_EXE) {
        $version = & $FLUENT_BIT_EXE --version 2>$null | Select-Object -First 1
        return
    }
    
    if (-not (Test-Path $INSTALL_DIR)) {
        New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
    }
    if (-not (Test-Path $BIN_DIR)) {
        New-Item -ItemType Directory -Path $BIN_DIR -Force | Out-Null
    }
    
    try {
        $version = "3.2.2"
        $archSuffix = if ($arch -eq "ARM64") { "winarm64" } else { "win64" }
        $downloadUrl = "https://packages.fluentbit.io/windows/fluent-bit-$version-$archSuffix.zip"
        $zipFile = "$env:TEMP\fluent-bit-$version-$archSuffix.zip"
        Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile
        Expand-Archive -Path $zipFile -DestinationPath $INSTALL_DIR -Force
        
        $extractedExe = Get-ChildItem -Path $INSTALL_DIR -Filter "fluent-bit.exe" -Recurse | Select-Object -First 1
        
        if ($extractedExe) {
            Copy-Item -Path $extractedExe.FullName -Destination $FLUENT_BIT_EXE -Force
            
            $dllPath = Split-Path $extractedExe.FullName
            Get-ChildItem -Path $dllPath -Filter "*.dll" -ErrorAction SilentlyContinue | ForEach-Object {
                Copy-Item -Path $_.FullName -Destination $BIN_DIR -Force
            }
            
            $pluginsDir = Join-Path $dllPath "plugins"
            if (Test-Path $pluginsDir) {
                $targetPluginsDir = Join-Path $BIN_DIR "plugins"
                if (-not (Test-Path $targetPluginsDir)) {
                    New-Item -ItemType Directory -Path $targetPluginsDir -Force | Out-Null
                }
                Copy-Item -Path "$pluginsDir\*" -Destination $targetPluginsDir -Recurse -Force
            }
        }
        else {
            Write-ErrorMsg "Could not find fluent-bit.exe in the downloaded package"
            exit 1
        }
        
        Remove-Item $zipFile -Force -ErrorAction SilentlyContinue
        
        $installedVersion = & $FLUENT_BIT_EXE --version 2>$null | Select-Object -First 1
    }
    catch {
        Write-ErrorMsg "Failed to install Fluent Bit: $_"
        exit 1
    }
}

function Start-FluentBit {
    if (Test-FluentBitRunning) {
        $processId = Get-Content $PID_FILE
        Write-Warning "Fluent Bit is already running (PID: $processId)"
        Write-Info "Use '$SCRIPT_CMD stop' to stop it first"
        return
    }
    
    if (-not (Test-Path $CONFIG_FILE)) {
        Write-ErrorMsg "Configuration file not found: $CONFIG_FILE"
        Write-ErrorMsg "Please run setup first"
        exit 1
    }
    
    if (-not (Test-Path $FLUENT_BIT_EXE)) {
        Write-ErrorMsg "Fluent Bit not installed. Installing..."
        Install-FluentBit
    }
    
    $version = & $FLUENT_BIT_EXE --version 2>$null | Select-Object -First 1
    
    Remove-Item $LOG_FILE, $ERROR_LOG_FILE -ErrorAction SilentlyContinue

    # Start Fluent Bit process in background and capture logs
    $process = Start-Process -FilePath $FLUENT_BIT_EXE `
        -ArgumentList "-c", "`"$CONFIG_FILE`"" `
        -WorkingDirectory $BIN_DIR `
        -WindowStyle Hidden `
        -RedirectStandardOutput $LOG_FILE `
        -RedirectStandardError $ERROR_LOG_FILE `
        -PassThru
    
    # Save PID
    $process.Id | Out-File -FilePath $PID_FILE -Force
    
    Write-Info "Process started with PID: $($process.Id)"
    Start-Sleep -Seconds 3
    
    # Check if process is still running
    $stillRunning = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    
    if (-not $stillRunning) {
        Write-ErrorMsg "Fluent Bit exited immediately"
        Write-ErrorMsg "Run '$SCRIPT_CMD debug' to see error details"
        Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        exit 1
    }
    else {
        Write-Info "Fluent Bit started successfully (PID: $($process.Id))"
        Write-Host ""
        Write-Info "To debug: $SCRIPT_CMD debug"
        Write-Info "To check status: $SCRIPT_CMD status"
        Write-Info "To see logs: $SCRIPT_CMD logs"
        Write-Info "To stop: $SCRIPT_CMD stop"
    }
}

function Restart-FluentBit {
    Stop-FluentBit
    Start-Sleep -Seconds 2
    Start-FluentBit
}

function Setup-FluentBit {
    param(
        [string]$IngestorHost,
        [string]$StreamName,
        [string]$ApiKey,
        [string]$TenantId
    )
    
    if ([string]::IsNullOrWhiteSpace($IngestorHost) -or [string]::IsNullOrWhiteSpace($StreamName) -or [string]::IsNullOrWhiteSpace($ApiKey)) {
        Write-ErrorMsg "Invalid setup parameters"
        exit 1
    }

    $tlsSetting = "On"
    $defaultPort = "443"
    if ($IngestorHost -like "https://*") {
        $IngestorHost = $IngestorHost.Substring("https://".Length)
        $tlsSetting = "On"
        $defaultPort = "443"
    }
    elseif ($IngestorHost -like "http://*") {
        $IngestorHost = $IngestorHost.Substring("http://".Length)
        $tlsSetting = "Off"
        $defaultPort = "80"
    }
    $IngestorHost = ($IngestorHost -split '/', 2)[0]

    if ($IngestorHost -match '^(.*):([0-9]+)$') {
        $Port = $Matches[2]
        $IngestorHost = $Matches[1]
    }
    else {
        $Port = $defaultPort
    }

    if ([string]::IsNullOrWhiteSpace($IngestorHost)) {
        Write-ErrorMsg "Invalid host"
        exit 1
    }

    $portNumber = 0
    if (-not [int]::TryParse($Port, [ref]$portNumber) -or $portNumber -lt 1 -or $portNumber -gt 65535) {
        Write-ErrorMsg "Invalid port: $Port"
        Write-ErrorMsg "Port must be a number between 1 and 65535"
        exit 1
    }

    Install-FluentBit

    $configLines = @(
        "[SERVICE]",
        "    flush                     1",
        "    log_level                 info",
        "",
        "[INPUT]",
        "    Name                      windows_exporter_metrics",
        "    Tag                       node_metrics",
        "    Scrape_interval           1",
        "    # Collect only essential metrics",
        "    metrics                   cpu",
        "",
        "[OUTPUT]",
        "    Name                      opentelemetry",
        "    Match                     node_metrics",
        "    Host                      $IngestorHost",
        "    Port                      $Port",
        "    Metrics_uri               /v1/metrics",
        "    Log_response_payload      True",
        "    TLS                       $tlsSetting",
        "    Grpc                      Off",
        "    Http2                     Off",
        "    Header                    X-API-Key $ApiKey"
    )

    if (-not [string]::IsNullOrWhiteSpace($TenantId)) {
        $configLines += "    Header                    X-P-Tenant $TenantId"
    }

    $configLines += @(
        "    Header                    X-P-Stream $StreamName",
        "    Header                    X-P-Log-Source otel-metrics"
    )

    $configContent = ($configLines -join [Environment]::NewLine) + [Environment]::NewLine
    
    # Use UTF8 without BOM (important for Fluent Bit)
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($CONFIG_FILE, $configContent, $utf8NoBom)
    
    Write-Host ""
    Start-FluentBit
}

function Show-Help {
    Write-Host @"
Fluent Bit Setup and Management Script for Windows

Usage:
  Setup:   $SCRIPT_CMD [host[:port]] [stream] [api_key] [tenant_id]
  Stop:    $SCRIPT_CMD stop
  Start:   $SCRIPT_CMD start
  Restart: $SCRIPT_CMD restart
  Status:  $SCRIPT_CMD status
  Logs:    $SCRIPT_CMD logs
  Debug:   $SCRIPT_CMD debug

Example:
  $SCRIPT_CMD https://your-host.com:443 node-metrics px_api_key
  $SCRIPT_CMD http://localhost:8000 node-metrics px_api_key tenant-id

"@
}

function Debug-FluentBit {
    if (-not (Test-Path $CONFIG_FILE)) {
        Write-ErrorMsg "Configuration file not found: $CONFIG_FILE"
        exit 1
    }
    
    if (-not (Test-Path $FLUENT_BIT_EXE)) {
        Write-ErrorMsg "Fluent Bit not installed"
        exit 1
    }
    Write-Info "Config: $CONFIG_FILE"
    Write-Host ""
    
    & $FLUENT_BIT_EXE -c "$CONFIG_FILE"
}

if ([string]::IsNullOrWhiteSpace($Param1)) {
    Show-Help
    exit 0
}

switch ($Param1.ToLower()) {
    "stop" {
        Stop-FluentBit
    }
    "restart" {
        Restart-FluentBit
    }
    "start" {
        Start-FluentBit
    }
    "status" {
        Show-Status
    }
    "logs" {
        Show-Logs
    }
    "debug" {
        Debug-FluentBit
    }
    "help" {
        Show-Help
    }
    "-h" {
        Show-Help
    }
    "--help" {
        Show-Help
    }
    default {
        if ([string]::IsNullOrWhiteSpace($Param2) -or [string]::IsNullOrWhiteSpace($Param3)) {
            Write-ErrorMsg "Usage: $SCRIPT_CMD [host[:port]] [stream] [api_key] [tenant_id]"
            Write-ErrorMsg "   Or: $SCRIPT_CMD [start|stop|restart|status|logs|debug|help]"
            exit 1
        }
        Setup-FluentBit -IngestorHost $Param1 -StreamName $Param2 -ApiKey $Param3 -TenantId $Param4
    }
}
