#!/usr/bin/env pwsh

param(
    [Parameter(Position=0)]
    [string]$Param1,
    
    [Parameter(Position=1)]
    [string]$Param2
)

$ProgressPreference = 'SilentlyContinue'

$INSTALL_DIR = "$env:LOCALAPPDATA\fluent-bit"
$BIN_DIR = "$INSTALL_DIR\bin"
$FLUENT_BIT_EXE = "$BIN_DIR\fluent-bit.exe"
$CONFIG_FILE = "$PSScriptRoot\fluent-bit.conf"
$PID_FILE = "$PSScriptRoot\fluent-bit.pid"

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
        Write-Host ""
        Write-Info "To see output, run: .\fluent-bit.ps1 debug"
    }
    else {
        Write-Warning "Fluent Bit is not running"
        if (Test-Path $PID_FILE) {
            Write-Info "Cleaning up stale PID file..."
            Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        }
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
        $downloadUrl = "https://packages.fluentbit.io/windows/fluent-bit-$version-win64.zip"
        $zipFile = "$env:TEMP\fluent-bit-$version-win64.zip"
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
        Write-Info "Use 'fluent-bit.ps1 stop' to stop it first"
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
    
    # Start Fluent Bit process in background (no logging)
    $process = Start-Process -FilePath $FLUENT_BIT_EXE `
        -ArgumentList "-c", "`"$CONFIG_FILE`"" `
        -WorkingDirectory $BIN_DIR `
        -WindowStyle Hidden `
        -PassThru
    
    # Save PID
    $process.Id | Out-File -FilePath $PID_FILE -Force
    
    Write-Info "Process started with PID: $($process.Id)"
    Start-Sleep -Seconds 3
    
    # Check if process is still running
    $stillRunning = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    
    if (-not $stillRunning) {
        Write-ErrorMsg "Fluent Bit exited immediately"
        Write-ErrorMsg "Run '.\fluent-bit.ps1 debug' to see error details"
        Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        exit 1
    }
    else {
        Write-Info "Fluent Bit started successfully (PID: $($process.Id))"
        Write-Host ""
        Write-Info "To debug: .\fluent-bit.ps1 debug"
        Write-Info "To check status: .\fluent-bit.ps1 status"
        Write-Info "To stop: .\fluent-bit.ps1 stop"
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
        [string]$CredentialsBase64
    )
    
    try {
        $credBytes = [Convert]::FromBase64String($CredentialsBase64)
        $credentials = [Text.Encoding]::UTF8.GetString($credBytes)
    }
    catch {
        Write-ErrorMsg "Failed to decode base64 credentials"
        exit 1
    }
    
    $parts = $credentials -split ':', 2
    if ($parts.Length -ne 2) {
        Write-ErrorMsg "Invalid credentials format"
        exit 1
    }
    
    $username = $parts[0]
    $password = $parts[1]
    
    if ([string]::IsNullOrWhiteSpace($IngestorHost) -or [string]::IsNullOrWhiteSpace($username) -or [string]::IsNullOrWhiteSpace($password)) {
        Write-ErrorMsg "Invalid credentials"
        exit 1
    }
    
    Install-FluentBit
    
    $configContent = @"
[SERVICE]
    flush                     1
    log_level                 info

[INPUT]
    Name                      windows_exporter_metrics
    Tag                       node_metrics
    Scrape_interval           1
    # Collect only essential metrics
    metrics          cpu

[OUTPUT]
    Name                      opentelemetry
    Match                     node_metrics
    Host                      ec9cfee0-2fd4-45eb-8209-d7cd992c4bcc-ingestor.workspace-staging.parseable.com
    Port                      443
    Metrics_uri               /v1/metrics
    Log_response_payload      False
    TLS                       On
    Http_User                 nikhil.sinha@parseable.com
    Http_Passwd               NH7oCUju
    Header                    X-P-Stream node-metrics
    Header                    X-P-Log-Source otel-metrics
    Compress                  gzip
"@
    
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
  Setup:   .\fluent-bit.ps1 [host] [base64_credentials]
  Stop:    .\fluent-bit.ps1 stop
  Start:   .\fluent-bit.ps1 start
  Restart: .\fluent-bit.ps1 restart
  Status:  .\fluent-bit.ps1 status
  Debug:   .\fluent-bit.ps1 debug     - Run in foreground to see output

To encode credentials:
  `$creds = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("username:password"))
  .\fluent-bit.ps1 your-host.com `$creds
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
        if ([string]::IsNullOrWhiteSpace($Param2)) {
            Write-ErrorMsg "Usage: .\fluent-bit.ps1 [host] [base64_credentials]"
            Write-ErrorMsg "   Or: .\fluent-bit.ps1 [start|stop|restart|status|debug|help]"
            exit 1
        }
        Setup-FluentBit -IngestorHost $Param1 -CredentialsBase64 $Param2
    }
}