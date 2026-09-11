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

$COLLECTOR_VERSION = "0.157.0"
$INSTALL_DIR = "$env:LOCALAPPDATA\parseable-otelcol"
$COLLECTOR_EXE = "$INSTALL_DIR\otelcol.exe"
$SCRIPT_DIR = if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    (Get-Location).Path
}
else {
    $PSScriptRoot
}
$CONFIG_FILE = Join-Path $SCRIPT_DIR "otelcol.yaml"
$PID_FILE = Join-Path $SCRIPT_DIR "otelcol.pid"
$LOG_FILE = Join-Path $SCRIPT_DIR "otelcol.log"
$ERROR_LOG_FILE = Join-Path $SCRIPT_DIR "otelcol.err.log"

$SCRIPT_PATH = $PSCommandPath
if ([string]::IsNullOrWhiteSpace($SCRIPT_PATH)) {
    $SCRIPT_PATH = $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($SCRIPT_PATH)) {
    $SCRIPT_PATH = Join-Path $SCRIPT_DIR "ingest.ps1"
}
$SCRIPT_CMD = "powershell -NoProfile -ExecutionPolicy Bypass -File '$SCRIPT_PATH'"

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

function ConvertTo-PowerShellSingleQuoted {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Write-ParseableBanner {
    $accent = "$([char]27)[38;2;158;158;240m"
    $reset = "$([char]27)[0m"
    $logo = @(
        ' ____   _    ____  ____  _____    _    ____  _     _____',
        '|  _ \ / \  |  _ \/ ___|| ____|  / \  | __ )| |   | ____|',
        '| |_) / _ \ | |_) \___ \|  _|   / _ \ |  _ \| |   |  _|',
        '|  __/ ___ \|  _ < ___) | |___ / ___ \| |_) | |___| |___',
        '|_| /_/   \_\_| \_\____/|_____/_/   \_\____/|_____|_____|'
    )

    Write-Host ""
    $logo | ForEach-Object { Write-Host "$accent$_$reset" }
    Write-Host ""
    Write-Host "Host metrics setup" -ForegroundColor White
    Write-Host "Installing and configuring the metrics agent for Parseable..."
    Write-Host ""
}

function Write-SetupComplete {
    param([string]$StreamName)

    $accent = "$([char]27)[38;2;158;158;240m"
    $ok = "$([char]27)[38;2;52;211;153m"
    $reset = "$([char]27)[0m"
    Write-Host ""
    Write-Host "${ok}[OK] You're all set!${reset}"
    Write-Host "Host metrics are now being sent to Parseable as OTLP JSON."
    Write-Host "Dataset: " -NoNewline
    Write-Host "${accent}${StreamName}${reset}"
    Write-Host "Return to Parseable and click Continue to verify your data."
}

function Get-Architecture {
    $arch = $env:PROCESSOR_ARCHITEW6432
    if ([string]::IsNullOrWhiteSpace($arch)) {
        $arch = $env:PROCESSOR_ARCHITECTURE
    }
    if ($arch -eq "AMD64") {
        return "AMD64"
    }
    if ($arch -eq "ARM64") {
        return "ARM64"
    }

    Write-ErrorMsg "Unsupported CPU architecture: $arch"
    exit 1
}

function Test-CollectorRunning {
    if (Test-Path $PID_FILE) {
        $processId = Get-Content $PID_FILE

        try {
            $process = Get-CimInstance -ClassName Win32_Process -Filter "ProcessId = $processId" -ErrorAction Stop
        }
        catch {
            throw "Unable to inspect process $processId; PID file was preserved. $_"
        }

        if ($null -eq $process) {
            Remove-Item $PID_FILE -ErrorAction SilentlyContinue
            return $false
        }

        if ([string]::IsNullOrWhiteSpace($process.ExecutablePath)) {
            throw "Unable to verify process $processId; PID file was preserved."
        }

        $actualPath = [System.IO.Path]::GetFullPath($process.ExecutablePath)
        $expectedPath = [System.IO.Path]::GetFullPath($COLLECTOR_EXE)
        if ([System.StringComparer]::OrdinalIgnoreCase.Equals($actualPath, $expectedPath)) {
            return $true
        }

        Remove-Item $PID_FILE -ErrorAction SilentlyContinue
    }
    return $false
}

function Stop-Collector {
    if (Test-CollectorRunning) {
        $processId = Get-Content $PID_FILE
        Write-Info "Stopping OpenTelemetry Collector (PID: $processId)..."

        try {
            Stop-Process -Id $processId -Force -ErrorAction Stop
            for ($attempt = 0; $attempt -lt 10; $attempt++) {
                if ($null -eq (Get-Process -Id $processId -ErrorAction SilentlyContinue)) {
                    break
                }
                Start-Sleep -Seconds 1
            }

            if ($null -ne (Get-Process -Id $processId -ErrorAction SilentlyContinue)) {
                throw "Process $processId did not stop within 10 seconds; PID file was preserved."
            }

            Remove-Item $PID_FILE -Force -ErrorAction Stop
            Write-Info "OpenTelemetry Collector stopped successfully"
        }
        catch {
            Write-ErrorMsg "Failed to stop OpenTelemetry Collector: $_"
            exit 1
        }
    }
    else {
        Write-Warning "OpenTelemetry Collector is not running"
    }
}

function Show-Status {
    if (Test-CollectorRunning) {
        $processId = Get-Content $PID_FILE
        Write-Info "OpenTelemetry Collector is running (PID: $processId)"
        Write-Host ""
        Get-Process -Id $processId | Format-Table Id, ProcessName, CPU, WS, StartTime -AutoSize
        Write-Info "Config file: $CONFIG_FILE"
        Write-Info "Log file: $LOG_FILE"
        Write-Info "Error log file: $ERROR_LOG_FILE"
        $pidFileCommand = ConvertTo-PowerShellSingleQuoted $PID_FILE
        $errorLogFileCommand = ConvertTo-PowerShellSingleQuoted $ERROR_LOG_FILE
        Write-Info "To see logs: Get-Content $errorLogFileCommand -Tail 80 -Wait"
        Write-Info "To stop: Stop-Process -Id (Get-Content $pidFileCommand); Remove-Item $pidFileCommand"
    }
    else {
        Write-Warning "OpenTelemetry Collector is not running"
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

function Install-Collector {
    $arch = Get-Architecture
    $archSuffix = if ($arch -eq "ARM64") { "arm64" } else { "amd64" }
    $expectedHash = if ($arch -eq "ARM64") {
        "5dbbd3dd0344f759f41ab3557604d73ab720a17827375c346af6d4c2234ce776"
    }
    else {
        "f1468356aee226c4bf8bb846d260e3bada0121ef7da31db2e2e023f8207b7b9e"
    }

    if (Test-Path $COLLECTOR_EXE) {
        $installedVersion = & $COLLECTOR_EXE --version 2>$null | Select-Object -First 1
        if ($installedVersion -match [regex]::Escape($COLLECTOR_VERSION)) {
            return
        }
    }

    $archiveName = "otelcol_${COLLECTOR_VERSION}_windows_${archSuffix}.tar.gz"
    $downloadUrl = "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${COLLECTOR_VERSION}/${archiveName}"
    $tempDir = Join-Path $env:TEMP ("parseable-otelcol-" + [guid]::NewGuid().ToString("N"))
    $archivePath = Join-Path $tempDir $archiveName

    try {
        Write-Info "Installing OpenTelemetry Collector v$COLLECTOR_VERSION..."
        New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
        Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath

        $actualHash = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash
        if (-not [System.StringComparer]::OrdinalIgnoreCase.Equals($actualHash, $expectedHash)) {
            throw "OpenTelemetry Collector checksum verification failed"
        }

        $tarCommand = Get-Command tar.exe -ErrorAction SilentlyContinue
        if ($null -eq $tarCommand) {
            throw "tar.exe is required to extract OpenTelemetry Collector"
        }

        & $tarCommand.Source -xzf $archivePath -C $tempDir
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to extract OpenTelemetry Collector archive"
        }

        $extractedExe = Get-ChildItem -Path $tempDir -Filter "otelcol.exe" -Recurse | Select-Object -First 1
        if ($null -eq $extractedExe) {
            throw "OpenTelemetry Collector executable not found in downloaded archive"
        }

        if (Test-CollectorRunning) {
            Stop-Collector
        }

        if (-not (Test-Path $INSTALL_DIR)) {
            New-Item -ItemType Directory -Path $INSTALL_DIR -Force | Out-Null
        }
        Copy-Item -Path $extractedExe.FullName -Destination $COLLECTOR_EXE -Force
    }
    catch {
        Write-ErrorMsg "Failed to install OpenTelemetry Collector: $_"
        exit 1
    }
    finally {
        Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Start-Collector {
    if (Test-CollectorRunning) {
        $processId = Get-Content $PID_FILE
        Write-Warning "OpenTelemetry Collector is already running (PID: $processId)"
        return $false
    }

    if (-not (Test-Path $CONFIG_FILE)) {
        Write-ErrorMsg "Configuration file not found: $CONFIG_FILE"
        Write-ErrorMsg "Please run setup first"
        exit 1
    }

    Install-Collector

    & $COLLECTOR_EXE validate --config $CONFIG_FILE *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-ErrorMsg "OpenTelemetry Collector configuration validation failed"
        exit 1
    }

    Remove-Item $LOG_FILE, $ERROR_LOG_FILE -ErrorAction SilentlyContinue

    $process = Start-Process -FilePath $COLLECTOR_EXE `
        -ArgumentList "--config", "`"$CONFIG_FILE`"" `
        -WorkingDirectory $INSTALL_DIR `
        -WindowStyle Hidden `
        -RedirectStandardOutput $LOG_FILE `
        -RedirectStandardError $ERROR_LOG_FILE `
        -PassThru

    $process.Id | Out-File -FilePath $PID_FILE -Force
    Write-Info "OpenTelemetry Collector started with PID: $($process.Id)"
    Start-Sleep -Seconds 3

    $stillRunning = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if (-not $stillRunning) {
        Write-ErrorMsg "OpenTelemetry Collector exited immediately"
        $errorLogFileCommand = ConvertTo-PowerShellSingleQuoted $ERROR_LOG_FILE
        Write-ErrorMsg "Check logs: Get-Content $errorLogFileCommand -Tail 80"
        Remove-Item $PID_FILE -ErrorAction SilentlyContinue
        exit 1
    }

    $pidFileCommand = ConvertTo-PowerShellSingleQuoted $PID_FILE
    $errorLogFileCommand = ConvertTo-PowerShellSingleQuoted $ERROR_LOG_FILE
    Write-Info "OpenTelemetry Collector started successfully (PID: $($process.Id))"
    Write-Info "To check status: Get-Process -Id (Get-Content $pidFileCommand)"
    Write-Info "To see logs: Get-Content $errorLogFileCommand -Tail 80 -Wait"
    Write-Info "To stop: Stop-Process -Id (Get-Content $pidFileCommand); Remove-Item $pidFileCommand"
    return $true
}

function Restart-Collector {
    Stop-Collector
    Start-Sleep -Seconds 2
    [void](Start-Collector)
}

function ConvertTo-YamlSingleQuoted {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Setup-Collector {
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

    Write-ParseableBanner

    $ingestorScheme = "https"
    $defaultPort = "443"
    if ($IngestorHost -like "https://*") {
        $IngestorHost = $IngestorHost.Substring("https://".Length)
        $ingestorScheme = "https"
        $defaultPort = "443"
    }
    elseif ($IngestorHost -like "http://*") {
        $IngestorHost = $IngestorHost.Substring("http://".Length)
        $ingestorScheme = "http"
        $defaultPort = "80"
    }
    $IngestorHost = ($IngestorHost -split '/', 2)[0]

    if ($IngestorHost -match '^(\[[^]]+\])(?::([0-9]+))?$') {
        $IngestorHost = $Matches[1]
        $Port = if ([string]::IsNullOrWhiteSpace($Matches[2])) { $defaultPort } else { $Matches[2] }
    }
    elseif (($IngestorHost -split ':').Count -gt 2) {
        Write-ErrorMsg "IPv6 hosts must be enclosed in brackets"
        exit 1
    }
    elseif ($IngestorHost -match '^(.*):([0-9]+)$') {
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

    Install-Collector

    $endpoint = ConvertTo-YamlSingleQuoted "${ingestorScheme}://${IngestorHost}:${Port}"
    $apiKeyValue = ConvertTo-YamlSingleQuoted $ApiKey
    $streamNameValue = ConvertTo-YamlSingleQuoted $StreamName
    $hostNameValue = ConvertTo-YamlSingleQuoted $env:COMPUTERNAME
    $configLines = @(
        "receivers:",
        "  host_metrics:",
        "    collection_interval: 2s",
        "    scrapers:",
        "      cpu:",
        "      disk:",
        "      filesystem:",
        "      load:",
        "      memory:",
        "      network:",
        "      paging:",
        "      system:",
        "",
        "processors:",
        "  resource:",
        "    attributes:",
        "      - key: host.name",
        "        value: $hostNameValue",
        "        action: upsert",
        "  batch:",
        "    timeout: 1s",
        "",
        "exporters:",
        "  otlp_http/parseable:",
        "    endpoint: $endpoint",
        "    encoding: json",
        "    compression: none",
        "    headers:",
        "      X-API-Key: $apiKeyValue",
        "      X-P-Stream: $streamNameValue"
    )

    if (-not [string]::IsNullOrWhiteSpace($TenantId)) {
        $tenantIdValue = ConvertTo-YamlSingleQuoted $TenantId
        $configLines += "      X-P-Tenant: $tenantIdValue"
    }

    $configLines += @(
        "",
        "service:",
        "  telemetry:",
        "    metrics:",
        "      level: none",
        "  pipelines:",
        "    metrics:",
        "      receivers: [host_metrics]",
        "      processors: [resource, batch]",
        "      exporters: [otlp_http/parseable]"
    )

    $configContent = ($configLines -join [Environment]::NewLine) + [Environment]::NewLine
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    $tempConfigFile = "$CONFIG_FILE.$([guid]::NewGuid().ToString('N')).tmp"

    try {
        $tempConfigStream = [System.IO.File]::Create($tempConfigFile)
        $tempConfigStream.Dispose()

        $currentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
        $fileRights = [System.Security.AccessControl.FileSystemRights]::Read -bor `
            [System.Security.AccessControl.FileSystemRights]::Write
        $accessRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $currentIdentity.User,
            $fileRights,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        $acl = [System.Security.AccessControl.FileSecurity]::new()
        $acl.SetOwner($currentIdentity.User)
        $acl.SetAccessRuleProtection($true, $false)
        $acl.AddAccessRule($accessRule)
        Set-Acl -Path $tempConfigFile -AclObject $acl -ErrorAction Stop

        [System.IO.File]::WriteAllText($tempConfigFile, $configContent, $utf8NoBom)

        & $COLLECTOR_EXE validate --config $tempConfigFile *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-ErrorMsg "OpenTelemetry Collector configuration validation failed"
            exit 1
        }

        Move-Item -LiteralPath $tempConfigFile -Destination $CONFIG_FILE -Force -ErrorAction Stop
    }
    catch {
        Write-ErrorMsg "Failed to update OpenTelemetry Collector configuration: $_"
        exit 1
    }
    finally {
        Remove-Item $tempConfigFile -Force -ErrorAction SilentlyContinue
    }

    if (Test-CollectorRunning) {
        Write-Info "Restarting OpenTelemetry Collector to apply updated configuration..."
        Stop-Collector
        Start-Sleep -Seconds 2
    }

    Write-Host ""
    if (Start-Collector) {
        Write-SetupComplete -StreamName $StreamName
    }
}

function Show-Help {
    Write-Host @"
OpenTelemetry Collector Host Metrics Setup and Management Script for Windows

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

function Debug-Collector {
    if (-not (Test-Path $CONFIG_FILE)) {
        Write-ErrorMsg "Configuration file not found: $CONFIG_FILE"
        exit 1
    }

    Install-Collector
    Write-Info "Config: $CONFIG_FILE"
    Write-Host ""
    & $COLLECTOR_EXE --config $CONFIG_FILE
}

if ([string]::IsNullOrWhiteSpace($Param1)) {
    Show-Help
    exit 0
}

switch ($Param1.ToLower()) {
    "stop" {
        Stop-Collector
    }
    "restart" {
        Restart-Collector
    }
    "start" {
        [void](Start-Collector)
    }
    "status" {
        Show-Status
    }
    "logs" {
        Show-Logs
    }
    "debug" {
        Debug-Collector
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
        Setup-Collector -IngestorHost $Param1 -StreamName $Param2 -ApiKey $Param3 -TenantId $Param4
    }
}
