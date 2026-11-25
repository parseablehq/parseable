#!/usr/bin/env pwsh

# Installs Parseable Enterprise for Windows x86_64.
[System.String[]]$SUPPORTED_ARCH = @("x86_64", "amd64")

# PowerShell's progress bar significantly slows Invoke-WebRequest.
$ProgressPreference = 'SilentlyContinue'

function Get-Env {
    param([String] $Key)

    [Microsoft.Win32.RegistryKey]$RegisterKey = Get-Item -Path 'HKCU:'
    [Microsoft.Win32.RegistryKey]$EnvRegisterKey = $RegisterKey.OpenSubKey('Environment')
    $EnvRegisterKey.GetValue($Key, $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
}

function Publish-Env {
    if (-not ("Win32.NativeMethods" -as [Type])) {
        Add-Type -Namespace Win32 -Name NativeMethods -MemberDefinition @"
[DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
public static extern IntPtr SendMessageTimeout(
    IntPtr hWnd, uint Msg, UIntPtr wParam, string lParam,
    uint fuFlags, uint uTimeout, out UIntPtr lpdwResult);
"@
    }
    $HWND_BROADCAST = [IntPtr] 0xffff
    $WM_SETTINGCHANGE = 0x1a
    $result = [UIntPtr]::Zero
    [Win32.NativeMethods]::SendMessageTimeout(
        $HWND_BROADCAST,
        $WM_SETTINGCHANGE,
        [UIntPtr]::Zero,
        "Environment",
        2,
        5000,
        [ref] $result
    ) | Out-Null
}

function Write-Env {
    param([String]$Key, [String]$Value)

    [Microsoft.Win32.RegistryKey]$RegisterKey = Get-Item -Path 'HKCU:'
    [Microsoft.Win32.RegistryKey]$EnvRegisterKey = $RegisterKey.OpenSubKey('Environment', $true)
    if ($null -eq $Value) {
        $EnvRegisterKey.DeleteValue($Key)
    }
    else {
        $RegistryValueKind = if ($Value.Contains('%')) {
            [Microsoft.Win32.RegistryValueKind]::ExpandString
        }
        elseif ($EnvRegisterKey.GetValue($Key)) {
            $EnvRegisterKey.GetValueKind($Key)
        }
        else {
            [Microsoft.Win32.RegistryValueKind]::String
        }
        $EnvRegisterKey.SetValue($Key, $Value, $RegistryValueKind)
    }

    Publish-Env
}

[String]$CPU_ARCH = "x86_64"
[String]$OS = "windows"
[String]$INSTALLDIR = "${HOME}\.parseable\bin"
[String]$BIN = "${INSTALLDIR}\parseable.exe"
[String]$DOWNLOAD_BASE_URL = "https://cdn.parseable.com/parseablepublicrelease/parseable-enterprise"
[String]$PARSEABLE_FILENAME = "Parseable_Enterprise_x86_64-pc-windows-msvc.exe"

function Install-Parseable {
    Write-Output "`n=========================`n"
    Write-Output "OS: $OS"
    Write-Output "CPU arch: $CPU_ARCH"

    if ($SUPPORTED_ARCH -notcontains $CPU_ARCH) {
        Write-Error "Unsupported CPU architecture ($CPU_ARCH)."
        exit 1
    }

    Write-Output "Checking for existing installation..."
    if (Test-Path $BIN) {
        Write-Error "Parseable is already installed. Run 'parseable --version' to check the version."
        Write-Error "Remove the existing installation before installing a new version."
        exit 1
    }

    Write-Output "Fetching latest release..."
    try {
        # Enterprise releases use the latest Parseable OSS release tag as their version.
        $release = Invoke-RestMethod -Uri "https://api.github.com/repos/parseablehq/parseable/releases/latest"
    }
    catch {
        Write-Error "Could not fetch the latest release information: $_"
        exit 1
    }

    [String]$release_tag = $release.tag_name
    if ([String]::IsNullOrWhiteSpace($release_tag)) {
        Write-Error "Could not determine the latest release version."
        exit 1
    }

    [String]$download_url = "${DOWNLOAD_BASE_URL}/${release_tag}/${PARSEABLE_FILENAME}"
    Write-Output "Latest Parseable version: $release_tag"
    Write-Output "Downloading Parseable Enterprise version $release_tag, for OS: $OS, CPU architecture: $CPU_ARCH"
    Write-Output "Download URL: $download_url`n"

    New-Item -ItemType Directory -Force -Path $INSTALLDIR | Out-Null
    try {
        Invoke-WebRequest -Uri $download_url -OutFile $BIN
    }
    catch {
        Remove-Item -Force -ErrorAction SilentlyContinue $BIN
        Write-Error "Download failed: $_"
        exit 1
    }

    Write-Output "Adding Parseable to PATH..."
    $Path = (Get-Env -Key "Path") -split ';'
    if ($Path -notcontains $INSTALLDIR) {
        $Path += $INSTALLDIR
        Write-Env -Key 'Path' -Value ($Path -join ';')
        $env:PATH = $Path -join ';'
    }
}

Install-Parseable

Write-Output "Parseable Enterprise was downloaded successfully at $INSTALLDIR"
Write-Output "Restart your terminal/editor, then type `"parseable`"`n"
