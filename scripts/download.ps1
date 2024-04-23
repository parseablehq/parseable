#!/usr/bin/env pwsh

<# only supports windows,
# `SUPPORTED_OS` array contains the supported linux and darwin for filtering only
# `SUPPORTED_ARCH` array contains the supported architectures but only x86_64/amd64 are
#  available for now. both amd64 and x86_64 are there cause windows does report either
#  of them when querying for the architecture.
#>
[System.String[]]$SUPPORTED_ARCH = @("x86_64", "arm64", "amd64")
[System.String]$DOWNLOAD_BASE_URL="cdn.parseable.com/"

#  util functions
function Get-Env {
    param([String] $Key)

    $RegisterKey = Get-Item -Path 'HKCU:'
    $EnvRegisterKey = $RegisterKey.OpenSubKey('Environment')
    $EnvRegisterKey.GetValue($Key, $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
}

# These three environment functions are roughly copied from https://github.com/prefix-dev/pixi/pull/692
# They are used instead of `SetEnvironmentVariable` because of unwanted variable expansions.
function Publish-Env {
    if (-not ("Win32.NativeMethods" -as [Type])) {
        <# dllimport should not be needed but still#>
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
    [Win32.NativeMethods]::SendMessageTimeout($HWND_BROADCAST,
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

function Get-Env {
    param([String] $Key)

    [Microsoft.Win32.RegistryKey]$RegisterKey = Get-Item -Path 'HKCU:'
    [Microsoft.Win32.RegistryKey]$EnvRegisterKey = $RegisterKey.OpenSubKey('Environment')
    $EnvRegisterKey.GetValue($Key, $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
}

# Get the system's CPU architecture and operating system
[String]$CPU_ARCH = "x86_64"
[String]$OS = "windows"
[String]$INSTALLDIR = "${HOME}\.parseable\bin"
[String]$BIN = "${INSTALLDIR}\parseable.exe"

function Install-Parseable {
    Write-Output "`n=========================`n"
    Write-Output "OS: $OS"
    Write-Output "CPU arch: $CPU_ARCH"

    # Check if the CPU architecture is supported
    if ($SUPPORTED_ARCH -notcontains $CPU_ARCH) {
        Write-Error "Unsupported CPU architecture ($CPU_ARCH)."
        exit 1
    }

    Write-Output "Checking for existing installation..."
    if (Test-Path $BIN) {
        Write-Error "Parseable is already installed. Run 'parseable --version' to check the version."
        Write-Error "Consider removing the existing installation"
        exit 1
    }

    Write-Output "No existing installation found"

    Write-Output "Fetching latest release..."
    # Get the latest release information using GitHub API
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/parseablehq/parseable/releases/latest"
    # Loop through binaries in the release and find the appropriate one
    $download_url = $DOWNLOAD_BASE_URL + $CPU_ARCH + "-" + $OS + "." + $release.tag_name

    mkdir -Force $INSTALLDIR

    Write-Output "Downloading Parseable version $release_tag, for OS: $OS, CPU architecture: $CPU_ARCH`n`n"
    # Download the binary using Invoke-WebRequest
    Invoke-WebRequest -Uri $download_url -OutFile $BIN

    Write-Output "Adding Parseable to PATH..."
    # Only try adding to path if there isn't already a bun.exe in the path
    $Path = (Get-Env -Key "Path") -split ';'
    if ($Path -notcontains $INSTALLDIR) {
        $Path += $INSTALLDIR
        Write-Env -Key 'Path' -Value ($Path -join ';')
        $env:PATH = $Path;
    }
}

Install-Parseable

Write-Output "Parseable was downloaded successfully! at $INSTALLDIR"
Write-Output "To get started, restart your terminal/editor, then type `"parseable`"`n"
