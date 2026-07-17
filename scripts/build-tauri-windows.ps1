# Build mtrxai desktop (Tauri) release installers on Windows.
#
# Usage (from repo root):
#   scripts\build-tauri-windows.cmd
#
# Or, if PowerShell script execution is allowed:
#   .\scripts\build-tauri-windows.ps1
#
# Or, without changing execution policy:
#   powershell -ExecutionPolicy Bypass -File .\scripts\build-tauri-windows.ps1
#
# Usage (attested release build + DB insert manifest):
#   .\scripts\build-tauri-windows.ps1 -Attestation
#
# Or pre-set credentials:
#   $env:MTRXAI_BUILD_ID = "<uuid>"
#   $env:MTRXAI_ATTESTATION_SECRET = "<32-byte-hex>"
#   .\scripts\build-tauri-windows.ps1 -Attestation
#
# Output:
#   release\windows\release\mtrxai.exe
#   release\windows\release\allowed_build.json   (when -Attestation)
#   release\windows\release\bundle\msi\*.msi
#   release\windows\release\bundle\nsis\*-setup.exe

#Requires -Version 5.1

param(
    [switch]$SkipNpmInstall,
    [switch]$Attestation
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$TauriDir = Join-Path $RootDir "desktop"
$OsReleaseDir = Join-Path $RootDir "release\windows"

function Write-Step($Message) {
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Ensure-Command($Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found on PATH: $Name"
    }
}

function Stop-RunningDesktopApp {
    $processes = @(Get-Process -Name "mtrxai" -ErrorAction SilentlyContinue)

    if ($processes.Length -eq 0) {
        return
    }

    Write-Host "Stopping $($processes.Length) running mtrxai desktop process(es) before build..."
    foreach ($process in $processes) {
        try {
            Stop-Process -Id $process.Id -Force -ErrorAction Stop
        } catch {
            throw "Could not stop $($process.ProcessName) (PID $($process.Id)). Close mtrxai manually and retry."
        }
    }

    Start-Sleep -Seconds 1
}

function New-AttestationBuildId {
    return [guid]::NewGuid().ToString()
}

function New-AttestationSecretHex {
    return -join ((1..32) | ForEach-Object { '{0:x2}' -f (Get-Random -Maximum 256) })
}

function Ensure-AttestationBuildEnv {
    param([switch]$Enabled)

    if (-not $Enabled) {
        if (-not $env:MTRXAI_ATTESTATION_SKIP) {
            $env:MTRXAI_ATTESTATION_SKIP = "1"
            Write-Host "Set MTRXAI_ATTESTATION_SKIP=1 (pass -Attestation for release builds)"
        }
        return
    }

    $env:MTRXAI_ATTESTATION_SKIP = "0"

    if (-not $env:MTRXAI_BUILD_ID -or -not $env:MTRXAI_BUILD_ID.Trim()) {
        $env:MTRXAI_BUILD_ID = New-AttestationBuildId
        Write-Host "Generated MTRXAI_BUILD_ID=$($env:MTRXAI_BUILD_ID)"
    }

    if (-not $env:MTRXAI_ATTESTATION_SECRET -or -not $env:MTRXAI_ATTESTATION_SECRET.Trim()) {
        $env:MTRXAI_ATTESTATION_SECRET = New-AttestationSecretHex
        Write-Host "Generated MTRXAI_ATTESTATION_SECRET (store securely - required to rebuild this attested binary)"
    }
}

function Ensure-ReleaseExeUnlocked {
    param([string]$ExePath)

    if (-not (Test-Path $ExePath)) {
        return
    }

    try {
        $stream = [System.IO.File]::Open(
            $ExePath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        $stream.Close()
    } catch {
        throw @"
Cannot overwrite locked executable:
  $ExePath

Close all mtrxai desktop windows, end any mtrxai.exe process in Task Manager, then rerun:
  .\scripts\build-tauri-windows.cmd
"@
    }
}

Write-Step "Checking prerequisites"
Ensure-Command "node"
Ensure-Command "npm"
Ensure-Command "cargo"

if (-not (Test-Path $TauriDir)) {
    throw "Tauri project directory not found: $TauriDir"
}

# Keep desktop build artifacts separate from the shared workspace target/ directory.
$env:CARGO_TARGET_DIR = $OsReleaseDir
Write-Host "CARGO_TARGET_DIR=$($env:CARGO_TARGET_DIR)"

Ensure-AttestationBuildEnv -Enabled:$Attestation

Push-Location $TauriDir
try {
    if (-not $SkipNpmInstall) {
        Write-Step "Installing npm dependencies"
        npm install
        if ($LASTEXITCODE -ne 0) {
            throw "npm install failed with exit code $LASTEXITCODE"
        }
    }

    Write-Step "Building Tauri release bundles"
    Stop-RunningDesktopApp
    Ensure-ReleaseExeUnlocked -ExePath (Join-Path $OsReleaseDir "release\mtrxai.exe")
    npm run build
    if ($LASTEXITCODE -ne 0) {
        throw "npm run build failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}

$ReleaseDir = Join-Path $OsReleaseDir "release"
$BundleDir = Join-Path $ReleaseDir "bundle"
$ExePath = Join-Path $ReleaseDir "mtrxai.exe"

Write-Step "Build complete"
Write-Host "Release root: $OsReleaseDir"
Write-Host "Executable: $ExePath"
if (Test-Path $BundleDir) {
    Write-Host "Bundles:"
    Get-ChildItem -Path $BundleDir -Recurse -File | ForEach-Object {
        Write-Host "  $($_.FullName)"
    }
} else {
    Write-Warning "Bundle directory not found: $BundleDir"
}

if ($Attestation -and (Test-Path $ExePath)) {
    Write-Step "Writing allowed_build manifest for DB insert"
    $ManifestScript = Join-Path $PSScriptRoot "write-allowed-build-manifest.ps1"
    & $ManifestScript -BinaryPath $ExePath -RootDir $RootDir

    $CredentialsPath = Join-Path $ReleaseDir "allowed_build.credentials.env"
    @(
        "MTRXAI_BUILD_ID=$($env:MTRXAI_BUILD_ID)"
        "MTRXAI_ATTESTATION_SECRET=$($env:MTRXAI_ATTESTATION_SECRET)"
        "MTRXAI_ATTESTATION_SKIP=0"
    ) | Set-Content -Path $CredentialsPath -Encoding UTF8
    Write-Host "Saved rebuild credentials: $CredentialsPath"
}
