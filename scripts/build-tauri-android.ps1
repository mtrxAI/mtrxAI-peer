# Build the mtrxAI Android APK (Tauri + in-process peer).
#
# Usage:
#   .\scripts\build-tauri-android.ps1 [-Attestation] [-Debug] [-Target aarch64|x86_64|universal]

param(
    [switch]$Attestation,
    [switch]$DebugBuild,
    [ValidateSet("aarch64", "x86_64", "universal")]
    [string]$Target = "aarch64"
)

$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
$TauriDir = Join-Path $RootDir "desktop"
$ReleaseDir = Join-Path $RootDir "release\android"
$ProfileDir = Join-Path $ReleaseDir "release"
New-Item -ItemType Directory -Force -Path $ProfileDir | Out-Null

if (-not $env:ANDROID_HOME -and -not $env:ANDROID_SDK_ROOT) {
    throw "ANDROID_HOME (or ANDROID_SDK_ROOT) is required"
}

$env:CARGO_TARGET_DIR = $ReleaseDir

if ($Attestation) {
    $env:MTRXAI_ATTESTATION_SKIP = "0"
    if (-not $env:MTRXAI_BUILD_ID) { $env:MTRXAI_BUILD_ID = [guid]::NewGuid().ToString() }
    if (-not $env:MTRXAI_ATTESTATION_SECRET) {
        $env:MTRXAI_ATTESTATION_SECRET = -join ((1..32) | ForEach-Object { "{0:x2}" -f (Get-Random -Max 256) })
    }
    if (-not $env:MTRXAI_BUILD_PLATFORM) { $env:MTRXAI_BUILD_PLATFORM = "android/aarch64" }
    @"
MTRXAI_BUILD_ID=$($env:MTRXAI_BUILD_ID)
MTRXAI_ATTESTATION_SECRET=$($env:MTRXAI_ATTESTATION_SECRET)
MTRXAI_ATTESTATION_SKIP=0
MTRXAI_BUILD_PLATFORM=$($env:MTRXAI_BUILD_PLATFORM)
"@ | Set-Content (Join-Path $ProfileDir "allowed_build.credentials.env")
    Write-Host "MTRXAI_BUILD_ID=$($env:MTRXAI_BUILD_ID)"
} else {
    if (-not $env:MTRXAI_ATTESTATION_SKIP) { $env:MTRXAI_ATTESTATION_SKIP = "1" }
}

Push-Location $TauriDir
try {
    if (-not (Test-Path "node_modules")) { npm install }
    if (-not (Test-Path "src-tauri\gen\android")) {
        npx tauri android init --ci --skip-targets-install
    }

    $tauriArgs = @("android", "build", "--target", $Target)
    if ($DebugBuild) { $tauriArgs += "--debug" }
    Write-Host "==> Building Android ($Target)$(if ($DebugBuild) { ' debug' })"
    & npx tauri @tauriArgs

    $apkRoot = "src-tauri\gen\android\app\build\outputs\apk"
    $apk = Get-ChildItem -Path $apkRoot -Recurse -Filter "*.apk" |
        Where-Object { if ($DebugBuild) { $_.FullName -match "debug" } else { $_.FullName -match "release" } } |
        Select-Object -First 1
    if (-not $apk) {
        $apk = Get-ChildItem -Path $apkRoot -Recurse -Filter "*.apk" | Select-Object -First 1
    }
    if (-not $apk) { throw "APK not found under $apkRoot" }

    $apkDest = Join-Path $ProfileDir "mtrxai-android-$Target.apk"
    Copy-Item $apk.FullName $apkDest -Force
    Write-Host "APK: $apkDest"

    $so = Get-ChildItem -Path "src-tauri\gen\android\app\src\main\jniLibs" -Recurse -Filter "libmtrxai_tauri.so" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if (-not $so) {
        $so = Get-ChildItem -Path $ReleaseDir -Recurse -Filter "libmtrxai_tauri.so" -ErrorAction SilentlyContinue |
            Select-Object -First 1
    }
    if ($so) {
        Copy-Item $so.FullName (Join-Path $ProfileDir "libmtrxai_tauri.so") -Force
        Write-Host "Native lib: $(Join-Path $ProfileDir 'libmtrxai_tauri.so')"
    }

    if ($Attestation) {
        $soPath = Join-Path $ProfileDir "libmtrxai_tauri.so"
        if (-not (Test-Path $soPath)) { throw "Cannot attest: libmtrxai_tauri.so missing" }
        & (Join-Path $PSScriptRoot "write-allowed-build-manifest.ps1") -BinaryPath $soPath
    }
} finally {
    Pop-Location
}

Write-Host "==> Android build complete"
Get-ChildItem $ProfileDir | Format-Table Name, Length
