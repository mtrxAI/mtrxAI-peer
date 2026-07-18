# Write allowed_builds row data for PostgreSQL / admin API after a release build.
#
# Usage:
#   .\scripts\write-allowed-build-manifest.ps1 -BinaryPath release\windows\release\mtrxai.exe
#
# Requires MTRXAI_ATTESTATION_SECRET (and ideally MTRXAI_BUILD_ID) from the build env.
# Optional: MTRXAI_BUILD_PLATFORM (default: windows/x86_64 or windows/aarch64 from PROCESSOR_ARCHITECTURE)

#Requires -Version 5.1

param(
    [Parameter(Mandatory = $true)]
    [string]$BinaryPath,

    [string]$OutputPath = "",

    [string]$BuildId = "",

    [string]$AttestationSecret = "",

    [string]$Platform = "",

    [string]$RootDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $RootDir) {
    $RootDir = Split-Path -Parent $PSScriptRoot
}

$BinaryPath = (Resolve-Path -LiteralPath $BinaryPath).Path

if (-not $BuildId) {
    $BuildId = $env:MTRXAI_BUILD_ID
}
if (-not $AttestationSecret) {
    $AttestationSecret = $env:MTRXAI_ATTESTATION_SECRET
}
if (-not $Platform) {
    if ($env:MTRXAI_BUILD_PLATFORM) {
        $Platform = $env:MTRXAI_BUILD_PLATFORM
    } else {
        $Arch = $env:PROCESSOR_ARCHITECTURE
        $Platform = if ($Arch -eq "ARM64") { "windows/aarch64" } else { "windows/x86_64" }
    }
}

if (-not $BuildId -or -not $BuildId.Trim()) {
    throw "MTRXAI_BUILD_ID is required (set env or pass -BuildId)."
}
if (-not $AttestationSecret -or -not $AttestationSecret.Trim()) {
    throw "MTRXAI_ATTESTATION_SECRET is required (set env or pass -AttestationSecret)."
}

$PeerToml = Join-Path $RootDir "peer\Cargo.toml"
if (-not (Test-Path $PeerToml)) {
    throw "peer/Cargo.toml not found under $RootDir"
}

$VersionLine = Select-String -Path $PeerToml -Pattern '^version\s*=\s*"' | Select-Object -First 1
if (-not $VersionLine) {
    throw "Could not read version from peer/Cargo.toml"
}
$Version = $VersionLine.Line -replace '^.*"([^"]+)".*$', '$1'

$GitSha = "unknown"
if (Get-Command git -ErrorAction SilentlyContinue) {
    Push-Location $RootDir
    try {
        $GitSha = (git rev-parse HEAD 2>$null)
        if (-not $GitSha) { $GitSha = "unknown" }
    } finally {
        Pop-Location
    }
}

$Sha256 = (Get-FileHash -LiteralPath $BinaryPath -Algorithm SHA256).Hash.ToLower()

$ParentDir = Split-Path $RootDir -Parent
$CommonDir = $null
foreach ($Candidate in @("mtrxAI-common", "common")) {
    $Try = Join-Path $ParentDir $Candidate
    if (Test-Path (Join-Path $Try "mtrxai-attestation")) {
        $CommonDir = $Try
        break
    }
}
if (-not $CommonDir) {
    throw "mtrxAI-common (or common) not found next to $RootDir - expected ../mtrxAI-common/mtrxai-attestation"
}
Push-Location $CommonDir
try {
    $PublicKey = (& cargo run -q -p mtrxai-attestation --bin pubkey_from_seed -- $AttestationSecret.Trim()).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $PublicKey) {
        throw "pubkey_from_seed failed"
    }
} finally {
    Pop-Location
}

$Record = [ordered]@{
    build_id      = $BuildId.Trim()
    binary_sha256 = $Sha256
    public_key    = $PublicKey
    version       = $Version
    git_sha       = $GitSha
    platform      = $Platform
    binary_path   = $BinaryPath
}

if (-not $OutputPath) {
    $OutputPath = Join-Path (Split-Path -Parent $BinaryPath) "allowed_build.json"
}

$Json = $Record | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText($OutputPath, "$Json`n", [System.Text.UTF8Encoding]::new($false))

$Sql = @"
INSERT INTO allowed_builds (build_id, binary_sha256, public_key, version, git_sha, platform)
VALUES (
  '$($Record.build_id)'::uuid,
  '$($Record.binary_sha256)',
  '$($Record.public_key)',
  '$($Record.version)',
  '$($Record.git_sha)',
  '$($Record.platform)'
)
ON CONFLICT (build_id) DO UPDATE SET
  binary_sha256 = EXCLUDED.binary_sha256,
  public_key = EXCLUDED.public_key,
  version = EXCLUDED.version,
  git_sha = EXCLUDED.git_sha,
  platform = EXCLUDED.platform,
  revoked_at = NULL;
"@

$ApiBody = @{
    build_id      = $Record.build_id
    binary_sha256 = $Record.binary_sha256
    public_key    = $Record.public_key
    version       = $Record.version
    git_sha       = $Record.git_sha
    platform      = $Record.platform
} | ConvertTo-Json -Compress

Write-Host ""
Write-Host "==> Allowed build manifest" -ForegroundColor Cyan
Write-Host "Wrote: $OutputPath"
Write-Host ""
Write-Host "build_id:      $($Record.build_id)"
Write-Host "binary_sha256: $($Record.binary_sha256)"
Write-Host "public_key:    $($Record.public_key)"
Write-Host "version:       $($Record.version)"
Write-Host "git_sha:       $($Record.git_sha)"
Write-Host "platform:      $($Record.platform)"
Write-Host ""
Write-Host "--- SQL (paste into psql) ---" -ForegroundColor Yellow
Write-Host $Sql
Write-Host ""
Write-Host "--- Admin API (PowerShell) ---" -ForegroundColor Yellow
Write-Host @"
`$body = '$ApiBody'
Invoke-RestMethod -Method POST -Uri "`$env:MTRXAI_LOBBY_URL/api/admin/builds" `
  -Headers @{ 'x-admin-key' = `$env:MTRXAI_ADMIN_KEY } `
  -ContentType 'application/json' -Body `$body
"@

return $OutputPath
