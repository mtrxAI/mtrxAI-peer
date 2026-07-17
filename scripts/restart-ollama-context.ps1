# Quick fix: restart Ollama with OLLAMA_CONTEXT_LENGTH=16384 (no peer rebuild required).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Recreating ollama-1 with OLLAMA_CONTEXT_LENGTH=16384..."
docker compose up -d ollama-1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Waiting for ollama-1..."
Start-Sleep -Seconds 5

Write-Host "Run .\debug-logs\verify-numctx.ps1 to confirm peer1 eval_count > 1"
