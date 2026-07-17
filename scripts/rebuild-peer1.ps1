# Rebuild and restart peer1 with the latest mtrxAI client (includes num_ctx fix).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Building peerai-peer1 image..."
docker compose build --no-cache peerai-peer1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Restarting ollama-1 (OLLAMA_CONTEXT_LENGTH) and peer1..."
docker compose up -d ollama-1 peerai-peer1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Done. peer1 proxy: http://127.0.0.1:11346"
Write-Host "Verify num_ctx injection: docker logs peer1 --tail 30 | Select-String mtrxAI-debug"
