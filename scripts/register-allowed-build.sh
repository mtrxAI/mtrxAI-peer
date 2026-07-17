#!/usr/bin/env bash
# Register an allowed_build.json manifest on a lobby (POST /api/admin/builds).
#
# Usage:
#   export MTRXAI_LOBBY_URL=https://127.0.0.1:8082   # admin UI / API base URL
#   export MTRXAI_ADMIN_KEY=...
#   scripts/register-allowed-build.sh [path/to/allowed_build.json]
#   scripts/register-allowed-build.sh --check-only [path/to/allowed_build.json]
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK_ONLY=0
MANIFEST="${ROOT_DIR}/release/docker/allowed_build.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only) CHECK_ONLY=1; shift ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--check-only] [allowed_build.json]"
      exit 0
      ;;
    *) MANIFEST="$1"; shift ;;
  esac
done

LOBBY_URL="${MTRXAI_LOBBY_URL:?set MTRXAI_LOBBY_URL to your lobby admin base URL (e.g. https://127.0.0.1:8082)}"
ADMIN_KEY="${MTRXAI_ADMIN_KEY:?set MTRXAI_ADMIN_KEY (must match the lobby server MTRXAI_ADMIN_KEY)}"

if [[ ! -f "${MANIFEST}" ]]; then
  echo "Manifest not found: ${MANIFEST}" >&2
  echo "Build first: scripts/build-client-docker-attestation.sh mtrxai-client:attested" >&2
  exit 1
fi

if command -v jq >/dev/null 2>&1; then
  BODY="$(jq '{build_id, binary_sha256, public_key, version, git_sha, platform}' "${MANIFEST}")"
  BUILD_ID="$(jq -r .build_id "${MANIFEST}")"
  BINARY_SHA256="$(jq -r .binary_sha256 "${MANIFEST}")"
else
  BODY="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps({k:d[k] for k in ("build_id","binary_sha256","public_key","version","git_sha","platform")}))' "${MANIFEST}")"
  BUILD_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["build_id"])' "${MANIFEST}")"
  BINARY_SHA256="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["binary_sha256"])' "${MANIFEST}")"
fi

check_allowlisted() {
  local list
  list="$(curl -fsS "${LOBBY_URL}/api/admin/builds" \
    -H "x-admin-key: ${ADMIN_KEY}")"
  if command -v jq >/dev/null 2>&1; then
    echo "${list}" | jq -e --arg id "${BUILD_ID}" '.[] | select(.build_id == $id and .status == "allowed")' >/dev/null
  else
    echo "${list}" | grep -q "${BUILD_ID}"
  fi
}

if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  echo "==> Checking allowlist for build ${BUILD_ID} on ${LOBBY_URL}"
  if check_allowlisted; then
    echo "OK: build ${BUILD_ID} is on the allowlist"
    exit 0
  fi
  echo "MISSING: build ${BUILD_ID} is NOT on the allowlist" >&2
  echo "Run: MTRXAI_ADMIN_KEY=... scripts/register-allowed-build.sh ${MANIFEST}" >&2
  exit 1
fi

echo "==> Registering build on ${LOBBY_URL}"
echo "    build_id:      ${BUILD_ID}"
echo "    binary_sha256: ${BINARY_SHA256}"

RESP="$(curl -fsS -X POST "${LOBBY_URL}/api/admin/builds" \
  -H "Content-Type: application/json" \
  -H "x-admin-key: ${ADMIN_KEY}" \
  -d "${BODY}")"
echo "${RESP}"

if check_allowlisted; then
  echo "Verified: build ${BUILD_ID} is on the allowlist"
else
  echo "Warning: POST succeeded but build ${BUILD_ID} not found on GET /api/admin/builds" >&2
  exit 1
fi
