#!/usr/bin/env bash
# Build the CLI client with attestation keys and publish (or only emit) the allowlist row.
#
# Usage:
#   export MTRXAI_ADMIN_KEY=...
#   scripts/publish-build-attestation.sh              # build + POST to lobby
#   scripts/publish-build-attestation.sh --manifest-only   # build + write JSON/SQL only
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOBBY_URL="${MTRXAI_LOBBY_URL:-http://127.0.0.1:8080}"
MANIFEST_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --manifest-only) MANIFEST_ONLY=1; shift ;;
        -h|--help)
            echo "Usage: $(basename "$0") [--manifest-only]"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

export MTRXAI_BUILD_ID="${MTRXAI_BUILD_ID:-$(uuidgen 2>/dev/null || python -c 'import uuid; print(uuid.uuid4())')}"
export MTRXAI_ATTESTATION_SECRET="${MTRXAI_ATTESTATION_SECRET:-$(openssl rand -hex 32)}"

echo "Building client with build_id=${MTRXAI_BUILD_ID}"

cd "${ROOT_DIR}"
cargo build --release -p peer

BINARY_PATH="${ROOT_DIR}/target/release/peer"
bash "${SCRIPT_DIR}/write-allowed-build-manifest.sh" "${BINARY_PATH}"

if [[ "${MANIFEST_ONLY}" -eq 1 ]]; then
    echo "Manifest only - skipping POST /api/admin/builds"
    exit 0
fi

ADMIN_KEY="${MTRXAI_ADMIN_KEY:?set MTRXAI_ADMIN_KEY}"
MANIFEST="${ROOT_DIR}/target/release/allowed_build.json"

if command -v jq >/dev/null 2>&1; then
    BODY="$(jq '{build_id, binary_sha256, public_key, version, git_sha, platform}' "${MANIFEST}")"
else
    BODY="$(python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps({k:d[k] for k in ("build_id","binary_sha256","public_key","version","git_sha","platform")}))' "${MANIFEST}")"
fi

curl -fsS -X POST "${LOBBY_URL}/api/admin/builds" \
  -H "Content-Type: application/json" \
  -H "x-admin-key: ${ADMIN_KEY}" \
  -d "${BODY}"

echo "Published allowed build ${MTRXAI_BUILD_ID}"
