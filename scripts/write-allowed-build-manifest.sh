#!/usr/bin/env bash
# Write allowed_builds row data for PostgreSQL / admin API after a release build.
#
# Usage:
#   scripts/write-allowed-build-manifest.sh /path/to/binary
#
# Requires MTRXAI_ATTESTATION_SECRET (and ideally MTRXAI_BUILD_ID) from the build env.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY_PATH="${1:?usage: write-allowed-build-manifest.sh <binary-path>}"

if [[ ! -f "${BINARY_PATH}" ]]; then
  echo "Binary not found: ${BINARY_PATH}" >&2
  exit 1
fi

BUILD_ID="${MTRXAI_BUILD_ID:-}"
ATTESTATION_SECRET="${MTRXAI_ATTESTATION_SECRET:-}"
PLATFORM="${MTRXAI_BUILD_PLATFORM:-}"

if [[ -z "${BUILD_ID}" ]]; then
  echo "MTRXAI_BUILD_ID is required" >&2
  exit 1
fi
if [[ -z "${ATTESTATION_SECRET}" ]]; then
  echo "MTRXAI_ATTESTATION_SECRET is required" >&2
  exit 1
fi

if [[ -z "${PLATFORM}" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64|Linux-amd64) PLATFORM="linux/x86_64" ;;
    Linux-aarch64|Linux-arm64) PLATFORM="linux/aarch64" ;;
    Darwin-x86_64) PLATFORM="macos/x86_64" ;;
    Darwin-arm64) PLATFORM="macos/aarch64" ;;
    *) PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')/$(uname -m)" ;;
  esac
fi

# Peer proofs use Rust OS/ARCH (linux/x86_64), not Docker GOARCH (linux/amd64).
case "${PLATFORM}" in
  linux/amd64) PLATFORM="linux/x86_64" ;;
  linux/arm64) PLATFORM="linux/aarch64" ;;
  darwin/arm64|macos/arm64) PLATFORM="macos/aarch64" ;;
  darwin/amd64|darwin/x86_64|macos/amd64) PLATFORM="macos/x86_64" ;;
  windows/amd64) PLATFORM="windows/x86_64" ;;
  windows/arm64) PLATFORM="windows/aarch64" ;;
  android/arm64|android/aarch64) PLATFORM="android/aarch64" ;;
  android/x86_64|android/amd64) PLATFORM="android/x86_64" ;;
esac

VERSION="$(python3 "${ROOT_DIR}/scripts/sync_release_version.py" get-version 2>/dev/null || awk -F'"' '/^version = / {print $2; exit}' "${ROOT_DIR}/peer/Cargo.toml")"
GIT_SHA="$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)"

if command -v sha256sum >/dev/null 2>&1; then
  BINARY_SHA256="$(sha256sum "${BINARY_PATH}" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  BINARY_SHA256="$(shasum -a 256 "${BINARY_PATH}" | awk '{print $1}')"
else
  echo "Need sha256sum or shasum on PATH" >&2
  exit 1
fi

PUBLIC_KEY_HEX="$(cd "${ROOT_DIR}/../mtrxAI-common" && cargo run -q -p mtrxai-attestation --bin pubkey_from_seed -- "${ATTESTATION_SECRET}")"
PUBLIC_KEY_HEX="$(echo "${PUBLIC_KEY_HEX}" | tr -d '[:space:]')"

OUTPUT_PATH="$(dirname "${BINARY_PATH}")/allowed_build.json"

cat > "${OUTPUT_PATH}" <<EOF
{
  "build_id": "${BUILD_ID}",
  "binary_sha256": "${BINARY_SHA256}",
  "public_key": "${PUBLIC_KEY_HEX}",
  "version": "${VERSION}",
  "git_sha": "${GIT_SHA}",
  "platform": "${PLATFORM}",
  "binary_path": "${BINARY_PATH}"
}
EOF

API_BODY="$(printf '{"build_id":"%s","binary_sha256":"%s","public_key":"%s","version":"%s","git_sha":"%s","platform":"%s"}' \
  "${BUILD_ID}" "${BINARY_SHA256}" "${PUBLIC_KEY_HEX}" "${VERSION}" "${GIT_SHA}" "${PLATFORM}")"

SQL="$(cat <<EOF
INSERT INTO allowed_builds (build_id, binary_sha256, public_key, version, git_sha, platform)
VALUES (
  '${BUILD_ID}'::uuid,
  '${BINARY_SHA256}',
  '${PUBLIC_KEY_HEX}',
  '${VERSION}',
  '${GIT_SHA}',
  '${PLATFORM}'
)
ON CONFLICT (build_id) DO UPDATE SET
  binary_sha256 = EXCLUDED.binary_sha256,
  public_key = EXCLUDED.public_key,
  version = EXCLUDED.version,
  git_sha = EXCLUDED.git_sha,
  platform = EXCLUDED.platform,
  revoked_at = NULL;
EOF
)"

echo ""
echo "==> Allowed build manifest"
echo "Wrote: ${OUTPUT_PATH}"
echo ""
echo "build_id:      ${BUILD_ID}"
echo "binary_sha256: ${BINARY_SHA256}"
echo "public_key:    ${PUBLIC_KEY_HEX}"
echo "version:       ${VERSION}"
echo "git_sha:       ${GIT_SHA}"
echo "platform:      ${PLATFORM}"
echo ""
echo "--- SQL (paste into psql) ---"
echo "${SQL}"
echo ""
echo "--- Admin API (curl) ---"
echo "curl -fsS -X POST \"\${MTRXAI_LOBBY_URL:-http://127.0.0.1:8080}/api/admin/builds\" \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -H \"x-admin-key: \${MTRXAI_ADMIN_KEY}\" \\"
echo "  -d '${API_BODY}'"
