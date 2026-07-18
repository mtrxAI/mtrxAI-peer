#!/usr/bin/env bash
# Build an attested mtrxAI peer Docker image and write allowed_build.json.
#
# Usage:
#   scripts/build-client-docker-attestation.sh [image-tag] [output-dir]
#
# Env (optional — generated when unset):
#   MTRXAI_BUILD_ID, MTRXAI_ATTESTATION_SECRET, MTRXAI_BUILD_PLATFORM

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORG_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_TAG="${1:-mtrx-peer:attested}"
OUT_DIR="${2:-${ROOT_DIR}/release/docker}"
CREDENTIALS_FILE="${OUT_DIR}/allowed_build.credentials.env"

if [[ "${GITHUB_ACTIONS:-}" != "true" && "${MTRXAI_FORCE_NEW_BUILD:-0}" != "1" && -f "${CREDENTIALS_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CREDENTIALS_FILE}"
  export MTRXAI_BUILD_ID MTRXAI_ATTESTATION_SECRET MTRXAI_BUILD_PLATFORM
  echo "==> Reusing build credentials from ${CREDENTIALS_FILE}"
fi

export MTRXAI_BUILD_ID="${MTRXAI_BUILD_ID:-$(uuidgen 2>/dev/null || python -c 'import uuid; print(uuid.uuid4())')}"
export MTRXAI_ATTESTATION_SECRET="${MTRXAI_ATTESTATION_SECRET:-$(openssl rand -hex 32)}"
export MTRXAI_BUILD_PLATFORM="${MTRXAI_BUILD_PLATFORM:-linux/x86_64}"

mkdir -p "${OUT_DIR}"

echo "==> Docker build ${IMAGE_TAG} (version=${MTRXAI_VERSION:-unknown})"
echo "MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}"

docker build -f "${ROOT_DIR}/Dockerfile" \
  --build-arg "MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}" \
  --build-arg "MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}" \
  --build-arg "MTRXAI_VERSION=${MTRXAI_VERSION:-unknown}" \
  -t "${IMAGE_TAG}" \
  "${ORG_DIR}"

cid="$(docker create "${IMAGE_TAG}")"
trap 'docker rm -f "${cid}" >/dev/null 2>&1 || true' EXIT
docker cp "${cid}:/usr/local/bin/peer" "${OUT_DIR}/client"
docker rm "${cid}"
trap - EXIT

bash "${SCRIPT_DIR}/write-allowed-build-manifest.sh" "${OUT_DIR}/client"

cat > "${OUT_DIR}/allowed_build.credentials.env" <<EOF
MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
MTRXAI_ATTESTATION_SKIP=0
MTRXAI_BUILD_PLATFORM=${MTRXAI_BUILD_PLATFORM}
IMAGE_TAG=${IMAGE_TAG}
EOF

echo "Saved rebuild credentials: ${OUT_DIR}/allowed_build.credentials.env"

if [[ -n "${MTRXAI_ADMIN_KEY:-}" ]]; then
  echo "==> MTRXAI_ADMIN_KEY set — registering build on lobby"
  bash "${SCRIPT_DIR}/register-allowed-build.sh" "${OUT_DIR}/allowed_build.json"
fi
