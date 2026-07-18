#!/usr/bin/env bash
# Load a Release CI peer image tarball and push it to Docker Hub.
# Use when Hub :VERSION tag is stale but attestation-docker-linux artifact matches the allowlist.
#
# Usage:
#   scripts/push-release-client-image.sh /path/to/mtrx-peer-image.tar.gz [version]
#
# Requires: docker login already done, or DOCKERHUB_USERNAME + DOCKERHUB_TOKEN in env.
set -euo pipefail

TARBALL="${1:?usage: push-release-client-image.sh <mtrx-peer-image.tar.gz> [version]}"
VERSION="${2:-$(python3 -c "import tomllib; print(tomllib.load(open('release.toml','rb'))['version'])" 2>/dev/null || echo 0.1.5)}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
eval "$(python3 "${ROOT_DIR}/scripts/sync_release_version.py" print-env 2>/dev/null | sed 's/^/export /' || true)"
REMOTE="${MTRXAI_DOCKER_CLIENT_IMAGE:-docker.io/mtrxai/mtrx-peer}"

echo "==> Loading ${TARBALL}"
gunzip -c "${TARBALL}" | docker load

LOCAL_TAG="mtrx-peer:${VERSION}"
if ! docker image inspect "${LOCAL_TAG}" >/dev/null 2>&1; then
  # Arch-specific tags from CI: mtrx-peer:VERSION-amd64 / arm64
  CANDIDATE="$(docker images --format '{{.Repository}}:{{.Tag}}' | awk -v v="${VERSION}" '$0 ~ ("^mtrx-peer:" v) {print; exit}')"
  if [[ -z "${CANDIDATE}" ]]; then
    echo "Expected loaded image tag ${LOCAL_TAG} (or mtrx-peer:${VERSION}-*) not found after docker load" >&2
    docker images | head -10
    exit 1
  fi
  LOCAL_TAG="${CANDIDATE}"
fi

if [[ -n "${DOCKERHUB_USERNAME:-}" && -n "${DOCKERHUB_TOKEN:-}" ]]; then
  echo "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin
fi

chmod +x "${ROOT_DIR}/scripts/docker-push-tags.sh"
"${ROOT_DIR}/scripts/docker-push-tags.sh" "${LOCAL_TAG}" "${REMOTE}" "${VERSION}"

echo "==> Verify embedded build_id matches allowlist for this release:"
docker run --rm --entrypoint /usr/local/bin/peer "${LOCAL_TAG}" 11345 127.0.0.1:8080 127.0.0.1:11434 2>&1 | grep Attestation || true
