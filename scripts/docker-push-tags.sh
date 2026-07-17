#!/usr/bin/env bash
# Tag a local image and push version + latest (+ optional sha) to a registry.
#
# Usage:
#   scripts/docker-push-tags.sh <local-image> <remote-image-base> <version> [git-sha]

set -euo pipefail

LOCAL_IMAGE="${1:?local image required}"
REMOTE_BASE="${2:?remote image base required (no tag)}"
VERSION="${3:?version required}"
GIT_SHA="${4:-}"

echo "==> Tagging ${LOCAL_IMAGE} -> ${REMOTE_BASE}"

docker tag "${LOCAL_IMAGE}" "${REMOTE_BASE}:${VERSION}"
docker tag "${LOCAL_IMAGE}" "${REMOTE_BASE}:latest"
docker push "${REMOTE_BASE}:${VERSION}"
docker push "${REMOTE_BASE}:latest"

if [[ -n "${GIT_SHA}" ]]; then
  docker tag "${LOCAL_IMAGE}" "${REMOTE_BASE}:${GIT_SHA}"
  docker push "${REMOTE_BASE}:${GIT_SHA}"
fi

echo "Pushed ${REMOTE_BASE}:${VERSION} and :latest"
