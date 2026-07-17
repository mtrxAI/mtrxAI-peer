#!/usr/bin/env bash
set -euo pipefail
IMAGE="${1:-cosmicentropy/mtrxai-client:0.1.5}"
TMP="/tmp/mtrxai-client-hub-$$"
cid="$(docker create "${IMAGE}")"
trap 'docker rm -f "${cid}" >/dev/null 2>&1 || true; rm -f "${TMP}"' EXIT
docker cp "${cid}:/usr/local/bin/peer" "${TMP}"
docker rm "${cid}" >/dev/null
trap - EXIT
echo "image=${IMAGE}"
echo "sha256=$(sha256sum "${TMP}" | awk '{print $1}')"
echo "embedded_uuids:"
strings "${TMP}" | grep -E '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' | sort -u | head -5
