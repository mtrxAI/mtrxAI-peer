#!/usr/bin/env bash
# Download and register the Docker client allowed_build.json from a GitHub Release.
#
# Usage:
#   export MTRXAI_APP_REPO=cosmic-entropy-official/mtrxAI
#   export MTRXAI_ADMIN_KEY=...
#   scripts/register-release-allowed-build.sh --tag v0.1.5
#   scripts/register-release-allowed-build.sh --run-id <Release-workflow-run-id>
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$(cd "${ROOT_DIR}/../mtrxAI-infra" 2>/dev/null && pwd || true)"
TAG=""
RUN_ID=""

usage() {
  cat <<EOF
Usage: $(basename "$0") (--tag vX.Y.Z | --run-id <workflow-run-id>)

Downloads allowed_build.json from the Release workflow (Docker linux/x86_64)
and registers it on the lobby configured via MTRXAI_LOBBY_URL.

Required env:
  MTRXAI_ADMIN_KEY
  MTRXAI_APP_REPO (e.g. cosmic-entropy-official/mtrxAI)
  MTRXAI_LOBBY_URL (lobby admin base URL)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag) TAG="${2:?}"; shift 2 ;;
    --run-id) RUN_ID="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${TAG}" && -z "${RUN_ID}" ]]; then
  usage >&2
  exit 1
fi

FETCH="${INFRA_DIR}/scripts/fetch-client-attestation-manifests.sh"
if [[ ! -x "${FETCH}" ]]; then
  FETCH="${ROOT_DIR}/scripts/fetch-client-attestation-manifests.sh"
fi
if [[ ! -f "${FETCH}" ]]; then
  echo "fetch-client-attestation-manifests.sh not found (expected mtrxAI-infra sibling)" >&2
  exit 1
fi

ARGS=()
if [[ -n "${RUN_ID}" ]]; then
  ARGS=(--run-id "${RUN_ID}")
else
  ARGS=(--tag "${TAG}")
fi

bash "${FETCH}" "${ARGS[@]}"

MANIFEST="$(find "${INFRA_DIR:-${ROOT_DIR}}/artifacts" -path '*/release/docker/allowed_build.json' | head -1)"
if [[ -z "${MANIFEST}" ]]; then
  MANIFEST="$(python3 - <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
for path in sorted(root.rglob("allowed_build.json")):
    try:
        d = json.loads(path.read_text())
    except Exception:
        continue
    if d.get("platform") == "linux/x86_64":
        print(path)
        break
PY
"${INFRA_DIR:-${ROOT_DIR}}/artifacts")"
fi

if [[ -z "${MANIFEST}" || ! -f "${MANIFEST}" ]]; then
  echo "Could not find linux/x86_64 allowed_build.json in downloaded artifacts" >&2
  exit 1
fi

echo "==> Registering ${MANIFEST}"
bash "${ROOT_DIR}/scripts/register-allowed-build.sh" "${MANIFEST}"
