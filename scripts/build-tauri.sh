#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAURI_DIR="${ROOT_DIR}/desktop"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ATTESTATION=0

usage() {
    cat <<EOF
Usage: $(basename "$0") [--attestation]

  --attestation   Embed signing keys and write release/.../allowed_build.json for DB insert
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --attestation) ATTESTATION=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

case "$(uname -s)" in
    Linux*) OS=linux ;;
    Darwin*) OS=macos ;;
    *)
        echo "Unsupported OS for build-tauri.sh: $(uname -s)" >&2
        echo "On Windows, use: .\\scripts\\build-tauri-windows.ps1" >&2
        exit 1
        ;;
esac

RELEASE_DIR="${ROOT_DIR}/release/${OS}"
export CARGO_TARGET_DIR="${RELEASE_DIR}"

PROFILE_DIR="${RELEASE_DIR}/release"

if [[ "${ATTESTATION}" -eq 1 ]]; then
    export MTRXAI_ATTESTATION_SKIP=0
    export MTRXAI_BUILD_ID="${MTRXAI_BUILD_ID:-$(uuidgen 2>/dev/null || python -c 'import uuid; print(uuid.uuid4())')}"
    export MTRXAI_ATTESTATION_SECRET="${MTRXAI_ATTESTATION_SECRET:-$(openssl rand -hex 32)}"
    echo "MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}"
    echo "Generated MTRXAI_ATTESTATION_SECRET (stored in allowed_build.credentials.env - required to rebuild this attested binary)"
    mkdir -p "${PROFILE_DIR}"
    # Persist before the long compile so a later manifest failure does not lose the secret.
    cat > "${PROFILE_DIR}/allowed_build.credentials.env" <<EOF
MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
MTRXAI_ATTESTATION_SKIP=0
EOF
    echo "Saved rebuild credentials: ${PROFILE_DIR}/allowed_build.credentials.env"
else
    export MTRXAI_ATTESTATION_SKIP="${MTRXAI_ATTESTATION_SKIP:-1}"
fi

echo "==> Building Tauri release for ${OS}"
echo "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"

if [[ ! -d "${TAURI_DIR}" ]]; then
    echo "Tauri project directory not found: ${TAURI_DIR}" >&2
    exit 1
fi

cd "${TAURI_DIR}"
npm install

# GitHub Actions sets CI=true. Tauri's bundle_dmg.sh uses Finder AppleScript and
# SetFile; that step fails on hosted macOS runners and would abort the whole
# attested desktop job (and skip allowlist publishing). Build the .app, then
# pack a plain UDZO DMG with hdiutil.
if [[ "${OS}" == "macos" && "${CI:-}" == "true" ]]; then
    echo "==> CI macOS: tauri --bundles app (skip bundle_dmg.sh)"
    npm run build -- --bundles app
    VERSION="${MTRXAI_VERSION:-}"
    if [[ -z "${VERSION}" ]]; then
        VERSION="$(python3 -c 'import json; print(json.load(open("src-tauri/tauri.conf.json"))["version"])')"
    fi
    case "$(uname -m)" in
        arm64) ARCH_LABEL=aarch64 ;;
        x86_64) ARCH_LABEL=x64 ;;
        *) ARCH_LABEL="$(uname -m)" ;;
    esac
    APP_BUNDLE="${PROFILE_DIR}/bundle/macos/mtrxAI.app"
    DMG_PATH="${PROFILE_DIR}/bundle/dmg/mtrxAI_${VERSION}_${ARCH_LABEL}.dmg"
    bash "${SCRIPT_DIR}/pack-macos-dmg.sh" "${APP_BUNDLE}" "${DMG_PATH}" "mtrxAI"
else
    npm run build
fi

echo ""
echo "==> Build complete"
echo "Release root: ${RELEASE_DIR}"
echo "Executable: ${PROFILE_DIR}/mtrxai"
if [[ -d "${PROFILE_DIR}/bundle" ]]; then
    echo "Bundles:"
    find "${PROFILE_DIR}/bundle" -type f | sed 's/^/  /'
fi

if [[ "${ATTESTATION}" -eq 1 ]]; then
    BINARY="${PROFILE_DIR}/mtrxai"
    if [[ ! -f "${BINARY}" ]]; then
        echo "Attested binary not found: ${BINARY}" >&2
        exit 1
    fi
    echo ""
    echo "==> Writing allowed_build manifest for DB insert"
    bash "${SCRIPT_DIR}/write-allowed-build-manifest.sh" "${BINARY}"
    cat > "${PROFILE_DIR}/allowed_build.credentials.env" <<EOF
MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
MTRXAI_ATTESTATION_SKIP=0
EOF
    echo "Saved rebuild credentials: ${PROFILE_DIR}/allowed_build.credentials.env"
fi
