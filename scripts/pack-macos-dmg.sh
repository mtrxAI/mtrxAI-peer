#!/usr/bin/env bash
# Create a compressed UDZO disk image without Finder AppleScript / SetFile.
# Tauri's bundle_dmg.sh (create-dmg) is unreliable on GitHub Actions macOS runners.
set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $(basename "$0") <app-bundle> <out.dmg> [volume-name]" >&2
    exit 1
fi

APP_BUNDLE="$1"
OUT_DMG="$2"
VOLNAME="${3:-mtrxAI}"

if [[ ! -d "${APP_BUNDLE}" ]]; then
    echo "App bundle not found: ${APP_BUNDLE}" >&2
    exit 1
fi

mkdir -p "$(dirname "${OUT_DMG}")"
rm -f "${OUT_DMG}"

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/mtrxai-dmg.XXXXXX")"
cleanup() { rm -rf "${STAGE}"; }
trap cleanup EXIT

APP_NAME="$(basename "${APP_BUNDLE}")"
cp -R "${APP_BUNDLE}" "${STAGE}/${APP_NAME}"
ln -s /Applications "${STAGE}/Applications"

echo "==> Creating DMG ${OUT_DMG} (volname=${VOLNAME})"
hdiutil create \
    -volname "${VOLNAME}" \
    -srcfolder "${STAGE}" \
    -ov \
    -format UDZO \
    -imagekey zlib-level=9 \
    "${OUT_DMG}"

echo "Wrote ${OUT_DMG}"
