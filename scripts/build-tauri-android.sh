#!/usr/bin/env bash
# Build the mtrxAI Android APK (Tauri + in-process peer).
#
# Usage:
#   scripts/build-tauri-android.sh [--attestation] [--debug] [--target aarch64|x86_64|universal]
#
# Env:
#   ANDROID_HOME / NDK_HOME   Android SDK + NDK
#   MTRXAI_BUILD_ID / MTRXAI_ATTESTATION_SECRET  when --attestation
#   ANDROID_KEYSTORE_PATH / ANDROID_KEYSTORE_PASSWORD / ANDROID_KEY_ALIAS / ANDROID_KEY_PASSWORD
#     optional Play Store signing; otherwise release APK is signed with the debug keystore (sideload).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAURI_DIR="${ROOT_DIR}/desktop"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ATTESTATION=0
DEBUG=0
TARGET="aarch64"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--attestation] [--debug] [--target aarch64|x86_64|universal]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --attestation) ATTESTATION=1; shift ;;
    --debug) DEBUG=1; shift ;;
    --target)
      TARGET="${2:?}"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "${ANDROID_HOME:-}${ANDROID_SDK_ROOT:-}" ]]; then
  echo "ANDROID_HOME (or ANDROID_SDK_ROOT) is required" >&2
  exit 1
fi

RELEASE_DIR="${ROOT_DIR}/release/android"
mkdir -p "${RELEASE_DIR}/release"
export CARGO_TARGET_DIR="${ROOT_DIR}/release/android"

PROFILE_DIR="${RELEASE_DIR}/release"

if [[ "${ATTESTATION}" -eq 1 ]]; then
  export MTRXAI_ATTESTATION_SKIP=0
  export MTRXAI_BUILD_ID="${MTRXAI_BUILD_ID:-$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')}"
  export MTRXAI_ATTESTATION_SECRET="${MTRXAI_ATTESTATION_SECRET:-$(openssl rand -hex 32)}"
  export MTRXAI_BUILD_PLATFORM="${MTRXAI_BUILD_PLATFORM:-android/aarch64}"
  echo "MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}"
  mkdir -p "${PROFILE_DIR}"
  cat > "${PROFILE_DIR}/allowed_build.credentials.env" <<EOF
MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
MTRXAI_ATTESTATION_SKIP=0
MTRXAI_BUILD_PLATFORM=${MTRXAI_BUILD_PLATFORM}
EOF
else
  export MTRXAI_ATTESTATION_SKIP="${MTRXAI_ATTESTATION_SKIP:-1}"
fi

cd "${TAURI_DIR}"
npm ci 2>/dev/null || npm install

if [[ ! -d "src-tauri/gen/android" ]]; then
  npx tauri android init --ci --skip-targets-install
fi

BUILD_ARGS=(android build --target "${TARGET}")
if [[ "${DEBUG}" -eq 1 ]]; then
  BUILD_ARGS+=(--debug)
fi

echo "==> Building Android (${TARGET})${DEBUG:+ debug}"
npx tauri "${BUILD_ARGS[@]}"

ANDROID_OUT="src-tauri/gen/android/app/build/outputs"
if [[ "${DEBUG}" -eq 1 ]]; then
  APK_SRC="$(find "${ANDROID_OUT}/apk" -name '*.apk' | head -n1)"
else
  APK_SRC="$(find "${ANDROID_OUT}/apk" -path '*/release/*.apk' | head -n1 || true)"
  if [[ -z "${APK_SRC}" ]]; then
    APK_SRC="$(find "${ANDROID_OUT}/apk" -name '*.apk' | head -n1)"
  fi
fi

if [[ -z "${APK_SRC}" || ! -f "${APK_SRC}" ]]; then
  echo "APK not found under ${ANDROID_OUT}" >&2
  exit 1
fi

APK_DEST="${PROFILE_DIR}/mtrxai-android-${TARGET}.apk"
cp -f "${APK_SRC}" "${APK_DEST}"
echo "APK: ${APK_DEST}"

# Native peer library used for attestation (same code as desktop peer::run).
SO_SRC="$(find src-tauri/gen/android/app/src/main/jniLibs -name 'libmtrxai_tauri.so' | head -n1 || true)"
if [[ -z "${SO_SRC}" ]]; then
  SO_SRC="$(find "${CARGO_TARGET_DIR}" -name 'libmtrxai_tauri.so' | head -n1 || true)"
fi
if [[ -n "${SO_SRC}" && -f "${SO_SRC}" ]]; then
  cp -f "${SO_SRC}" "${PROFILE_DIR}/libmtrxai_tauri.so"
  echo "Native lib: ${PROFILE_DIR}/libmtrxai_tauri.so"
fi

if [[ "${ATTESTATION}" -eq 1 ]]; then
  if [[ ! -f "${PROFILE_DIR}/libmtrxai_tauri.so" ]]; then
    echo "Cannot attest: libmtrxai_tauri.so missing" >&2
    exit 1
  fi
  bash "${SCRIPT_DIR}/write-allowed-build-manifest.sh" "${PROFILE_DIR}/libmtrxai_tauri.so"
  echo "Attestation manifest: ${PROFILE_DIR}/allowed_build.json"
fi

echo "==> Android build complete"
ls -la "${PROFILE_DIR}"
