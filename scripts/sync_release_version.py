#!/usr/bin/env python3
"""Read release.toml and sync version into peer and desktop manifests in this repo."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASE_TOML = ROOT / "release.toml"

CARGO_MANIFESTS = [
    ROOT / "peer" / "Cargo.toml",
    ROOT / "desktop" / "src-tauri" / "Cargo.toml",
]
PACKAGE_JSON = ROOT / "desktop" / "package.json"
TAURI_CONF = ROOT / "desktop" / "src-tauri" / "tauri.conf.json"

SIBLING_CARGO = [
    ROOT.parent / "mtrxAI-server" / "server" / "Cargo.toml",
    ROOT.parent / "mtrxAI-common" / "mtrxai-attestation" / "Cargo.toml",
]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def read_release_config() -> dict[str, str]:
    text = _read_text(RELEASE_TOML)
    version = _require_match(r'^version\s*=\s*"([^"]+)"', text, "version")
    registry = _require_match(r'^registry\s*=\s*"([^"]+)"', text, "docker.registry")
    client_image = _require_match(r'^client_image\s*=\s*"([^"]+)"', text, "docker.client_image")
    server_image = _require_match(r'^server_image\s*=\s*"([^"]+)"', text, "docker.server_image")
    return {
        "version": version,
        "docker_registry": registry.rstrip("/"),
        "docker_client_image": client_image,
        "docker_server_image": server_image,
    }


def _require_match(pattern: str, text: str, label: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    if not match:
        raise SystemExit(f"release.toml: missing {label}")
    return match.group(1)


def set_release_version(version: str) -> None:
    version = version.removeprefix("v").strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+(-[\w.-]+)?(\+[\w.-]+)?", version):
        raise SystemExit(f"invalid semver: {version!r}")
    text = _read_text(RELEASE_TOML)
    text = re.sub(
        r'^version\s*=\s*"[^"]*"',
        f'version = "{version}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    _write_text(RELEASE_TOML, text)


def _sync_cargo(path: Path, version: str) -> None:
    text = _read_text(path)
    text = re.sub(
        r'^version\s*=\s*"[^"]*"',
        f'version = "{version}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    _write_text(path, text)


def _sync_json_version(path: Path, version: str) -> None:
    data = json.loads(_read_text(path))
    data["version"] = version
    _write_text(path, json.dumps(data, indent=2) + "\n")


def sync_all(version: str | None = None) -> str:
    if version is not None:
        set_release_version(version)
    cfg = read_release_config()
    version = cfg["version"]
    for manifest in CARGO_MANIFESTS:
        if not manifest.is_file():
            raise SystemExit(f"missing manifest: {manifest}")
        _sync_cargo(manifest, version)
    for manifest in SIBLING_CARGO:
        if manifest.is_file():
            _sync_cargo(manifest, version)
    if PACKAGE_JSON.is_file():
        _sync_json_version(PACKAGE_JSON, version)
    if TAURI_CONF.is_file():
        _sync_json_version(TAURI_CONF, version)
    return version


def print_env() -> None:
    cfg = read_release_config()
    registry = cfg["docker_registry"]
    version = cfg["version"]
    client = cfg["docker_client_image"]
    server = cfg["docker_server_image"]
    print(f"MTRXAI_VERSION={version}")
    print(f"MTRXAI_DOCKER_REGISTRY={registry}")
    print(f"MTRXAI_DOCKER_CLIENT_IMAGE={registry}/{client}")
    print(f"MTRXAI_DOCKER_SERVER_IMAGE={registry}/{server}")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: sync_release_version.py <get-version|sync|print-env> [version]")

    command = sys.argv[1]
    if command == "get-version":
        print(read_release_config()["version"])
        return
    if command == "print-env":
        print_env()
        return
    if command == "sync":
        version = sys.argv[2] if len(sys.argv) > 2 else None
        print(sync_all(version))
        return
    raise SystemExit(f"unknown command: {command}")


if __name__ == "__main__":
    main()
