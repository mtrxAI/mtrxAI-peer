# mtrxAI Desktop Shell

Native desktop packaging for the mtrxAI **peer** application. Tauri does not implement app logic — it starts the same `peer` crate used in containers, opens a webview to the peer HTTP server, and shows the embedded UI from `peer/ui/v2/index.html`.

> **Note:** The production distributed LLM proxy path is the **CLI peer** (`peer/`). The desktop app is a thin wrapper around that binary logic. See the [root README](../README.md) for the full architecture.

## How it works

1. On first launch, the setup window defaults to the managed lobby (`api.mtrxai.net`). Saved settings are reused on later launches unless you delete them or set `MTRXAI_LOBBY_HOST`.
2. Tauri sets desktop environment variables (`MTRXAI_LOBBY_HOST`, `MTRXAI_PROXY_PORT`, `MTRXAI_CONFIG_PATH`, `MTRXAI_ATTESTATION_SKIP`) and spawns `peer::run()` in-process.
3. After the local client HTTP server is ready (`/health`), a webview opens at `http://127.0.0.1:11345` (default port).
4. All UI ↔ backend communication uses the same HTTP REST API as the container deployment (`/api/*` on the peer local server).

The desktop app is **self-contained** — it does not require Docker or any Ollama/peer containers. Lobby and LLM servers are runtime dependencies: the app always boots and shows connection status in the UI (amber indicators when offline, background retry). P2P registration and inference still need those services when you use those features.

No Tauri IPC is used for application features — only a bootstrap command for the lobby setup form.

## Prerequisites

- Rust 1.77+
- Node.js 18+ (for `@tauri-apps/cli` only)
- **Lobby server** (for P2P/network features; optional at boot — the app starts offline and retries)
- Ollama or another LLM backend (optional; configure via the peer UI — not required to launch)

## Installation

```bash
cd desktop
npm install
```

## Running (development)

### 1. Start PostgreSQL and lobby server

```bash
# From repo root
export DATABASE_URL=postgres://mtrxai:mtrxai@127.0.0.1:5432/mtrxAI
cargo run -p server
```

Or use Docker Compose from **mtrxAI-infra** `deploy/development/`.

### 2. Launch the desktop app

```bash
cd desktop
npm run dev
```

On first run the app connects to `api.mtrxai.net` by default. To use a local lobby, enter `127.0.0.1:8080` in the setup form, delete `desktop_settings.json` and restart, or set `MTRXAI_LOBBY_HOST` before launch:

```bash
export MTRXAI_LOBBY_HOST=127.0.0.1:8080
npm run dev
```

## Building a release

Desktop installers are written under OS-specific folders:

```
release/
├── windows/          # Windows (.ps1 script)
│   └── release/
│       ├── mtrxai.exe
│       └── bundle/
├── linux/            # Linux (build-tauri.sh)
│   └── release/
└── macos/            # macOS (build-tauri.sh)
    └── release/
```

From the repository root on Windows:

```powershell
.\scripts\build-tauri-windows.cmd
```

If PowerShell allows scripts on your machine, you can also run `.\scripts\build-tauri-windows.ps1` directly.

From Linux or macOS:

```bash
./scripts/build-tauri.sh
```

Or manually (uses the default workspace `target/` directory):

```bash
cd desktop
npm run build
```

## Project structure

```
desktop/
├── dist/                   # Minimal static assets (setup form + build placeholder)
├── src-tauri/
│   ├── src/
│   │   ├── main.rs         # Tauri entry + lobby bootstrap command
│   │   ├── bootstrap.rs    # Client spawn, health poll, window creation
│   │   └── settings.rs     # Desktop settings persistence
│   ├── capabilities/
│   └── tauri.conf.json
└── package.json            # Tauri CLI scripts only
```

## Desktop settings

Saved under the OS app config directory:

- `desktop_settings.json` — lobby host and proxy port
- `client_config.json` — same client config used by the CLI/container (`MTRXAI_CONFIG_PATH`)

To re-prompt for the lobby server, delete `desktop_settings.json` and restart the app.

## Related documentation

- [Root README](../README.md) — architecture, Docker Compose, peer proxy
- [Peer README](../peer/README.md) — peer API and configuration
- [Server README](../../mtrxAI-server/server/README.md) — lobby API and database

## License

Apache-2.0 — see [LICENSE](../LICENSE) in the repository root.
