# mtrxAI CLI Client

Runs two concurrent services:

1. **LLM HTTP proxy** — Ollama-compatible API on `proxy_port` (default `11345`)
2. **WebRTC manager** — Lobby WebSocket + P2P data channel for remote model routing

## Running

```bash
# From repo root
cargo run -p client

# With arguments: [proxy_port] [lobby_host] [ollama_host]
cargo run -p client -- 11346 127.0.0.1:8080 127.0.0.1:11434
```

Requires the lobby server (with PostgreSQL) to be running for peer registration and remote routing.

## Startup flow

1. Load or create `peer_config.json` via `POST /api/peers/register`
2. Connect to lobby WebSocket as `ws://<lobby>/ws?name=<peer_id>`
3. Discover geo/ASN and poll local Ollama models
4. Advertise models to the lobby (`updatemodels`)
5. Serve HTTP proxy and handle remote routing over WebRTC

## Source modules

| File | Role |
|------|------|
| `main.rs` | Startup, shared state, spawns proxy + WebRTC tasks |
| `llm_proxy.rs` | Axum HTTP server; local Ollama forward or remote via WebRTC |
| `webrtc_manager.rs` | Lobby WS, SDP signaling, data-channel proxy protocol, token reports |
| `peer_config.rs` | `peer_config.json` persistence and lobby registration |
| `token_usage.rs` | Parse `prompt_eval_count` / `eval_count` from Ollama streams |
| `ollama_client.rs` | Model catalog polling, `_status` enrichment, GPU probe |
| `peer_discovery.rs` | Public IP geolocation (lat/lon/ASN) |
| `agent_compat.rs` | Cursor/VS Code agent request normalization |
| `shared.rs` | `AppState`, `ProxyRequestCommand` |

### Legacy (not used by `src/main.rs`)

| File | Notes |
|------|-------|
| `../main.rs` | Interactive menu binary |
| `llm_chat.rs` | Standalone local Ollama chat |
| `remote_llm_chat.rs` | Older WebRTC chat protocol |

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MTRXAI_MODEL_POLL_SECS` | `30` | Ollama poll + lobby advertisement interval |
| `MTRXAI_GEO_LOOKUP_URL` | ip-api.com | Geo lookup endpoint |
| `MTRXAI_GPU_PROBE` | `auto` | GPU probe mode: `auto`, `off`, `force` (detects `nvidia-smi`, `amd-smi`/`rocm-smi`, Apple Silicon) |
| `MTRXAI_GPU_PROBE_VENDORS` | all | Comma-separated vendor filter: `nvidia`, `amd`, `apple`, or `all` |
| `MTRXAI_AGENT_DEBUG` | off | Agent traffic logging (`1` to enable) |

## Persistent files

**`peer_config.json`** (created on first run):

```json
{
  "peer_id": "uuid",
  "service_id": "uuid"
}
```

## Remote routing

When a request arrives for a model not in `local_models`:

1. `llm_proxy` enqueues a `ProxyRequestCommand`
2. `webrtc_manager` asks the lobby for ranked peers (`getpeersformodel`)
3. WebRTC data channel carries chunked HTTP request/response
4. Both sides send `reporttokenusage` to the lobby after completion

## HTTP proxy endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/tags`, `/api/chat`, `/api/generate`, … | Ollama-compatible |
| `/v1/models`, `/v1/chat/completions` | OpenAI-compatible |
| `/health` | Health check |
| `/debug/agent` | Last agent request/response summary |

## Docker

```bash
docker build -f client/Dockerfile -t mtrxai-client:latest .
docker compose up -d peerai-peer1
```

Docker peers use `MTRXAI_CLUSTER_NAME` (or `MTRXAI_ROOM_GROUP`), `MTRXAI_CONFIG_PATH`, and optional `MTRXAI_PROXY_PORT` / `MTRXAI_LOBBY_HOST` / `MTRXAI_OLLAMA_HOST`.

When the lobby requires service registration (`register` / `invitation` mode), the container **starts anyway** and serves the web UI at `http://<host>:<proxy-port>/` for manual setup. For unattended deploys, set `MTRXAI_SERVICE_NAME` and `MTRXAI_SERVICE_PASSWORD` (approved service credentials); the client auto-registers on first boot if `peer_id` is not yet stored. Pre-provisioned peers can use `MTRXAI_PEER_ID` instead. The service password is used once for registration and is **not** persisted in `client_config.json`.

## Related documentation

- [Root README](../README.md)
- [Server README](../server/README.md)
