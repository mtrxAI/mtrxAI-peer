# Confidential Inference on NVIDIA GPU TEE

Deploy mtrxAI as a **zero-trust inference provider** using NVIDIA Hopper (H100/H200) Confidential Computing inside an Intel TDX or AMD SEV-SNP confidential VM.

Attestation flags, icell’s role, and lobby vs client TEE flux: **[ATTESTATION.md](./ATTESTATION.md)**.

## Architecture

```
Host hypervisor
  └── Confidential VM (TDX / SEV-SNP)
        ├── mtrxAI client (P2P relay — no session keys when sidecar enabled)
        ├── mtrxAI icell sidecar (decrypt / encrypt)
        ├── Ollama + model weights
        └── NVIDIA H100/H200 CC (encrypted HBM)
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `MTRXAI_E2EE_ENABLED=1` | Application-layer E2EE on proxy protocol v2 (default: on) |
| `MTRXAI_INFERENCE_SIDECAR=1` | Run inference in isolated sidecar; P2P relay stays blind |
| `MTRXAI_INFERENCE_IPC_PORT=12745` | Local TCP port for sidecar IPC |
| `MTRXAI_REQUIRE_TEE=1` | Consumer rejects non-TEE providers when ranking peers |
| `MTRXAI_TEE_MOCK=1` | CI/dev: accept mock GPU attestation without NRAS |
| `MTRXAI_PROXY_BIND=127.0.0.1` | Bind local HTTP proxy to loopback (default) |
| `MTRXAI_PROXY_TOKEN` | Bearer token required for local proxy routes |

## Provider setup

1. Boot a confidential VM with GPU passthrough (H100 CC enabled).
2. Install Ollama and mtrxAI inside the VM only.
3. Set `MTRXAI_INFERENCE_SIDECAR=1`.
4. Set `gpu_cc_mode: true` in `client_config.json`.
5. Register GPU attestation with the lobby (NRAS via `mtrxAI-tee-attestation`).

## TLS for production lobby

Terminate HTTPS/WSS at a reverse proxy (nginx, Caddy, cloud LB). WebSocket signaling must use `wss://`.

For the production server stack, use the automated nginx setup in **mtrxAI-infra** `deploy/production/server/nginx/README.md`:

- `setup-nginx.sh` — normal install
- `repair-nginx.sh` — fix broken apt/nginx on Ubuntu
- Default TLS paths: `/etc/ssl/mtrxai/rsa.pem` + `rsa.key` (or your own certificate directory)
- Bootnode TCP proxied on `:4010` via nginx `stream {}`

## Trust levels

| `trust_level` | Meaning |
|---------------|---------|
| `tee_gpu` | H100+ CC + verified attestation |
| `host` | App E2EE + sidecar split |
| `transport` | libp2p Noise / WebRTC DTLS only |
