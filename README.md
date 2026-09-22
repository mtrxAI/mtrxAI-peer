# mtrxAI peer (inference / desktop node)

Cargo workspace: `peer`, `peer-tests`, `desktop/src-tauri`.

## Checkout layout

Clone siblings under `mtrxAI-org`:

```
mtrxAI-org/
  common/    # required path dependency
  peer/      # this repo
  server/
  icell/
  mtrxAI-infra/
  mtrxAI-website/
```

## Build

```bash
cargo build -p peer
cargo test -p peer -p peer-tests
```

## Docker (peer + icell)

**Beginner deploy-and-use guide** (official images, dashboard, models, troubleshooting):

[mtrxAI-infra/deploy/production/peer/README.md](../mtrxAI-infra/deploy/production/peer/README.md)

That stack runs **mtrxAI peer** plus a sealed **icell** (Ollama backend). Icell exists so attestation is not limited to the peer binary — see [docs/ATTESTATION.md](docs/ATTESTATION.md). Install Docker first: [Get Docker](https://docs.docker.com/get-docker/).

Quick start (from `mtrxAI-infra/deploy/production/peer`):

```bash
cp .env.example .env   # set MTRXAI_LOBBY_HOST and MTRXAI_P2P_ANNOUNCE_HOST
docker compose up -d
```

Dashboard: `http://localhost:11345/`

To **build** the peer image yourself, run this from the **org root** (sibling of `mtrxAI-peer/` and `mtrxAI-common/`):

```bash
docker build -f mtrxAI-peer/Dockerfile -t mtrxai/mtrx-peer .
```

## CI

- **Test** — unit + all `peer-tests` integration suites (matrix).
- **Release** — attested Docker (linux amd64/arm64) + desktop (Windows/Linux amd64+arm64, macOS arm64).

Android chat APK (consumer peer, chat-first UI): see [desktop/README.md](desktop/README.md#android-chat-apk).

Secrets: see [docs/CI_SECRETS.md](docs/CI_SECRETS.md).
