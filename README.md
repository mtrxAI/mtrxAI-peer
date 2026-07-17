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

## Docker

Build from the **org root** (`mtrxAI-org/`):

```bash
docker build -f peer/Dockerfile -t mtrxai-client .
```
