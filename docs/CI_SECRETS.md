# GitHub Actions secrets & tokens

Suggested GitHub repos: `mtrxAI-peer`, `mtrxAI-server`, `mtrxAI-common`, `mtrxAI-icell`, `mtrxAI-infra`, `mtrxAI-website`.

## mtrxAI-peer

### Required for Test workflow

| Secret | Purpose |
|--------|---------|
| `MTRXAI_COMMON_READ_TOKEN` | PAT with `contents:read` on **private** `mtrxAI-common`. Skip if common is public (`github.token` is enough). |

### Required for Release (attested Docker + desktop)

| Secret | Purpose |
|--------|---------|
| `MTRXAI_COMMON_READ_TOKEN` | Same as above (checkout `common` for path deps / attestation pubkey tool). |
| `DOCKERHUB_USERNAME` | Docker Hub user that can push `cosmicentropy/mtrxai-client`. |
| `DOCKERHUB_TOKEN` | Docker Hub access token (push). Required when `push_docker` is true / on `v*` tags. |
| `MTRXAI_INFRA_DISPATCH_TOKEN` | PAT with `repo` scope on **mtrxAI-infra** — dispatches `client-release` so the lobby allowlist updates. |

### Optional

| Secret | Purpose |
|--------|---------|
| _(none for Apple signing yet)_ | macOS builds are unsigned unless you add Apple Developer certs later. |

Release artifacts (8): Docker linux amd64/arm64 + desktop linux/windows/macos × amd64/arm64.

---

## mtrxAI-server

| Secret | Purpose |
|--------|---------|
| `MTRXAI_COMMON_READ_TOKEN` | Read private `mtrxAI-common` for Test CI. |
| `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN` | Only if you add a server image Release workflow that pushes `mtrxai-server`. |

Server has Test CI today; no attested peer-style release in this pass.

---

## mtrxAI-common

| Secret | Purpose |
|--------|---------|
| _(none)_ | Public or org-internal crate; Test needs no special secrets. |

If common is private, peer/server need `MTRXAI_COMMON_READ_TOKEN` instead.

---

## mtrxAI-icell

| Secret | Purpose |
|--------|---------|
| _(none)_ | Build/test only. |

---

## mtrxAI-infra

| Secret | Purpose |
|--------|---------|
| `MTRXAI_ADMIN_KEY` | Lobby admin API key — `POST /api/admin/builds`. |
| `MTRXAI_LOBBY_URL` | Production lobby base URL (e.g. `https://app.mtrxai.net`). |
| `MTRXAI_APP_READ_TOKEN` | PAT with `contents` + `actions` read on **mtrxAI-peer** — download Release artifacts. Alias: `MTRXAI_CLIENT_READ_TOKEN`. |

Infra receives `repository_dispatch` from peer (`MTRXAI_INFRA_DISPATCH_TOKEN` lives on the **peer** repo).

---

## mtrxAI-website

| Secret | Purpose |
|--------|---------|
| _(deploy-specific)_ | Only if you add Pages/hosting deploy secrets. |

---

## PAT scopes (quick)

| Token | Create on | Scopes / permissions |
|-------|-----------|----------------------|
| `MTRXAI_COMMON_READ_TOKEN` | peer (+ server) | fine-grained: read contents of `mtrxAI-common` |
| `MTRXAI_INFRA_DISPATCH_TOKEN` | peer | fine-grained: read/write Actions on `mtrxAI-infra` (or classic `repo`) |
| `MTRXAI_APP_READ_TOKEN` | infra | fine-grained: read contents + actions on `mtrxAI-peer` |
| `DOCKERHUB_*` | peer (and server if needed) | Docker Hub access token |
| `MTRXAI_ADMIN_KEY` / `MTRXAI_LOBBY_URL` | infra | lobby credentials (not GitHub) |
