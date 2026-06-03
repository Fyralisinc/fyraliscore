# GitHub (IN-13)

> Repository activity (PRs, pushes, issues, reviews, checks) as the *what
> happened* layer. A GitHub **App** install, not per-user OAuth.

| Field | Value |
|---|---|
| Source | `github` |
| Primary channel | `github:webhook` |
| Trust tier | `authoritative` |
| Live ingress | App **webhook** → full pipeline (cutover-enabled) |
| Backfill | per accessible repo |
| Auth | GitHub App — JWT (RS256) → installation access token (cached) |
| Signature | HMAC-SHA256, single App-level webhook secret |

## Auth & install

GitHub App, self-serve install
([services/ingest/integrations/github/oauth.py](../../../services/ingest/integrations/github/oauth.py)):
`/integrations/github/install` → `/callback` (public allowlist entry) UPSERTs the
install and seeds `selected_repositories`.

- [jwt.py](../../../services/ingest/integrations/github/jwt.py) — `mint_app_jwt` RS256
  from `GITHUB_APP_PRIVATE_KEY` env, **re-read on every mint** so rotation is a
  no-op deploy.
- [client.py](../../../services/ingest/integrations/github/client.py) — installation
  access-token cache + outbound chokepoint; 404/401 disables the install (no
  secret deletion — auth is App-level).
- [lifecycle.py](../../../services/ingest/integrations/github/lifecycle.py) —
  `installation.*` + `installation_repositories.*` dispatch.
- [replay_cache.py](../../../services/ingest/integrations/github/replay_cache.py) —
  in-process LRU keyed on `(installation_id, X-GitHub-Delivery)`;
  defense-in-depth, observation-layer dedup is the correctness backstop.

## Ingress (live)

`gateway /webhooks/github` → signature verified (single App secret;
[signatures/github.py](../../../services/app/webhooks/signatures/github.py)). Handled
events: `pull_request`, `push`, `issues`, `issue_comment`,
`pull_request_review`, `check_run`.

**Lifecycle events intercept at the router, not the ingestion handler.**
`installation.*` and `installation_repositories.*` produce audit rows + state
changes; they do **not** create observations. Content events → `github:webhook`
handler → full pipeline (cutover) or inline fallback.

The App webhook secret is deployment-wide (not tenant-scoped), so the env-var
secret path is permitted in prod for GitHub **without**
`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW`; per-tenant isolation is structural via the
`installation.id`-based tenant resolver.

## Backfill

[planners/github.py](../../../services/ingest/ingestion/planners/github.py) shards per
accessible repo; [fetchers/github.py](../../../services/ingest/ingestion/fetchers/github.py)
pulls events → `RawEnvelope` (`ingress_kind="backfill"`) → same `github:webhook`
channel.

## Migrations

- `0042_provider_installations_selected_repositories.sql` — nullable JSONB
  `selected_repositories` (NULL = all repos; array = explicit selection).
- `0043_widen_installation_audit_log_actions.sql` — widens the audit `action`
  CHECK for GitHub transitions (reinstall/update/suspend/unsuspend/repo_change/…).

No raw `installation_id` in logs (FR-016). Spec:
`specs/IN-13-github-integration/`. See [architecture.md](../architecture.md).
