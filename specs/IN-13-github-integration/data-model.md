# Phase 1 Data Model: GitHub Production Integration

## Overview

IN-13 introduces **one new column** on the existing `provider_installations` table and consumes the existing `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, and `observations` tables unchanged.

## New DDL

### Migration 0042: `selected_repositories` column

File: `db/migrations/0042_provider_installations_selected_repositories.sql`

```sql
-- 0042_provider_installations_selected_repositories.sql
--
-- IN-13: GitHub installations carry a per-installation repository allowlist
-- that the customer admin sets at install time and mutates via
-- `installation_repositories` webhook events. NULL means "all repositories"
-- (the admin granted the App org-wide access). A JSONB array of
-- `<owner>/<repo>` full-name strings means an explicit selection.
--
-- Idempotent. Additive. Default NULL preserves existing rows' semantics
-- as "all repositories" (no prior selection was recorded).
--
-- Reader cutover: not required. The new column is read-on-write only by
-- the new GitHub router branch added in this PR; existing Slack / Discord /
-- Linear / Stripe paths do not consult it.

BEGIN;

ALTER TABLE provider_installations
    ADD COLUMN IF NOT EXISTS selected_repositories JSONB DEFAULT NULL;

COMMIT;
```

**Tenant isolation (Constitution §III)**: `provider_installations` already enables RLS + `tenant_isolation` policy (migration 0039) + `tenant_id` FK to `tenants(id)` + `idx_provider_installations_tenant_provider` tenant-prefixed index. The new column inherits these properties — no further DDL is needed.

**Validation rules** (enforced in application code, not in DDL — for forward-compatibility with future GHES `repository_selection` schema additions):
- Either `NULL` (meaning "all repositories") OR a JSONB array of strings.
- Each string MUST match the regex `^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$` (GitHub's documented repo full-name shape).
- Array length capped at 10000 in application code; oversized writes log a structured ERROR and persist only the first 10000.

## Entities

### `GithubInstallation` (Pydantic model — application-layer view of `provider_installations` rows for `provider='github'`)

Defined in `services/integrations/github/oauth.py` and re-exported from the package `__init__.py`.

```python
class GithubInstallation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID                         # provider_installations.id (uuid7)
    tenant_id: UUID
    installation_id: str             # GitHub-issued numeric id, stored as string
    enabled: bool
    selected_repositories: list[str] | None  # None == "all repositories"
    installed_at: datetime
```

`secret_ref` is NOT exposed on the Pydantic view — GitHub does NOT use per-installation secrets (Clarifications Q1). The DB column `secret_ref` is always `NULL` for GitHub rows; the application code MUST NOT read it for GitHub.

### `GithubAppCredentials` (env-loaded, not persisted)

```python
class GithubAppCredentials(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app_id: str                      # GITHUB_APP_ID env var
    app_slug: str                    # GITHUB_APP_SLUG env var (used in install URL)
    private_key_pem: str             # GITHUB_APP_PRIVATE_KEY or contents of GITHUB_APP_PRIVATE_KEY_PATH
    webhook_secret: str              # current App-level webhook secret (FR-007)
    webhook_secret_prev: str | None  # previous secret during rotation overlap
```

The webhook-secret loading happens in `services/webhooks/secrets.py` (extended in Slice 4 T023); the `GithubAppCredentials` Pydantic model is a typed view used by `services/integrations/github/jwt.py` and `client.py`.

### `CachedInstallationToken` (in-process cache entry, not persisted)

```python
@dataclass(frozen=True, slots=True)
class CachedInstallationToken:
    token: str                       # GitHub installation access token
    expires_at: datetime             # UTC; cache evicts on expiry
```

Stored in `GithubClient._installation_tokens: dict[str, CachedInstallationToken]` keyed on `installation_id`. Process-local; not shared across pods. Invalidated by:
- TTL expiry on the next `mint_installation_token` call.
- Explicit invalidation in `_disable_installation_github` (when the chokepoint fires).

### `ReplayCacheKey` and `ReplayCacheValue` (in-process)

```python
ReplayCacheKey = tuple[str, str]    # (installation_id, X-GitHub-Delivery UUID)
ReplayCacheValue = float            # monotonic-clock seconds when first seen
```

5-minute TTL, 4096-entry LRU. Same shape as `services/webhooks/tenant_resolver.py::InstallationCache`.

## Reused Tables (no changes)

### `provider_installations` (migration 0039)

For `provider='github'`:
- `id` — uuid7
- `tenant_id` — FK to tenants
- `provider` — `'github'`
- `installation_id` — GitHub's numeric installation id, as a string (e.g., `'12345678'`)
- `secret_ref` — **always NULL** for GitHub (the App-level webhook secret lives in the secret store or env, not per-installation)
- `enabled` — boolean; flipped to FALSE by uninstall chokepoint OR `installation.deleted` / `installation.suspend` webhooks
- `installed_at` — timestamptz
- `selected_repositories` — **NEW** (migration 0042); JSONB array or NULL

### `encrypted_secrets`

For GitHub, **exactly one row** is consumed: the App-level webhook secret. The row's label convention is `github_app:webhook_secret` and `github_app:webhook_secret_prev` (during rotation). `tenant_id` for the App-level rows is `NULL` (App-scope, not tenant-scope) OR a deployment-wide singleton tenant_id; the exact value is operator-supplied via `GITHUB_APP_WEBHOOK_SECRET_REF`.

GitHub App private key is **NOT** stored in this table for v1 (Clarifications Q3 — env-only). If a follow-up promotes it to the secret store, the label convention will be `github_app:private_key`.

### `oauth_install_states` (migration 0040)

Consumed unchanged. Each install request inserts a row with `provider='github', nonce=<random>, tenant_id=<authenticated tenant>, expires_at=<now+10min>, consumed_at=NULL`. The callback atomically updates `consumed_at` to mark single-use.

### `installation_audit_log` (migration 0041)

Consumed unchanged. New action vocabulary documented in `research.md` R5.

### `observations`

Consumed unchanged. GitHub events produce rows with:
- `source_channel='github:webhook'` (existing handler constant)
- `external_id` per the existing handler's per-event shaping (PR node_id, issue node_id, comment node_id, commit-after-sha, check_run node_id)
- `tenant_id` resolved from `installation.id` via `TenantResolver`
- `trust_tier`, `kind`, `content_text`, `content`, `entities_hint`, `raw_payload` per `services/ingestion/handlers/github.py`

Idempotency on `(source_channel, external_id, occurred_at)` is enforced by the existing UNIQUE index.

## Relationships

```text
tenants (1) ──── (N) provider_installations [provider='github']
                          │
                          ├── (1..1) selected_repositories JSONB (nullable)
                          ├── (1..N) installation_audit_log [via installation_row_id]
                          └── (0..N) observations [via tenant_id + payload.installation.id resolution]

deployment-wide ─── (1..2) encrypted_secrets [label='github_app:webhook_secret(_prev)?']
                    (NOT per-tenant for GitHub)

deployment-wide ─── (operator-supplied env) GITHUB_APP_PRIVATE_KEY
                    (NOT per-tenant; NOT in encrypted_secrets in v1)
```

## State Transitions

### `provider_installations.enabled`

- `NULL → TRUE` on `install` (first OAuth callback)
- `TRUE → FALSE` on `uninstall` (inbound `installation.deleted` OR outbound chokepoint)
- `TRUE → FALSE` on `suspend` (inbound `installation.suspend`)
- `FALSE → TRUE` on `reinstall` (OAuth callback for previously-disabled row)
- `FALSE → TRUE` on `unsuspend` (inbound `installation.unsuspend`)

Each transition writes an `installation_audit_log` row.

### `provider_installations.selected_repositories`

- `NULL → JSONB array` on first `setup_action='install'` callback (if `GET /installation/repositories` succeeds) OR on first `installation_repositories.added` event (if customer was previously "all-repos" and switched to "selected")
- `JSONB array → NULL` on flip back to "all-repos" mode (`repository_selection='all'` on `installation_repositories` payload root)
- `JSONB array → JSONB array` on `installation_repositories.added` / `.removed` (idempotent merge)
- `NULL → NULL` on `installation_repositories.added` if `repository_selection='all'` (no-op; audit row written)

### `CachedInstallationToken` lifecycle

- `MISS → INSERT` on first `mint_installation_token(installation_id)` call after install
- `HIT → READ` on subsequent calls within TTL
- `EXPIRY → DELETE + INSERT` on call after TTL elapsed
- `EVICT` on `_disable_installation_github` (chokepoint or inbound uninstall)

## Indexes

No new indexes. The existing UNIQUE `(provider, installation_id)` and tenant-prefixed `idx_provider_installations_tenant_provider` cover all read paths added by this feature:

- Router-side tenant resolution: `WHERE provider='github' AND installation_id=$1 AND enabled=TRUE` (uses UNIQUE + partial filter)
- OAuth callback UPSERT: `ON CONFLICT (provider, installation_id)` (uses UNIQUE)
- Admin enumeration "list a tenant's GitHub installations": `WHERE tenant_id=$1 AND provider='github'` (uses idx_provider_installations_tenant_provider)

The `selected_repositories` column is read on every webhook delivery but it's a single-row lookup, not a JSONB query — no GIN index needed.

## Encoding Conventions

- `installation_id`: GitHub's numeric id stored as a TEXT string (matches the IN-08 / IN-09 convention for `team_id` and `guild_id`).
- `selected_repositories`: JSONB array of strings; each string is the canonical GitHub `<owner>/<repo>` full-name.
- `external_id`: PR/issue/comment node_id strings, or `<repo>@<sha>` for push events — verbatim from the existing GitHub handler.
- `source_actor_ref`: `github:<login>` per the existing handler.
- Audit-log `context` JSONB fields:
  - `installation_id_hash`: 8-byte BLAKE2b hex of `installation_id` (for forensic correlation without exposing the raw id)
  - `delivery_id`: GitHub's `X-GitHub-Delivery` UUID (for correlation with GitHub's delivery log UI)
  - `event_type`: GitHub's `X-GitHub-Event` value
  - `added` / `removed`: lists of `<owner>/<repo>` strings (for `repo_change` actions)
  - `selected_repositories_unknown`: bool (true when callback completed but `GET /installation/repositories` failed)
  - `selected_repositories_truncated`: bool + `total_available: int` (true when pagination hit the 90-repo cap)
