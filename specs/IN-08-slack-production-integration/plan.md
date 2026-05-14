# Implementation Plan: Slack Production Integration — OAuth Install, DB-Backed Secrets, Customer Self-Serve

**Branch**: `feat/IN-08-slack-production-integration` | **Date**: 2026-05-14 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification at `specs/IN-08-slack-production-integration/spec.md`

## Summary

Close the gap between IN-06 (Slack webhook receive + ingestion) and IN-07 (DB-backed `TenantResolver` engine). After this feature ships, a Slack workspace admin can self-serve "Add to Fyralis" via OAuth and have inbound messages land as `Observations` under the correct `tenant_id` — with no operator intervention, no plaintext secrets, and uninstalls handled cleanly.

**Technical approach**: introduce a generic envelope-encrypted `encrypted_secrets` row store backing a new `lib/shared/secrets/` module; wire the IN-07 `TenantResolver` into the webhook router (replacing the env-var path entirely); add a Slack OAuth install/callback pair under a new `services/integrations/` package whose callback consumes single-use state tokens persisted in `oauth_install_states`, performs `oauth.v2.access`, persists tokens via the secret store, and UPSERTs `provider_installations` rows; extend the existing Slack ingestion handler to recognize `app_uninstalled` / `tokens_revoked` and disable+zeroize; and provide a thin outbound `lib/integrations/slack/client.py` (mounted via `services/integrations/slack/client.py` per ClickUp) for `chat.postMessage` / `users.info` / `conversations.info`.

The hot-path read remains O(1): cached `(provider, installation_id) → Installation` in the resolver, decrypted-on-demand secret material via the same per-request lookup. The cold-path (OAuth callback) runs at workspace-install cadence — not a performance concern.

## Technical Context

**Language/Version**: Python 3.12 (project venv pinned). New code uses `from __future__ import annotations`, full type hints, Pydantic v2 with `extra="forbid"` at wire boundaries.
**Primary Dependencies**: existing — FastAPI, uvicorn[standard], asyncpg, Pydantic v2, structlog, `lib.shared.errors`, `lib.shared.ids.uuid7`, `services.webhooks.tenant_resolver` (IN-07). New — `cryptography` (`Fernet`, already in tree via Slack/Discord HMAC paths if present; otherwise add to `pyproject.toml`), `httpx` (already used elsewhere; outbound Slack API).
**Storage**: Postgres 16 + pgvector (existing). Three new tenant-scoped tables: `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`. No change to partition layout; no `Observations` written by this feature directly.
**Testing**: `pytest` with the `db_pool` / `fresh_db` fixtures (real Postgres on `localhost:5433`). Markers: `integration` for DB-touching tests. Slack HTTP boundary mocked via `respx` (permitted under §IV).
**Target Platform**: Linux server (Docker Compose, `docker-compose.yml` topology). Same `gateway` service image.
**Project Type**: Web service (FastAPI gateway + asyncpg + Postgres). No frontend changes shipping in this task — the install-success / install-error redirect targets are URL contracts; the UI rendering of those pages is out of scope.
**Performance Goals**: Webhook hot path unchanged (no new DB calls beyond what IN-07 already does, plus one envelope-decrypt per request when verifying signatures). OAuth callback budget: complete within 5 s p95 (Slack imposes no hard timeout on the redirect, but the human admin will perceive >5 s as broken). Outbound Slack client honors Tier 1–4 rate limits with bounded backoff.
**Constraints**: Workspace signing secrets MUST NOT live in env vars or in plaintext at rest in any production environment (SC-002). Forged `team_id` MUST resolve to HTTP 401 `unknown_installation` (SC-007); IN-07 SC-008 (no log leak) remains the controlling rule. All new substrate-adjacent IDs use `uuid7()`. State tokens are single-use server-side.
**Scale/Scope**: Per-tenant. Multi-tenant. Expected install rate: ≤10 installs/day during private beta, ≤100/day at GA. Webhook RPS bounded by tenant count × Slack workspace activity (~10–100 RPS realistic). `encrypted_secrets` row count grows ~3 rows per provider install (bot token + user token + signing secret) — well within Postgres b-tree comfort zone.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Compliance | Notes |
|-----------|-----------|-------|
| **§I Four Foundations** | ✅ PASS | No new Foundation. `encrypted_secrets`, `oauth_install_states`, `installation_audit_log` are side tables for cross-cutting concerns (credentials / auth flow / audit), explicitly permitted by §I. Install flow does not produce `Observations`; downstream `slack:message` webhooks do (preserves Universal Flow Rule). |
| **§II Append-only migrations** | ✅ PASS | Two new migrations: `0040_slack_installation_tokens.sql` (creates `encrypted_secrets` **and** `oauth_install_states` — one file may create multiple related objects), `0041_installation_audit_log.sql`. Both additive (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`), idempotent, slotted after the latest applied migration `0039_provider_installations.sql`. No edits to applied migrations. |
| **§III Tenant isolation** | ✅ PASS | All three new tables get `tenant_id UUID NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`, `ENABLE` + `FORCE ROW LEVEL SECURITY` with the `tenant_isolation` policy, and tenant-prefixed indexes on the hot lookup predicate. Hand-rolled `WHERE tenant_id = $1` is required for all new queries. Cross-tenant joins forbidden. |
| **§IV Real DB in integration tests** | ✅ PASS | All new integration tests use `fresh_db` (real Postgres). Slack HTTP boundary mocked via `respx` (allowed). No mocked Postgres anywhere. The existing `services/webhooks/tests/test_tenant_resolver_admin.py` continues to pass unchanged (SC-008). |
| **§V Reasoning vs Rendering split** | ✅ PASS — N/A | No LLM calls in this feature. |
| **§VI Trust/Confidence/Falsifier** | ✅ PASS — N/A | No `Model` writes in this feature. |
| **§VII Determinism, IDs, audit** | ✅ PASS | All new substrate-adjacent rows allocated via `lib.shared.ids.uuid7()`. The `installation_audit_log` is a **side audit table** distinct from §VII's `audit_events` chain (which governs Model state transitions). Region locks and queue patterns are not invoked here. |
| **§VIII Structured errors** | ✅ PASS | Three new `CompanyOSError` subclasses: `InstallationCollisionError` (HTTP 409, code `installation_collision`), `StateTokenInvalidError` (HTTP 400/401 depending on reason), `SecretStoreError` (HTTP 503 when store unavailable). Existing `InstallationNotFoundError` / `InstallationConflictError` reused. |
| **§IX Dual-write until proven** | ✅ PASS | The env-var → DB cutover in `services/webhooks/secrets.py` follows the dual-write template: dev/test path may still read from env vars under `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`; prod path is DB-only. A measured soak (24 h staging) precedes deletion of `services/webhooks/tenant_resolution.py` (explicit in the ClickUp body; tracked here so it doesn't slip). |
| **§X YAGNI** | ✅ PASS | Pluggable secret-store interface earns its keep (Fernet now, KMS planned per spec) — same bar as the `Embedder` Protocol. No new feature flags beyond `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW`. No new abstractions for the outbound Slack client beyond what's needed for the three named endpoints. |

**Stack constraints**: respected. Python ≥3.11, asyncpg, FastAPI factory routers, Postgres 16. No module-level globals for pools, embedders, providers. structlog JSON; no `print()` in service code.

**Review gates** (constitution Workflow §Review-gates): none of the rejection criteria apply — no edits to applied migrations, no audit-chain bypass, no Model writes (so `born_from_event_id` is N/A), all new tenant-scoped tables get FK + RLS + index, no `uuid.uuid4()` for substrate rows, no `print()`, no mocked Postgres.

## Project Structure

### Documentation (this feature)

```text
specs/IN-08-slack-production-integration/
├── source.md                # Verbatim ClickUp body (Phase 0 output)
├── spec.md                  # Phase 1 output (with Phase 2 clarifications inline)
├── plan.md                  # This file (Phase 3 output)
├── research.md              # Phase 3 Phase-0 output — research findings
├── data-model.md            # Phase 3 Phase-1 output — table & entity shapes
├── quickstart.md            # Phase 3 Phase-1 output — local exercise of install flow
├── contracts/               # Phase 3 Phase-1 output — HTTP & module contracts
│   ├── http-integrations-slack.md
│   ├── http-webhooks-slack-events.md
│   └── module-secret-store.md
├── checklists/
│   └── requirements.md      # Pre-existing checklist
└── tasks.md                 # Phase 4 output (NOT created by /speckit-plan)
```

### Source Code (repository root)

The scope-boundary is the ClickUp `Files relevant` list (verbatim in [spec.md §Scope Boundary](./spec.md#scope-boundary-verbatim-from-clickup-files-relevant)). Plan does not expand it.

```text
services/integrations/                       # NEW package — top-level FastAPI router for /integrations/*
├── __init__.py
├── router.py                                # NEW — mounts /integrations/slack/* sub-router
├── slack/
│   ├── __init__.py
│   ├── oauth.py                             # NEW — install + callback handlers
│   ├── uninstall.py                         # NEW — app_uninstalled / tokens_revoked dispatch
│   └── client.py                            # NEW — outbound Slack Web API (chat.postMessage, users.info, conversations.info)
└── tests/                                   # NEW — integration tests for the above
    ├── conftest.py
    ├── test_oauth_install.py
    ├── test_oauth_callback.py
    ├── test_uninstall.py
    └── test_client.py

lib/shared/secrets/                          # NEW module — envelope-encrypted secret store
├── __init__.py                              # Public API: SecretStore protocol, build_secret_store(), errors
├── store.py                                 # FernetSecretStore impl
└── tests/
    └── test_store.py

db/migrations/
├── 0040_slack_installation_tokens.sql       # NEW — creates `encrypted_secrets` AND `oauth_install_states`
└── 0041_installation_audit_log.sql          # NEW — creates `installation_audit_log`

services/webhooks/
├── router.py                                # CHANGED — TenantResolver replaces resolve_tenant; PayloadMissing → 400, UnknownInstallation → 401
├── secrets.py                               # CHANGED — load_secrets resolves provider_installations.secret_ref via secret store; env-var fallback gated by WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1
├── signatures/
│   └── slack.py                             # CHANGED — accepts per-installation signing secret from the secret store
└── tenant_resolution.py                     # DEPRECATED — retained as a no-op shim for one staging cycle, deleted post-soak (per ClickUp Phase 2)

services/gateway/
└── main.py                                  # CHANGED — mount services.integrations.router; add /integrations/slack/install and /integrations/slack/callback to _PUBLIC_PATH_PREFIXES (specific routes, NOT a /integrations/ blanket)
```

**Structure Decision**: All new code lives under `services/integrations/` (a brand-new top-level package created by this feature) and `lib/shared/secrets/` (a new shared lib module). The existing `services/webhooks/` package is only modified — no files added there. The two new migration files slot into the existing `db/migrations/` directory using the next two unused numbers.

A single thing to note about the gateway public-path allowlist: today's `_PUBLIC_PATH_PREFIXES` allows blanket-prefix entries (e.g., `"/webhooks/"`). For the OAuth callback, we MUST NOT add `"/integrations/"` as a prefix (that would expose every future integration route publicly). The fix is structural: add specific `/integrations/slack/install` and `/integrations/slack/callback` entries to a NEW `_PUBLIC_PATHS_EXACT` frozenset checked separately (since `_PUBLIC_PATHS` today does exact match, we can reuse it). This is a small but principled change to `services/gateway/main.py` and is in-scope per the ClickUp body's "single-route, not blanket public" wording.

## Phase 0 — Outline & Research

Unknowns extracted from the Technical Context and clarifications:

1. **Fernet vs. alternative envelope encryption library** — confirm `cryptography.fernet` is adequate for MVP and how to structure the key-rotation seam for future KMS.
2. **Slack `oauth.v2.access` response shape** — exact JSON fields (bot token, user token, signing secret? — verify Slack returns the signing secret in the OAuth response or whether it's a per-app constant).
3. **Slack 429 Retry-After parsing** — confirm Slack uses standard `Retry-After` header semantics.
4. **Production environment detection** — which existing config flag the env-var fallback gate reads (`ENV`, `DEPLOY_ENV`, `FYRALIS_ENV`?).
5. **Install / uninstall metric labels** — name and label set for `slack_install_outcomes_total` so downstream dashboards key off a stable contract.
6. **OAuth callback redirect target hosts** — does Fyralis already have a configured app-host base URL for absolute redirects, or do we 302 to relative paths?
7. **Existing webhooks router test fixtures** — confirm `app.state.tenant_resolver` is already wired by the gateway lifespan (per IN-07) so the router refactor is a one-import-swap.
8. **`oauth_install_states` sweep mechanism** — periodic worker, gateway lifespan task, or read-time TTL filter only?

→ Resolved in `research.md` (next file written this turn).

**Output**: research.md with all NEEDS CLARIFICATION resolved.

## Phase 1 — Design & Contracts

Phase 1 produces three Phase-1 artifacts: `data-model.md`, `contracts/`, `quickstart.md`. Plus updates `CLAUDE.md` between the SPECKIT markers (if those markers exist) to reference this plan.

### Data model

Three new tables, all tenant-scoped. Details in `data-model.md`:

- **`encrypted_secrets`** — generic envelope-encrypted row store. `id UUID PK (uuid7)`, `tenant_id UUID FK`, `label TEXT`, `ciphertext BYTEA NOT NULL`, `created_at`, `rotated_at`. Tenant-prefixed index on `(tenant_id, id)`.
- **`oauth_install_states`** — single-use nonce ledger. `id UUID PK (uuid7)`, `tenant_id UUID FK`, `nonce TEXT NOT NULL UNIQUE`, `provider TEXT NOT NULL`, `expires_at TIMESTAMPTZ NOT NULL`, `consumed_at TIMESTAMPTZ NULL`, `created_at`. Tenant-prefixed index on `(tenant_id, expires_at)`.
- **`installation_audit_log`** — installation-lifecycle audit. `id UUID PK (uuid7)`, `tenant_id UUID FK`, `installation_row_id UUID NULL` (NULL for collision-rejected installs that didn't create a row), `provider TEXT NOT NULL`, `action TEXT NOT NULL CHECK (action IN ('install','uninstall','token_refresh','rejected_collision'))`, `status TEXT NOT NULL CHECK (status IN ('ok','rejected_collision','error'))`, `context JSONB NOT NULL DEFAULT '{}'::jsonb`, `created_at`. Tenant-prefixed index on `(tenant_id, created_at DESC)`.

### Contracts

Three contracts under `contracts/`:

- **`http-integrations-slack.md`** — `GET /integrations/slack/install` (Bearer-auth, 302 to Slack), `GET /integrations/slack/callback` (public + state-token-auth, 302 to Fyralis UI). Request/response/error contracts.
- **`http-webhooks-slack-events.md`** — extension of the existing IN-06 contract to recognize `app_uninstalled` / `tokens_revoked` event types and route them to `services/integrations/slack/uninstall.py`. Reuses the existing IN-06 signature verification.
- **`module-secret-store.md`** — `lib/shared/secrets` Python API: `SecretStore` Protocol with `put`/`get`/`rotate`/`delete`, error model, and the `build_secret_store(pool, master_kek_loader)` factory.

### Quickstart

`quickstart.md` walks a developer through:

1. Setting `MASTER_KEK` and `SLACK_CLIENT_ID` / `SLACK_CLIENT_SECRET` in `.env`.
2. Running migrations (`psql … -f db/migrations/0040_slack_installation_tokens.sql -f db/migrations/0041_installation_audit_log.sql`).
3. Hitting `GET /integrations/slack/install` with a Bearer token to obtain the Slack consent URL.
4. Following the Slack consent flow against a Slack dev workspace.
5. Verifying the install lands a `provider_installations` row, an `installation_audit_log` row, and `encrypted_secrets` rows.
6. Posting a Slack message in the connected workspace and watching it land as an `Observation` under the session tenant.
7. Triggering `app_uninstalled` via the Slack admin UI and verifying the next webhook → 401 `unknown_installation`.

### Agent context update

After Phase 1, this plan path is wired into the `<!-- SPECKIT START -->` / `<!-- SPECKIT END -->` block in `CLAUDE.md` (if those markers exist) so subsequent agent invocations pick up the IN-08 design context automatically.

## Phase 2 — Re-evaluate Constitution Check

After the Phase 1 artifacts are written, re-evaluate:

- §I — no new Foundation introduced by data-model: confirmed.
- §II — migrations confirmed idempotent against an existing DB.
- §III — every new table has FK + RLS + tenant-prefixed index: confirmed by data-model.md.
- §VII — `installation_audit_log` schema clearly separated from `audit_events`; row IDs `uuid7()`.
- §VIII — three new `CompanyOSError` subclasses identified in contracts/.

If any artifact reveals a NON-NEGOTIABLE violation, that's a stop-and-revise event — Phase 2 ends with explicit re-pass.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _(none)_ | _(no violations identified)_ | _(no simpler alternative needed)_ |

No constitution violations identified. The closest dual-use of an abstraction is the `SecretStore` Protocol — justified per §X because there's a real second backend (KMS) on the roadmap and the existing `Embedder` and `LLMProvider` patterns set the bar; this isn't speculative.
