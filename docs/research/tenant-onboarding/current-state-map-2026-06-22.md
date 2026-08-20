# Tenant onboarding — Fyralis current-state map (codebase reconnaissance)

> Generated 2026-06-22 for [tenant-onboarding-ui spec](../../plans/tenant-onboarding-ui.md).
> Output of a 5-agent read-only reconnaissance (tenant model / auth-identity / ingestion-install /
> frontend / onboarding-data-plane), synthesized and spot-verified against the codebase.
> This tree is `exclude_docs`'d from the MkDocs build.

All claims verified against the codebase. Here is the synthesized brief.

# Fyralis Current-State Map — Grounding the Tenant-Onboarding-UI Spec

> Synthesis of 5 subsystem reconnaissance reports, spot-verified against the codebase on branch `feat/signal-source-synthetic-precheck`. Verified facts are stated plainly; inferred or unconfirmed items are flagged.

## 1. Tenant model today

A **tenant is a UUID row in the `tenants` table** (`db/migrations/0023_demo_infrastructure.sql:22`): `id, name, is_demo BOOL, demo_config_id UUID, created_at, archived_at` (nullable, used as soft-delete marker). The table started life as **demo infrastructure** — there is no first-class "company account" concept beyond this row.

- **Creation today is ad-hoc / non-UI.** Sandbox provisioning is a script: `scripts/sandbox_seed_tenant.py` (INSERT `tenants` + a CEO actor, idempotent). Tests use `conftest.py:_seed_test_baseline()` plus an auto-register trigger on tenant-scoped tables. **No HTTP provisioning endpoint** (`POST /tenants`) exists.
- **Multi-tenancy = PostgreSQL RLS.** `db/migrations/0036_rls_permissive_default.sql` enables RLS on tenant-scoped tables with a permissive predicate: `app.current_tenant IS NULL OR tenant_id = current_setting('app.current_tenant')::uuid`. Note: NULL `app.current_tenant` **bypasses** the filter (superuser/unset context sees all rows) — relevant for any admin/onboarding console.
- **Tenant binding mechanism:** `lib/shared/tenant_context.py` — `tenant_transaction()` async context manager + `bind_tenant()` issue `SET LOCAL app.current_tenant` for the transaction lifetime. Repos run inside this.
- **Per-tenant feature flags:** `tenant_flags` table (`db/migrations/0061_tenant_flags.sql`), notably `ingestion.kafka_path_enabled` (kill-switch; default-on inverted). `TenantOnboardingOrchestrator` enables this flag at onboarding start.
- **`tenant_id` is denormalized through the whole pipeline** (NormalizedEnvelope → ObservationWriter → `observations` rows), so isolation survives the ingest path.

## 2. Auth & identity today — **THE BIG GAP CALL**

**There is NO end-user login. None.** The system is **API/agent-only**. What exists is a backend bearer-token session minter, not a human auth surface.

- **What exists:** `services/app/gateway/auth.py` — `create_session(pool, actor_id, tenant_id, ttl) → (token, AuthContext)`, `validate_token()`, `revoke_session()`. Tokens are opaque UUID-v7, SHA-256 hashed at rest (`actor_sessions`, `db/migrations/0003_actor_sessions.sql`), 24h default TTL. Enforced by `BearerAuthMiddleware` (`services/app/gateway/middleware.py`) with a public-path allowlist. The only mint endpoint is `POST /auth/session` (`core_router.py:116`), which takes `actor_id`/`tenant_id` directly and is guarded by a bootstrap secret in prod — **it is a backend bootstrap, not a login.**
- **Actor model:** `actors` table (`db/migrations/0001_foundation.sql`) with `ActorType = human_internal | human_external | ai_agent` and `actor_identity_mappings` (`channel:ref → actor_id`, used for ingestion de-dup, not login). Managed via `services/domain/actors/repo.py:ActorRepo`.
- **RBAC exists but is internal:** `actor_roles` (`db/migrations/0014_access_control.sql`) — verified 7 roles `{owner, contributor, viewer, admin, finance, legal, leadership}` over two scopes (tenant-wide where `entity_id IS NULL`; entity-scoped `{goal, commitment, decision, resource}`). API: `services/platform/access_control/roles.py` (`grant_role/revoke_role/has_role`, with tenant-wide admin/leadership override). **No user-facing roles API** (`GET /me/roles`).
- **GREENFIELD (explicit):** no `/auth/login`, `/signup`, `/forgot-password`; no password store; **no SSO/OIDC/SAML for humans** (all OAuth in the repo is *source-connection* OAuth, not user login); **no SCIM / directory sync**; no user-invitation flow; no `email:tenant` uniqueness constraint; no session-browser UI; no auth-event audit. For enterprise org-aware onboarding, **the entire human-identity layer must be built new.**

## 3. Source-connection machinery (the "connect sources" seams)

This is the most built-out area. **26 sources** confirmed in `services/ingest/ingestion/raw_tier/envelope.py:SourceLiteral`: slack, github, discord, gmail, notion, google_calendar, google_drive, jira, mercury, quickbooks, grafana, telegram, brex, ramp, gusto, deel, fireflies, signal, aws, miro, figma, carta, hibob, ashby, linkedin, **whatsapp** (whatsapp merged to main; migrations renumbered to `0144/0145`).

**Four install archetypes:**
1. **OAuth 2.0, short-lived token** — slack, github, discord, gmail, notion, google_calendar/drive, jira, miro, figma, linkedin.
2. **OAuth + token rotation/re-mint** — quickbooks, ramp, gusto, carta (`services/ingest/integrations/oauth_refresh.py:REFRESH_CONFIGS`).
3. **API-token / service-account / webhook** — mercury, brex, deel, fireflies, signal, aws, grafana, hibob, ashby.
4. **Gateway / MTProto / WebSocket** — telegram, whatsapp.

**Verified seam inventory:**
- **HTTP install/callback routes exist for only 4 providers:** `services/ingest/integrations/router.py` wires GET `/integrations/{slack,discord,github,notion}/install|callback`. The other ~22 sources have **no HTTP install route** — they onboard via per-source `onboarding.py:finalize_install()` (DB UPSERT + shards + trigger), reachable today only by scripts. WhatsApp has its own router (`services/app/gateway/whatsapp_router.py`).
- **Secret store:** `lib/shared/secrets/store.py:FernetSecretStore` (MultiFernet rotation, per-install `secret_ref` UUID, tenant-scoped `put/get`).
- **Webhook ingress:** `POST /webhooks/{provider}` (`services/app/webhooks/router.py`) → `TenantResolver.resolve()` (`services/app/webhooks/tenant_resolver.py`, ~19 provider-native ID extractors + LRU cache, Ashby per-install subpath) → signature verify (`signatures.py:VERIFIERS`) → inline `ingest()`.
- **Registries/tables:** `provider_installations` (`db/migrations/0050`, the webhook routing registry), per-source `{source}_installations` tables, `installation_audit_log` (`db/migrations/0052`), `onboarding_triggers` outbox (`db/migrations/0058`).
- **Status/health:** **no HTTP status endpoint** — installation/health is queryable only by DB inspection today.

## 4. Existing UI surface

**The onboarding UI is greenfield, but not a blank platform — it would sit on a gatewayed FastAPI backend with an existing (separate) React/Vite SPA pattern.**

- **No UI ships in `fyraliscore`.** Verified: no `services/app/gateway/static` and no `ui/` in core. The SPA lives in the **separate overlay repo `../fyraliscore-demo/ui/src`** (confirmed present): React + Vite, `AutoDemoSession.tsx` bootstraps a hardcoded **demo (Pelago) tenant** via a public `POST /v1/demo/sessions/start`, then renders 4 product pages (Today/Model/Forecasts/Ledger) + a debug panel. This is a **demo harness, not an account-onboarding flow** — there is no tenant creation, account settings, source wizard, or member management.
- **Backend surfaces that exist** (`services/app/gateway/route_mounts.py`): core (auth/ingest/health/metrics), product APIs (`ask`, recommendations, decision-deltas, forecasts, history, model-trace), a WS realtime `/stream` (Postgres LISTEN), and **developer-only panels** — `finance_router.py` (install/backfill/live/status for finance sources) and `debug_router.py` (raw DB JSON). The finance/debug panels are the closest thing to a connection console but are dev tools, not user UX.
- **Extension seam to mount new UI/routes:** `services/app/gateway/extensions.py:mount_extension_routers` (entry-point group `company_os.gateway_extensions`) — a real plug point for an onboarding router.

## 5. Onboarding → ingestion data plane (what fires + what's surfacable)

**The orchestration chain is fully built; the observability is DB-poll-shaped, not push-clean.**

Sequence: OAuth/install poller emits `onboarding_run_created` → `TenantOnboardingOrchestrator` (`workflows/tenant_onboarding.py`) loads active installations and fans out to per-source `source_onboarding_runs`, emitting `source_onboarding_requested` → `SourceOnboarding` (`workflows/source_onboarding.py`) calls `PLANNER_DISPATCH[source]`, INSERTs `onboarding_shards`, emits `shard_fetch_requested` → `ShardFetch` (`workflows/shard_fetch.py`) runs `FETCHER_DISPATCH`, writes S3 + publishes `ingestion.raw` (N1 cursor barrier) → normalizer → `ingestion.normalized` → `ObservationWriter` (`writers/observation_writer.py`) writes `observations` (gated by `kafka_path_enabled`; backfill exempt) → `Reconciler` (`workflows/reconciler.py`) gap-detects, re-shares or emits `source_onboarding_completed`.

**Surfacable progress signals:**
- **Progress events** (`progress/events.py`, verified classes): `TenantOnboardingStarted`, `SourceOnboardingStarted`, `ShardFetched`, `SourceOnboardingComplete`, `TenantOnboardingComplete`, plus `TenantOnboardingBehindSchedule` / `SourceOnboardingFeelsOnboarded`. Published to the `onboarding.progress` Kafka topic by `progress/publisher.py`. **No UI consumer exists.**
- **Pollable tables:** `source_onboarding_runs` (status/started_at/completed_at/failure_reason/reconciled_at/reconciliation_pass_count), `onboarding_shards` (state/observations_seen/pages_fetched/last_error/parent_shard_id), `workflow_states` (`workflow_kind='shard_fetch'` → JSON cursor/pages_fetched/records_fetched), `observations` (COUNT / MIN-MAX occurred_at).
- **Gaps:** ETA is coarse (`ETA_MINUTES_PER_SOURCE = 5`); events fire at shard/source completion, not per-page (UI must poll for intermediate state); no "first-data-landed" wall-clock event; failure context is a single `failure_reason` text field; per-source health must be inferred by joining tables; throughput must be summed across shards client-side.

## 6. Integration seams table

| Onboarding UI step | Existing primitive to call (file:symbol / endpoint / table) | Gap |
|---|---|---|
| Create company / tenant | `tenants` table; `scripts/sandbox_seed_tenant.py` (script only) | **No HTTP `POST /tenants`** — greenfield |
| Create installer/admin account | `services/domain/actors/repo.py:ActorRepo.create_actor()` | No signup/invite endpoint; no `email:tenant` uniqueness — greenfield |
| User logs in | `services/app/gateway/auth.py:create_session()` (backend mint, needs `actor_id`) + `BearerAuthMiddleware` | **No `/auth/login`, no SSO/OIDC/SAML, no password** — fully greenfield |
| Set up roles / invite members | `services/platform/access_control/roles.py:grant_role()`; `actor_roles` table | No user-facing roles API, no invite flow — greenfield |
| List connectable sources | `services/ingest/ingestion/raw_tier/envelope.py:SourceLiteral` (26 sources, code only) | **No `GET /integrations/sources` catalog endpoint** — greenfield |
| Start OAuth source connect | `services/ingest/integrations/router.py` GET `/integrations/{slack,discord,github,notion}/install`; per-provider `oauth.py:install_handler` | Only 4 of 26 have HTTP install routes — greenfield for the rest |
| OAuth callback / token capture | `/integrations/{provider}/callback`; `oauth.py:callback_handler` → `FernetSecretStore.put()` → `provider_installations` UPSERT + `onboarding_triggers` | Redirect lands on static `/integrations/{source}/installed` stub; no rich success/error UX |
| Connect API-token / gateway source | per-source `integrations/{source}/onboarding.py:finalize_install()` | **No HTTP register endpoint or API-key entry form** (script-only) — greenfield |
| Store / rotate credentials | `lib/shared/secrets/store.py:FernetSecretStore`; `services/ingest/integrations/oauth_refresh.py:refresh_access_token()` | No rotation-visibility / consent UI |
| Configure webhook ingress | `POST /webhooks/{provider}` (`services/app/webhooks/router.py`); `TenantResolver`; `provider_installations` | No webhook test/verify UI (partial `/debug/*` only) |
| Trigger backfill after connect | `onboarding_triggers` outbox → `TenantOnboardingOrchestrator.tick()` | No HTTP "start/re-sync" endpoint (manual_replay via DB) — greenfield |
| Show onboarding progress | `onboarding.progress` Kafka topic; `source_onboarding_runs`, `onboarding_shards`, `workflow_states` tables | **No progress endpoint / WS / SSE subscription** — greenfield |
| Show data landed | `observations` table `COUNT/MIN/MAX(occurred_at)` per tenant+source | No real-time count endpoint; no "first-data-landed" event — greenfield |
| Source health / errors | `source_onboarding_runs.failure_reason`, `onboarding_shards.last_error`, DLQ | No health endpoint; failure rates only in app logs — greenfield |
| Disconnect / uninstall source | DB `UPDATE provider_installations.enabled=false` (+ provider uninstall webhook) | No centralized uninstall handler / UI — greenfield |
| Toggle ingestion (kill-switch) | `tenant_flags` table, `ingestion.kafka_path_enabled` | No flag-management UI — greenfield |

## 7. Top greenfield gaps (ranked) for enterprise-grade hybrid org-aware onboarding

1. **Human authentication layer (highest priority).** There is no login at all — only a backend `actor_id`-keyed token mint. Enterprise onboarding requires end-user login + **SSO (OIDC/SAML)** and ideally **SCIM/directory sync**. This is the single largest build and the foundation for everything org-aware. (`auth.py` gives token primitives to build on; identity-provider integration is entirely new.)
2. **Tenant/company provisioning + first-run flow.** `tenants` is a demo-origin table with ad-hoc creation. Need `POST /tenants`, owner-actor seeding, the org profile, and lifecycle (archival via existing `archived_at`).
3. **Member invitation + RBAC management UI.** The 7-role `actor_roles` engine exists but has no invite flow, no user-facing roles API, and no `email:tenant` uniqueness — all needed for org-aware multi-user setup.
4. **Source catalog + unified connect API.** Build `GET /integrations/sources` (archetype, scopes, status, consent text) and extend HTTP install beyond the 4 OAuth providers to all 26 — including **API-key entry forms** (9 token sources) and **gateway credential capture** (Telegram/WhatsApp), which are script-only today.
5. **Onboarding progress + health surface.** Consume the already-emitted `onboarding.progress` events (Kafka) via a WS/SSE endpoint, plus REST reads over `source_onboarding_runs` / `onboarding_shards` / `observations`. The data plane is built; the read/stream API and any meaningful ETA are not.
6. **Connection management (post-connect lifecycle).** Centralized uninstall handler, re-sync trigger endpoint, token-rotation visibility, and a per-tenant flag console (`kafka_path_enabled`). All are DB-mutation-only today.
7. **Onboarding UI app itself.** No UI in core; the only SPA is a demo overlay in a separate repo wired to one hardcoded tenant. A real onboarding app can mount through the existing `services/app/gateway/extensions.py` router seam but is otherwise greenfield.

**Net:** the *ingestion/connect/data-plane machinery is largely built and reusable* (orchestrator, secret store, webhook resolver, progress events, 26 source connectors); the *human-facing platform — login, SSO, tenant provisioning, member/RBAC management, source catalog, and onboarding UI/progress API — is greenfield.* The hardest and most foundational gap is **end-user authentication and identity**, which does not exist in any form today.

Relevant files: `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/app/gateway/auth.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/app/gateway/core_router.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/app/gateway/extensions.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/ingest/integrations/router.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/app/webhooks/tenant_resolver.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/ingest/ingestion/workflows/tenant_onboarding.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/services/ingest/ingestion/progress/events.py`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/db/migrations/0023_demo_infrastructure.sql`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/db/migrations/0014_access_control.sql`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/db/migrations/0050_provider_installations.sql`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore/db/migrations/0066_source_onboarding_runs.sql`, `/home/prajwal-adhikari/Desktop/v2/fyraliscore-demo/ui/src/`.