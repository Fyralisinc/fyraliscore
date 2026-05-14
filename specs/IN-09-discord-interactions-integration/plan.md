# Implementation Plan: Discord Production Integration — Interactions HTTP, OAuth Install, Slash Command Self-Serve

**Branch**: `feat/IN-09-discord-interactions-integration` | **Date**: 2026-05-14 | **Spec**: [./spec.md](./spec.md)
**Input**: Feature specification from [./spec.md](./spec.md)

## Summary

After IN-08 closed the Slack production integration with a generic, reusable substrate (`lib/shared/secrets`, `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, the `build_integrations_router()` factory), IN-09 extends that substrate to Discord — the second OAuth-installing provider — by adding:

- A Discord-side OAuth install/callback path under `/integrations/discord/*` mounted into the existing `services/integrations/router.py` factory.
- A repurposed and pinned ingestion handler at `services/ingestion/handlers/discord.py` that emits Observations with `source_channel='discord:interaction'` (renamed from the current generic `discord:webhook`), `content.text=<option value verbatim>` (per Clarifications Q3), and the per-interaction `token` field stripped from content before persistence.
- An outbound Discord REST client at `services/integrations/discord/client.py` that resolves per-installation bot tokens from the secret store, honours Discord's `Retry-After` / `X-RateLimit-Remaining`, and is the single chokepoint where 401/403-code-50001 trigger an idempotent bot-kick disable (FR-015 / Clarifications Q1).
- A one-shot slash-command registration helper that POSTs the `/fyralis ask` command spec to Discord on every successful OAuth callback (Clarifications Q2: POST upsert, not PUT bulk overwrite).

**Zero new migrations.** The existing unique index `observations_source_channel_external_id_occurred_at_key` on `(source_channel, external_id, occurred_at)` enforces FR-002's interaction-id idempotency by construction. `encrypted_secrets`, `oauth_install_states`, `installation_audit_log` are reused verbatim. The only schema-adjacent change is a new label convention in `encrypted_secrets`: `discord_bot_token:<guild_id>` and `discord_public_key:<guild_id>`. Label conventions are application-layer, not DDL.

**Zero changes to `services/integrations/slack/*`** (FR-016 / SC-009 — verified by re-running the IN-08 suite as part of IN-09 CI).

The OAuth flow mirrors IN-08's exactly: signed state token bound to the authenticated tenant via HMAC over `{tenant_id, nonce, expiry_ts}` with single-use enforcement via atomic `UPDATE oauth_install_states ... WHERE consumed_at IS NULL RETURNING ...`. The cross-tenant collision detection (US2.4) reuses the `ON CONFLICT ... WHERE tenant_id = EXCLUDED.tenant_id` shape from IN-08's Slack callback. The bot-kick chokepoint (US3) is a direct port of `services/integrations/slack/uninstall.py::_disable_and_zeroize`, named `_disable_and_zeroize_discord` and triggered from the outbound REST client rather than from a webhook event (the architectural distinction Discord forces).

## Technical Context

**Language/Version**: Python 3.11+ (project uses 3.12 in `.venv`).
**Primary Dependencies**:
- **PyNaCl** (`nacl.signing.VerifyKey`) — Ed25519 verification. **Confirmed present** in `services/webhooks/signatures/discord.py` via lazy `_import_nacl()`; no new dependency needed.
- **httpx** — async HTTP client for Discord REST + OAuth (already in project for Slack).
- **asyncpg** — DB driver, factory-injected via `request.app.state.pool`.
- **cryptography.fernet** — already in `lib/shared/secrets/` from IN-08; reused for envelope encryption of bot tokens and the (per-installation mirror of the) application public key.
- **FastAPI** factory routers — `APIRouter` extended within `build_integrations_router()`.

**Storage**: Postgres 16 + pgvector — reused tables only (see Data Model).

**Testing**: pytest with `integration` marker (live Postgres + Ollama per Constitution §IV). `respx` for mocking `discord.com/api` HTTP calls in unit and integration tests. No mocking of the Postgres or `lib/shared/secrets` boundary.

**Target Platform**: Linux server (docker-compose deploy).

**Project Type**: Web service (backend-only for this task; UI is separate).

**Performance Goals**:
- US2 OAuth callback wall time ≤ 1.5 s under live Discord token exchange (mocked to ≤ 100 ms in tests).
- US1 interaction ingest ≤ 3 s (Discord's hard ack timeout) — handler must complete in ≤ 500 ms even under cold-cache conditions.
- Outbound chokepoint (US3) — disable + delete + audit ≤ 50 ms (no row lock, no extra round-trip).

**Constraints**:
- Discord's 3-second ack window for interaction responses is a hard deadline; if Observation commit is at risk of breaching, return `{"type": 5}` (DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE) and complete the commit after the deferred ack. Plan keeps the default response shape minimal so we land well under 3s.
- Bot token at rest envelope-encrypted via the same `MASTER_KEK` IN-08 provisioned; key rotation is out of scope (deferred to operational follow-up).
- `FYRALIS_ENV=prod` + `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` MUST fail-fast at startup (FR-020 — already enforced by `assert_prod_safety_invariants()` from IN-08; verify it continues to apply).

**Scale/Scope**: Per-tenant Discord installs are expected to number in the low tens during the lifetime of this task. The bottleneck for IN-09 is correctness, not throughput — every Discord-tier rate limit applies per-bot, well above expected load.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-evaluated at end of Phase 1.*

| Principle | Status | Notes |
|---|---|---|
| §I Four Foundations distinct | PASS | Discord interactions land as **Observations** with `kind='signal'`, `trust_tier='attested_agent'`. No Model / Act / Resource writes in this task. `discord_application` / `discord_guild` / `discord_channel` entity hints in `entities_mentioned` for downstream entity-alias resolution — these are hints, not foundation rows. |
| §II Append-only migrations | PASS | **ZERO new migrations.** Existing tables `encrypted_secrets` (0040), `oauth_install_states` (0040), `installation_audit_log` (0041), `provider_installations` (0039) are reused. The dedup unique index on `observations(source_channel, external_id, occurred_at)` already enforces FR-002. |
| §III Tenant isolation structural | PASS | No new tables. `encrypted_secrets` rows for Discord carry `tenant_id` via existing column + RLS + index. `provider_installations` rows for Discord carry `tenant_id` via existing FK. All new queries written with hand-rolled `WHERE tenant_id = $1`. |
| §IV Integration tests, real DB | PASS | Plan mandates live Postgres for all `services/integrations/discord/tests/test_*.py` files; `respx` only for `discord.com/api` HTTP mocks. The `lib/shared/secrets/` boundary is real Fernet via real Postgres. |
| §V Reasoning vs rendering | N/A | This task is integration plumbing — no Think or Rendering changes. The Observations produced will trigger Think downstream via the existing `think_trigger_queue` plumbing in `services/ingestion/`. |
| §VI Trust/confidence/falsifiers | PASS | Observations from Discord interactions carry `trust_tier='attested_agent'` (consistent with Slack messages); no Model writes, so no falsifier obligations. |
| §VII Determinism + audit | PASS | Every install / uninstall / token_refresh writes an `installation_audit_log` row. `uuid7()` for every new substrate row (Observations, encrypted_secrets, oauth_install_states, audit rows). Interaction-id dedup via existing UNIQUE index. |
| §VIII Structured errors | PASS | New exception classes derive from existing IN-08 hierarchy: `DiscordOAuthError`, `DiscordApiError` (both `code`/`message`/`context`). Existing `InstallationCollisionError`, `StateTokenInvalidError`, `SecretStoreError`, `SecretNotFoundError` are reused unchanged. |
| §IX Dual-write until proven | PASS — N/A | No substrate-shape change in this task; nothing to dual-write. The DB-backed secret-store path was already proven live by IN-08; IN-09 is a second consumer of that proven path. |
| §X Simplicity / YAGNI | PASS | Single global slash command. No per-guild customisation UI. No KMS plug. No Gateway WS. No periodic reconciliation job. Each item is explicitly deferred to a follow-up task with rationale in the spec's Out-of-Scope. |

**No NON-NEGOTIABLE violations.** Complexity Tracking table below is empty.

### Complexity Tracking

(none — no deviations from the constitution require justification)

## Project Structure

### Documentation (this feature)

```text
specs/IN-09-discord-interactions-integration/
├── plan.md              # This file
├── research.md          # Phase 0: dependency confirmations, mechanism choices
├── data-model.md        # Phase 1: entities, label conventions, no new DDL
├── quickstart.md        # Phase 1: end-to-end install + invocation flow
├── contracts/           # Phase 1: HTTP route specs + module contracts
│   ├── http-integrations-discord.md
│   ├── http-webhooks-discord-events.md
│   └── module-discord-client.md
├── source.md
└── tasks.md             # Phase 2: produced by /speckit-tasks
```

### Source Code (repository root)

New files (all under `services/integrations/discord/`):

```text
services/integrations/discord/
├── __init__.py
├── oauth.py            # GET install / GET callback handlers + state-token + cross-tenant collision
├── commands.py         # POST /applications/{app_id}/commands wrapper (slash-command registration)
├── client.py           # async outbound Discord REST client (chokepoint for 401 → disable)
├── uninstall.py        # _disable_and_zeroize_discord (port of IN-08 Slack uninstall)
└── metrics.py          # discord_install_outcomes_total / discord_uninstall_outcomes_total counters
```

Plus the colocated tests:

```text
services/integrations/tests/
├── conftest.py                       # already provides autouse MASTER_KEK (IN-08)
├── test_oauth_install_discord.py     # state-token mint + 302 + Bearer auth
├── test_oauth_callback_discord.py    # full callback + collision + state failures + slash-command POST recorded
├── test_uninstall_discord.py         # chokepoint: 401 → disable + idempotent under race
├── test_client_discord.py            # 429 backoff + 401 chokepoint + bounded budget
└── test_ingest_discord.py            # interaction → Observation with stripped token, content.text=option, source_channel='discord:interaction'
```

Changed files:

```text
services/integrations/router.py        # add /integrations/discord/install + /callback sub-routes
services/gateway/main.py               # add /integrations/discord/callback to _PUBLIC_PATHS exact-match set
services/ingestion/handlers/discord.py # rewrite source_channel + content.text + token-strip
services/webhooks/router.py            # ZERO logic changes; regression test for PING short-circuit ordering
services/webhooks/signatures/discord.py # accept per-installation public key from secret store; env fallback for PING
services/webhooks/tenant_resolver.py   # regression test only — _extract_discord already extracts guild_id correctly
lib/shared/errors.py                   # add DiscordOAuthError, DiscordApiError
pyproject.toml                         # NO change; pynacl already vendored via webhook verifier
CODEBASE-ARCHITECTURE.md               # append §15 documenting IN-09 (mirror §14 IN-08 shape)
```

**Structure Decision**: Mirror IN-08's directory layout under `services/integrations/discord/`. Tests live in `services/integrations/tests/` (shared conftest). No top-level reorg; everything slots into the prov-namespaced shape IN-08 established.

## Phase Ordering (per Constitution §IX)

Constitution §IX mandates migrations → dual-write → reader cutover for substrate-shape changes. IN-09 has **no substrate-shape changes** — zero migrations, zero new write paths against existing data. So the phase order collapses to functionality-first:

**Slice 1 (foundational, no gate)** — confirm reusable substrate is intact:
- T001: Verify `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `provider_installations` exist and have the expected columns / RLS / indexes from IN-08 migrations 0039 / 0040 / 0041. Pure read-only assertion.
- T002: Verify `observations_source_channel_external_id_occurred_at_key` UNIQUE index exists. If absent for any reason (test fixture drift), surface immediately — but DO NOT create a new migration; instead route to a remediation task in the spec phase.
- T003: Verify PyNaCl import works in a fresh venv (regression: catch a sibling project removing the dep).

**Slice 2 (US1: Ingestion contract)** — interactions land as Observations:
- T004: Rewrite `services/ingestion/handlers/discord.py` to use `source_channel='discord:interaction'`, `content.text = <primary string option value>`, `content.metadata = payload \ {token, member.user.token if present}`, `external_id = f"discord:{interaction.id}"`. The existing entities_hint stays.
- T005: `test_ingest_discord.py::test_interaction_lands_as_observation` (integration: real DB).
- T006: `test_ingest_discord.py::test_duplicate_interaction_id_is_idempotent` (integration: relies on the existing unique index — the handler must catch `UniqueViolationError` and treat as success).
- T007: `test_ingest_discord.py::test_token_stripped_from_content_metadata` (integration: assert content does not contain the literal token string).

**Slice 3 (US2 + US4: OAuth install + slash-command registration)** — self-serve onboarding:
- T008: Create `services/integrations/discord/__init__.py`, `oauth.py` (install + callback handlers), `commands.py` (slash-command POST wrapper). Implement state-token mint via the existing `services.integrations.slack.oauth.issue_state_token` helper (which is generic since IN-08; if its name reads Slack-specific, rename to `services.integrations.oauth_state.issue_state_token` as part of T008 with a thin compatibility shim in slack/oauth.py to keep IN-08 tests green).
- T009: Mount `/integrations/discord/install` (Bearer-authed) + `/integrations/discord/callback` (public) in `services/integrations/router.py`.
- T010: Add `/integrations/discord/callback` to `_PUBLIC_PATHS` in `services/gateway/main.py` as an exact-match path.
- T011: `test_oauth_install_discord.py`: 302 to discord.com/oauth2/authorize with right client_id, scopes (`applications.commands+bot`), permissions, redirect_uri, state token.
- T012: `test_oauth_callback_discord.py::test_first_install`: end-to-end with respx-mocked Discord token exchange → installation row + encrypted bot token + encrypted public key + audit row + slash-command POST recorded + 302 to success page.
- T013: `test_oauth_callback_discord.py::test_state_token_failures`: expired / invalid / consumed each routes to `/integrations/discord/install-error?reason=...`.
- T014: `test_oauth_callback_discord.py::test_cross_tenant_collision`: installation_collision audit row + redirect; no log line containing the foreign tenant id.
- T015: `test_oauth_callback_discord.py::test_command_registration_failure_does_not_block_install`: Discord 4xx on command POST → audit row `status='error'` with code in context, installation row still written.

**Slice 4 (US3: Bot-kick chokepoint)** — outbound 401 → disable:
- T016: Create `services/integrations/discord/uninstall.py::_disable_and_zeroize_discord` — port of Slack's `_disable_and_zeroize` with Discord-specific resolver-cache invalidation. Lock-free per Clarifications Q1.
- T017: Wire the chokepoint into `services/integrations/discord/client.py` (Slice 5).
- T018: `test_uninstall_discord.py::test_401_disables_installation_and_zeroes_token` (integration).
- T019: `test_uninstall_discord.py::test_concurrent_401s_are_idempotent` (integration: two awaitables racing; both complete; final state is enabled=FALSE; secret deleted; ≤ 2 audit rows; no exception).
- T020: `test_uninstall_discord.py::test_disabled_installation_rejects_next_inbound` (integration: post-disable, next signed interaction returns 401 unknown_installation).

**Slice 5 (US5: Outbound REST client)** — Discord API wrapper:
- T021: Create `services/integrations/discord/client.py::DiscordClient` with `post_followup_message`, `get_guild_member`, `get_channel`. Per-call bot-token resolution via `secret_store.get(label=f"discord_bot_token:{guild_id}", tenant_id=...)`.
- T022: Add `Retry-After` / `X-RateLimit-Remaining` handling with budget: ≤ 3 attempts, ≤ 30 s wall, exponential backoff capped at the header value.
- T023: Add 401/403-code-50001 chokepoint trigger; raises `DiscordApiError` to caller.
- T024: `test_client_discord.py::test_429_retry_within_budget`.
- T025: `test_client_discord.py::test_401_triggers_chokepoint_once`.
- T026: `test_client_discord.py::test_orphaned_secret_ref_raises`.

**Slice 6 (Polish)** — final integration + docs:
- T027: `services/integrations/discord/metrics.py` (counter family `discord_install_outcomes_total{outcome}`).
- T028: Update `CODEBASE-ARCHITECTURE.md` §15 documenting IN-09 (mirror §14 IN-08 shape).
- T029: `services/webhooks/tests/test_verifier_discord_db_backed.py` regression — signed Discord interaction with DB-backed public key in `encrypted_secrets` resolves end-to-end via the IN-08 `load_secrets` path.
- T030: Re-run **full IN-08 test suite** with no test file modifications to satisfy SC-009.

**No reader cutover phase, no dual-write phase, no migration phase — they are not needed.**

## Risk Register

1. **Discord's `oauth2/token` response shape**: Discord returns `guild.id` nested under `guild` (per current docs). Older / non-bot OAuth flows may not include the guild at all. Plan mitigation: read `response["guild"]["id"]` with defensive parsing; if missing AND the `bot` scope was requested, log a structured warning and route the user to `install-error?reason=slack_oauth_error` (reusing the IN-08 error reason code is acceptable — the UI surface treats it as "OAuth provider returned an unexpected shape").
2. **Public key per-installation mirroring**: The application public key is identical across installations (Discord application-level fact). Mirroring it into `encrypted_secrets` per `<guild_id>` means N rows hold the same plaintext. This is intentional — it lets `load_secrets` resolve via DB without provider-specific code. Risk: if the application key rotates in the Developer Portal, every per-installation mirror is stale. Mitigation: rotation is operationally documented as "redeploy `WEBHOOK_SECRET_DISCORD` env + force a sweep of all `discord_public_key:*` labels", out of scope here.
3. **Cold-start latency on first install**: Discord enforces a 3 s ack window for interactions. If the first interaction lands while `secret_store` hasn't cached anything, expect ~1 RTT to Postgres + Fernet decrypt. Plan mitigation: optimistic ack with `{"type": 5}` if the Observation insert hasn't returned at 2.5 s — defer the actual ack content. For v1, skip the optimistic ack and rely on warm DB + sub-100ms insert; revisit if the integration test flakes.
4. **Slack and Discord state-token nonce collision**: Both providers reuse `oauth_install_states`. The `provider` column on that table MUST be set; otherwise a Slack-issued nonce could be replayed against a Discord callback. Plan mitigation: confirmed `oauth_install_states.provider` column exists from IN-08 migration 0040. State-token verification in `oauth.py` checks `provider='discord'` explicitly when consuming the nonce.
5. **Test pollution from leftover Slack installs**: The IN-08 test suite seeds Slack installs into `provider_installations`. If the IN-09 suite runs against the same DB without truncation, cross-tenant collision tests can flake. Plan mitigation: `fresh_db` fixture truncates between tests (Constitution §IV); add a per-test `_unique_guild_id()` factory in `services/integrations/tests/conftest.py` to avoid same-guild-id collisions across tests.

## Open Questions

(none — all resolved in spec Clarifications)
