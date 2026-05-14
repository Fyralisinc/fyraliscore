# Tasks: Discord Production Integration — Interactions HTTP, OAuth Install, Slash Command Self-Serve

**Input**: Design documents from `/specs/IN-09-discord-interactions-integration/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: REQUIRED (Constitution §IV: live Postgres in integration tests).

**Organization**: Phase 4 bundles US2 + US4 because slash-command registration is part of the OAuth callback (plan.md Slice 3). Phase 5 bundles US3 + US5 because the bot-kick chokepoint is *triggered from* the outbound client (plan.md Slices 4 + 5 are co-dependent).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)

## Path Conventions

This is a backend web service. Paths root at the repo root (`/home/prajwal-adhikari/Desktop/v2/fyraliscore/`).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: IN-09 has no cross-cutting setup. All shared infrastructure (FastAPI, asyncpg, structlog, lib/shared/secrets, lib/shared/errors, the integrations router factory) is already in place from IN-08.

(no tasks)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Confirm reused substrate is intact and add the structured-error classes IN-09 will raise. No user story can proceed without these.

- [ ] T001 Verify IN-08 substrate exists by reading `db/migrations/0039_provider_installations.sql`, `0040_slack_installation_tokens.sql`, `0041_installation_audit_log.sql`; confirm `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `provider_installations` are all created with `tenant_id` FK + RLS + tenant-prefixed indexes (no code change — read-only assertion captured as a docstring or comment in `services/integrations/discord/__init__.py` once it exists)
- [ ] T002 Verify `observations` has the UNIQUE index `observations_source_channel_external_id_occurred_at_key` on `(source_channel, external_id, occurred_at)` by running `psql -c "\d observations"` and noting the result; if absent, STOP and surface to user — do NOT create a new migration
- [ ] T003 Verify `pynacl` is importable in the project venv: `python -c "from nacl.signing import VerifyKey"` exits 0; if it fails, STOP and surface — the dep should already be transitive via the existing webhook verifier
- [ ] T004 [P] Add `DiscordOAuthError` and `DiscordApiError` subclasses to `lib/shared/errors.py`. Both inherit from `CompanyOSError`. Add codes: `discord_oauth_token_exchange_failed`, `discord_oauth_missing_guild`, `discord_command_registration_failed`, `discord_api_rate_limited`, `discord_api_unauthorized`, `discord_secret_unavailable`, `discord_api_error`. Reuse existing `InstallationCollisionError`, `StateTokenInvalidError`, `SecretStoreError`, `SecretNotFoundError` unchanged.
- [ ] T005 [P] Create empty package skeleton: `services/integrations/discord/__init__.py` with a module docstring describing the package's role (mirror `services/integrations/slack/__init__.py` in shape).
- [ ] T006 [P] Confirm `oauth_install_states.provider` column exists from IN-08 migration 0040 (read-only `\d oauth_install_states`); document in a comment in `services/integrations/discord/oauth.py` (to be created in Phase 4) that Discord uses `provider='discord'` to disambiguate state tokens from Slack-issued ones.

**Checkpoint**: At end of Phase 2, the dependency surface (errors, substrate, package) is ready. No behaviour change yet.

---

## Phase 3: User Story 1 — Slash-Command Invocations Land as Observations (Priority: P1) 🎯 MVP

**Goal**: A signed Discord `INTERACTION_CREATE` (type=2 ApplicationCommand) arriving at `/webhooks/discord/events` for a guild with a valid `provider_installations` row produces exactly one Observation under the right tenant, with `source_channel='discord:interaction'`, `content.text=<option value verbatim>`, and the per-interaction `token` field stripped from `content.metadata`. Duplicate interaction-ids are idempotent.

**Independent Test**: Seed a `provider_installations` row + encrypted public key + encrypted bot token for a test guild_id, then POST a synthetic signed `INTERACTION_CREATE` (type=2) to `/webhooks/discord/events`. Assert one new Observation row exists with the expected fields, no token in content.metadata, and a second identical POST returns 200 without inserting a second row.

### Implementation

- [ ] T007 [US1] Rewrite `services/ingestion/handlers/discord.py::handle_discord_webhook`: change `_CHANNEL` from `'discord:webhook'` to `'discord:interaction'`; set `external_id = f"discord:{interaction_id}"` (already correct); rewrite `_summary` to extract the primary string option's value verbatim and return it as `content_text`; build `content.metadata` from the full payload minus the `token` field at the top level AND minus `member.user.token` / `user.token` if those exist (defensive); preserve the existing `entities_hint` extraction.
- [ ] T008 [US1] Add catch for `asyncpg.exceptions.UniqueViolationError` raised during the Observation insert (the existing unique index on `(source_channel, external_id, occurred_at)` will raise on duplicate interaction-ids). The handler treats this as idempotent success and returns the original `ObservationDraft` so the router responds 200 to Discord.

### Tests for User Story 1

- [ ] T009 [US1] Create `services/integrations/tests/test_ingest_discord.py::test_interaction_lands_as_observation` (integration, real DB): seed `provider_installations` row + secrets via `secret_store.put`, POST a synthetic signed Discord interaction payload through `services/gateway/main:build_app()` with `httpx.ASGITransport`, assert HTTP 200 AND one Observation row exists with `source_channel='discord:interaction'`, `content_text=<query>`, `external_id='discord:<interaction_id>'`, `trust_tier='attested_agent'`, `source_actor_ref='discord:<user_id>'`.
- [ ] T010 [US1] `test_ingest_discord.py::test_duplicate_interaction_id_is_idempotent` (integration): POST the same payload twice; assert HTTP 200 both times, exactly one Observation row in the DB.
- [ ] T011 [US1] `test_ingest_discord.py::test_token_stripped_from_content_metadata` (integration): include a known-string `token` field in the synthetic payload; after ingest, assert the literal token string does not appear in `content::text` of the persisted Observation.
- [ ] T012 [US1] `test_ingest_discord.py::test_ping_returns_pong_via_router` (integration): POST a type=1 PING with a valid signature (using the env-var public key); assert HTTP 200 with body `{"type": 1}` AND no Observation row is created.

**Checkpoint**: At end of Phase 3, the receive side of Discord interactions is fully wired. The OAuth install flow is still missing (US2), so users still can't onboard themselves — but the ingestion contract is testable end-to-end via the synthetic-POST harness.

---

## Phase 4: User Story 2 + User Story 4 — OAuth Install + Slash-Command Registration (Priority: P1 + P2)

**Goal (US2)**: A Discord server admin can click `/integrations/discord/install` in a Bearer-authenticated session, complete Discord's OAuth consent flow, and be redirected to a Fyralis success page with a `provider_installations` row UPSERTed for their guild.

**Goal (US4)**: As part of the same OAuth callback, the `/fyralis ask` global slash command is registered (POST upsert per Clarifications Q2) so it appears in Discord's command picker.

**Independent Test (US2)**: Mint a Bearer token for a test tenant, GET `/integrations/discord/install`, assert 302 to Discord's consent URL with the right `client_id`, scopes `applications.commands+bot`, redirect URI, and a signed state token. Mock Discord's `oauth2/token` endpoint with `respx`, GET the callback with a valid code + state, assert the install row + audit row + encrypted secrets exist.

**Independent Test (US4)**: From the same US2 happy-path test, assert the respx mock recorded exactly one `POST /applications/{app_id}/commands` call with the correct command spec.

### Implementation

- [ ] T013 [US2] Create `services/integrations/discord/oauth.py`. Implement `short_guild_hash(guild_id: str) -> str` using `blake2b(guild_id.encode(), digest_size=8).hexdigest()` (mirrors IN-08's `short_team_hash`).
- [ ] T014 [US2] In `services/integrations/discord/oauth.py`, implement `issue_state_token(tenant_id: UUID, pool: asyncpg.Pool) -> str`: insert an `oauth_install_states` row with `provider='discord'`, mint an HMAC-SHA256 signature over `{tenant_id, nonce, expires_at}` using `OAUTH_STATE_HMAC_KEY` env var, return the JWT-style `<base64(payload)>.<base64(sig)>` token. (If `services.integrations.slack.oauth.issue_state_token` is already provider-agnostic — it takes a `provider` argument internally — re-export it from `services.integrations.oauth_state` and use that here. Otherwise duplicate the implementation. See research R5.)
- [ ] T015 [US2] In `services/integrations/discord/oauth.py`, implement `verify_and_consume_state(token: str, pool: asyncpg.Pool) -> UUID`: verify HMAC, atomically `UPDATE oauth_install_states SET consumed_at=now() WHERE nonce=$1 AND provider='discord' AND consumed_at IS NULL AND expires_at > now() RETURNING tenant_id`. Raise `StateTokenInvalidError(code='state_invalid'|'state_expired'|'state_consumed')` with the specific reason.
- [ ] T016 [US2] In `services/integrations/discord/oauth.py`, implement `install_handler(request: Request) -> RedirectResponse`: read `request.state.auth.tenant_id`, call `issue_state_token`, construct the Discord OAuth URL with `client_id=os.environ['DISCORD_CLIENT_ID']`, `scope='applications.commands bot'` (space-separated), `permissions=<int from constants>`, `redirect_uri=os.environ['DISCORD_REDIRECT_URI']`, `state=<token>`, `response_type='code'`. Return a 302.
- [ ] T017 [US2] In `services/integrations/discord/oauth.py`, implement `_exchange_code(code: str) -> DiscordTokenResponse`: POST `https://discord.com/api/v10/oauth2/token` with HTTP Basic auth `(DISCORD_CLIENT_ID:DISCORD_CLIENT_SECRET)` and form body `grant_type=authorization_code, code=<code>, redirect_uri=<exact match>`. Defensively parse `guild.id` per research R7. Raise `DiscordOAuthError(code='discord_oauth_token_exchange_failed')` or `code='discord_oauth_missing_guild'` on failures.
- [ ] T018 [US4] Create `services/integrations/discord/commands.py`. Implement `async def register_fyralis_command(application_id: str, bot_token: str, *, http_client: httpx.AsyncClient | None = None) -> dict`: POST `https://discord.com/api/v10/applications/{application_id}/commands` with body `{"name":"fyralis","description":"Ask Fyralis a question about your organization.","type":1,"options":[{"name":"ask","description":"What you want to ask","type":3,"required":true}]}` and `Authorization: Bot <bot_token>`. Return the JSON response. Raise `DiscordOAuthError(code='discord_command_registration_failed', context={'http_status': resp.status_code})` on 4xx (5xx propagates as a generic httpx exception — caller decides).
- [ ] T019 [US2+US4] In `services/integrations/discord/oauth.py`, implement `callback_handler(request: Request) -> RedirectResponse`: parse `code` and `state` query params; call `verify_and_consume_state(state, pool)` → `tenant_id`; call `_exchange_code(code)` → `DiscordTokenResponse`. **THEN, BEFORE writing the new secrets**, call `_cleanup_prior_secrets(pool, secret_store, *, tenant_id, guild_id)` — a helper that SELECTs all `encrypted_secrets.id` rows where `tenant_id=$1 AND label IN ('discord_bot_token:<gid>', 'discord_public_key:<gid>')` and calls `secret_store.delete(ref, tenant_id=tenant_id)` on each, suppressing `SecretNotFoundError`. This satisfies SC-010's "deletes the prior bot-token row from encrypted_secrets if it lingered" contract and prevents the orphan accumulation we hit live during IN-08 dev. Then call `secret_store.put(plaintext=token.access_token, label=f'discord_bot_token:{token.guild_id}', tenant_id=tenant_id)` → `bot_ref`; call `secret_store.put(plaintext=os.environ['WEBHOOK_SECRET_DISCORD'], label=f'discord_public_key:{token.guild_id}', tenant_id=tenant_id)` → `public_key_ref`; UPSERT `provider_installations` with `ON CONFLICT (provider, installation_id) DO UPDATE WHERE provider_installations.tenant_id = EXCLUDED.tenant_id RETURNING id, (xmax = 0) AS inserted`; zero rows = cross-tenant collision → write audit row with `status='rejected_collision'`, redirect to install-error; non-zero rows → call `register_fyralis_command(application_id, token.access_token)`; on registration failure, write audit row with `status='error'` and `context.error_code=<code>`, still redirect to success; on success, write audit row with `status='ok'`, redirect to `/integrations/discord/installed?guild=<short_guild_hash>`.
- [ ] T020 [US2] On any exception path in `callback_handler` (invalid state, OAuth failure, secret store unavailable), redirect to `/integrations/discord/install-error?reason=<code>` per the contract table in `contracts/http-integrations-discord.md`. Never raise to the caller; the redirect IS the error contract.
- [ ] T021 [US2] In `services/integrations/router.py::build_integrations_router`, add `@router.get('/discord/install')` and `@router.get('/discord/callback')` sub-routes pointing at the two handlers from T016 + T019. Match the shape of the existing `slack_install` / `slack_callback` registrations.
- [ ] T022 [US2] In `services/gateway/main.py`, add `/integrations/discord/callback`, `/integrations/discord/installed`, and `/integrations/discord/install-error` to `_PUBLIC_PATHS` as exact-match paths (NOT a prefix). `/integrations/discord/install` stays off the allowlist (Bearer-required).

### Tests for User Story 2 + User Story 4

- [ ] T023 [US2] Create `services/integrations/tests/test_oauth_install_discord.py::test_install_redirects_to_discord_oauth_with_signed_state` (integration): mint a session bearer for a test tenant, GET `/integrations/discord/install` through `httpx.ASGITransport`, assert 302 to `https://discord.com/oauth2/authorize?...` with the right `client_id`, `scope=applications.commands+bot` (urlencoded as `applications.commands%20bot` or `+`), `redirect_uri`, `response_type=code`, AND a state token whose payload decodes to `{"tenant_id": <test_tenant>, "nonce": <any>, "expires_at": <future>}` with a valid HMAC.
- [ ] T024 [US2] `test_oauth_install_discord.py::test_install_requires_bearer` (integration): GET `/integrations/discord/install` without a bearer; assert 401 `missing_bearer` (existing Bearer middleware contract).
- [ ] T025 [US2+US4] Create `services/integrations/tests/test_oauth_callback_discord.py::test_first_install_end_to_end` (integration): with `respx.mock(base_url='https://discord.com')` mocking POST `/api/v10/oauth2/token` to return `{access_token, token_type, scope, guild:{id}, application:{id}}` AND mocking POST `/api/v10/applications/<app_id>/commands` to return 200; issue a fresh state token via `issue_state_token`, GET `/integrations/discord/callback?code=fresh&state=<token>`. Assert 302 to `/integrations/discord/installed?guild=<expected_hash>`, exactly one `provider_installations` row with `installation_id=<test_guild_id>` and `enabled=true`, two `encrypted_secrets` rows (bot_token + public_key labels), one `installation_audit_log` row with `action='install', status='ok'`, AND the respx mock recorded exactly one POST to `/applications/.../commands`.
- [ ] T026 [US2] `test_oauth_callback_discord.py::test_state_invalid_redirects_to_error` (integration): forge an HMAC-mismatched state token; GET the callback; assert 302 to `/integrations/discord/install-error?reason=state_invalid` and zero DB writes.
- [ ] T027 [US2] `test_oauth_callback_discord.py::test_state_expired_redirects_to_error` (integration): issue a state token with `expires_at` in the past (use `freezegun` or inject a backdated UPDATE on the oauth_install_states row); GET the callback; assert 302 with `reason=state_expired`.
- [ ] T028 [US2] `test_oauth_callback_discord.py::test_state_consumed_redirects_to_error` (integration): GET the callback twice with the same token; first call succeeds, second call asserts 302 with `reason=state_consumed`.
- [ ] T029 [US2] `test_oauth_callback_discord.py::test_cross_tenant_collision` (integration): pre-seed a `provider_installations` row for `guild_id=G` under tenant A; issue a state token for tenant B; GET the callback (respx returns the same `G`). Assert 302 to `install-error?reason=installation_collision`, an audit row with `status='rejected_collision'`, and NO log line containing tenant A's UUID (use `caplog` to scan).
- [ ] T030 [US4] `test_oauth_callback_discord.py::test_command_registration_failure_does_not_block_install` (integration): respx mocks `/oauth2/token` 200 + `/applications/<app_id>/commands` 403 with body `{"code": 50001}`. Assert 302 to success (install completes), `provider_installations` enabled=true, audit row `status='error'` with `context.error_code=50001`.
- [ ] T031 [US2] `test_oauth_callback_discord.py::test_reinstall_after_disable_reuses_row_and_orphan_free` (integration): pre-seed a disabled `provider_installations` row for tenant A + guild G AND a stale `encrypted_secrets` row with `label='discord_bot_token:G', tenant_id=A` (simulating a prior install whose token was never cleaned up). Run OAuth callback for the same tenant A + guild G. Assert (a) the SAME `provider_installations.id` is reused, `enabled` flips to true, no duplicate row exists; (b) **`encrypted_secrets` contains exactly 2 rows** for `tenant_id=A AND label LIKE 'discord_%:G'` (the two fresh refs from this install); (c) the pre-seeded stale row's `id` is NO LONGER present (it was deleted by `_cleanup_prior_secrets` in T019). Together (b)+(c) satisfy SC-010's "zero orphans" assertion.

**Checkpoint**: At end of Phase 4, a Discord server admin can self-serve install. The receive side from Phase 3 + the install side from Phase 4 deliver a complete inbound product. Outbound (US3 + US5) is still missing.

---

## Phase 5: User Story 3 + User Story 5 — Bot-Kick Chokepoint + Outbound REST Client (Priority: P2 + P3)

**Goal (US3)**: The `_disable_and_zeroize_discord` function disables an installation row, deletes the bot token from `encrypted_secrets`, writes an audit row, and invalidates any in-process tenant-resolver cache — all idempotently per Clarifications Q1.

**Goal (US5)**: The `DiscordClient` is the single chokepoint for outbound calls to `discord.com/api`. Per-call bot-token resolution from the secret store; `Retry-After` honored with a ≤30s wall budget and ≤3 attempts; 401/403-code-50001 triggers the chokepoint.

**Why bundled**: The chokepoint is *triggered from* the client; tests for US3 (T037-T039) require the US5 client wiring to exist first. Implementation order: T032 (function) → T034-T036 (client with wiring) → T037-T040 (US3 tests).

**Independent Test (US3)**: Seed an enabled installation; call `_disable_and_zeroize_discord` directly. Assert installation `enabled=false`, bot-token secret deleted, audit row written.

**Independent Test (US5)**: Construct a `DiscordClient`, point it at a respx-mocked `discord.com/api` returning 429 then 200; call any outbound; assert exactly 2 attempts with the `Retry-After` honored, returns the final 200.

### Implementation

- [ ] T032 [US3] Create `services/integrations/discord/uninstall.py::_disable_and_zeroize_discord(*, pool, secret_store, installation_row_id, tenant_id, guild_id, reason: str = 'outbound_401') -> None`: in a single connection (no transaction needed — operations are independently idempotent), `UPDATE provider_installations SET enabled=FALSE WHERE id=$1 AND tenant_id=$2`; SELECT the `discord_bot_token:<guild_id>` row id from `encrypted_secrets` where `tenant_id=$1`; call `secret_store.delete(ref, tenant_id=tenant_id)` and suppress `SecretNotFoundError`; INSERT `installation_audit_log` with `action='uninstall', status='ok', context={'reason': reason}`. Also call `request.app.state.tenant_resolver.invalidate(provider='discord', installation_id=guild_id)` if a resolver cache exists (defensive — the API may not have an invalidate method; if not, no-op).
- [ ] T033 [US5] Create `services/integrations/discord/client.py::RateLimitState` dataclass (research R-defined) and `DiscordClient` class per `contracts/module-discord-client.md`. Constructor takes `pool, secret_store, installation_row_id, tenant_id, guild_id, http_client?`. Implement private `_resolve_bot_token()` via `secret_store.get(label=f'discord_bot_token:{guild_id}', tenant_id=tenant_id)` → bytes → `.decode('utf-8')`; on `SecretNotFoundError` raise `DiscordApiError(code='discord_secret_unavailable')`.
- [ ] T034 [US5] In `DiscordClient`, implement private `async _request(method, url_template, **kwargs) -> dict`: substitute path params into `url_template`, attach `Authorization: Bot <token>` header (skip for `/webhooks/{app_id}/{interaction_token}` follow-ups which don't need a bot token — see contract). Loop with `RateLimitState`: on 429, sleep `min(retry_after, 30 - state.total_wall_seconds)`, increment attempts, retry; on 200, return resp.json(); on 401 (or 403 with body `code=50001`), call `_disable_and_zeroize_discord` then raise `DiscordApiError(code='discord_api_unauthorized')`; on other 4xx/5xx raise `DiscordApiError(code='discord_api_error', context={'http_status': resp.status_code})`; on budget exhausted raise `DiscordApiError(code='discord_api_rate_limited')`.
- [ ] T035 [US5] In `DiscordClient`, implement public methods `post_followup_message`, `get_guild_member`, `get_channel`, `post_register_global_command` per `contracts/module-discord-client.md`. Each is a thin wrapper that calls `_request` with the right method + URL template.
- [ ] T036 [US5] Add structured logging via `structlog.get_logger(__name__)` in `_request`: log `discord_api_request` with `method`, `endpoint=<url_template>` (the unsubstituted form so guild_id does not leak), `tenant_id`, `http_status`, `duration_ms`. NEVER include the raw substituted URL or the `guild_id` in the structured log (FR-005, SC-006).

### Tests for User Story 3

- [ ] T037 [US3] Create `services/integrations/tests/test_uninstall_discord.py::test_401_disables_installation_and_zeroes_token` (integration): seed an enabled installation + bot-token secret; respx mocks `https://discord.com/api/v10/guilds/<gid>/members/<uid>` to return 401; construct a `DiscordClient` and call `get_guild_member(user_id)`. Assert raise `DiscordApiError(code='discord_api_unauthorized')`, installation row `enabled=false`, bot-token secret row deleted, audit row with `action='uninstall', status='ok'`.
- [ ] T038 [US3] `test_uninstall_discord.py::test_concurrent_401s_are_idempotent` (integration): seed an enabled installation + bot-token secret; respx mocks the same endpoint to return 401; construct TWO `DiscordClient` instances pointing at the same installation; `await asyncio.gather(c1.get_guild_member(uid1), c2.get_guild_member(uid2))`. Assert both raise `DiscordApiError(code='discord_api_unauthorized')`, final state has installation `enabled=false`, secret deleted, ≤ 2 audit rows with `action='uninstall'` (per Clarifications Q1).
- [ ] T039 [US3] `test_uninstall_discord.py::test_403_code_50001_triggers_chokepoint` (integration): same as T037 but respx returns 403 with body `{"code": 50001}`. Assert chokepoint fires identically.
- [ ] T040 [US3] `test_uninstall_discord.py::test_disabled_installation_rejects_next_inbound` (integration): seed a disabled installation; POST a signed Discord interaction for that guild's `guild_id` to `/webhooks/discord/events`. Assert HTTP 401 with `context.reason='unknown_installation'`; NO observation row written; `guild_id` not in `caplog.text`.

### Tests for User Story 5

- [ ] T041 [US5] Create `services/integrations/tests/test_client_discord.py::test_429_retry_within_budget` (integration): seed an enabled installation + bot-token secret; respx mocks `/guilds/<gid>/members/<uid>` to return 429 with `Retry-After: 1` on attempt 1, 200 on attempt 2. Call `get_guild_member`; assert it returns 200, exactly 2 attempts recorded, total wall ≥ 1 s and < 30 s.
- [ ] T042 [US5] `test_client_discord.py::test_budget_exhausted_raises_rate_limited` (integration): respx returns 429 with `Retry-After: 2` repeatedly. Call `get_guild_member`; assert raises `DiscordApiError(code='discord_api_rate_limited')`, ≤ 3 attempts.
- [ ] T043 [US5] `test_client_discord.py::test_orphan_secret_ref_raises_discord_secret_unavailable` (integration): seed a `provider_installations` row but DO NOT seed the `discord_bot_token:<guild_id>` row. Call any outbound; assert raises `DiscordApiError(code='discord_secret_unavailable')`.
- [ ] T044 [US5] `test_client_discord.py::test_no_guild_id_in_structured_logs` (integration): with `structlog` configured to capture in-memory, call an outbound that succeeds and one that fails with 401. Assert the captured log records contain `tenant_id`, `http_status`, but NEVER the raw `guild_id` value (SC-006).

**Checkpoint**: At end of Phase 5, all five user stories are complete and tested. The integration is feature-complete.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final polish, metrics, docs, and the IN-08-regression sweep that satisfies SC-009.

- [ ] T045 [P] Create `services/integrations/discord/metrics.py` defining a counter family `discord_install_outcomes_total{outcome}` and `discord_uninstall_outcomes_total{outcome}` (mirror `services/integrations/slack/metrics.py`). Wire `metrics.record_install_outcome(...)` calls into `oauth.py::callback_handler` at every redirect endpoint AND into `uninstall.py::_disable_and_zeroize_discord`.
- [ ] T046 [P] Add a regression test `services/webhooks/tests/test_verifier_discord_db_backed.py::test_signed_interaction_resolves_via_db_backed_public_key` (integration): seed `provider_installations` + `encrypted_secrets[label='discord_public_key:<guild_id>']` with a known ed25519 public key; POST a synthetic signed Discord interaction for that guild; assert verification succeeds AND the secret_label in the verifier context reads `installation:<ref>` (not the env-var path).
- [ ] T047 [P] Add a regression test `services/webhooks/tests/test_verifier_discord_db_backed.py::test_ping_uses_env_var_public_key` (integration): seed nothing in DB; set `WEBHOOK_SECRET_DISCORD` env to a known public key; POST a signed PING (type=1). Assert HTTP 200, body `{"type": 1}`, the env-var path was used.
- [ ] T048 [P] Add a regression test `services/webhooks/tests/test_tenant_resolver_lookup.py::test_discord_unknown_guild_returns_unknown_installation` (or extend existing): with no `provider_installations` row for `guild_id=Z`, POST a signed interaction. Assert 401 `unknown_installation`, no log line contains `Z` (use `caplog`).
- [ ] T049 Re-run the full IN-08 test suite (`pytest services/integrations/tests/ services/webhooks/tests/`) AGAINST UNCHANGED IN-08 FILES; assert all 163 IN-08 tests pass (SC-009). If any IN-08 test fails, the change in IN-09 violated FR-016 and must be rolled back.
- [ ] T050 [P] Update `CODEBASE-ARCHITECTURE.md` — append a new §15 documenting IN-09 in the same shape as the IN-08 §14 entry. Mention: new package `services/integrations/discord/`; new source_channel `discord:interaction`; new label conventions in `encrypted_secrets`; bot-kick chokepoint pattern (outbound 401 → disable, no webhook); Gateway WebSocket deferred to IN-12.

**Checkpoint**: At end of Phase 6, IN-09 is shippable. Run `python scripts/check_schema_drift.py` (should be zero changes — no migrations) and `ruff` on changed paths.

---

## Dependencies

```
Phase 2 (Foundational) ───┐
                          ├──► Phase 3 (US1: Ingest) ──┐
                          │                            │
                          ├──► Phase 4 (US2 + US4) ────┤
                          │                            │
                          └──► Phase 5 (US3 + US5) ────┴──► Phase 6 (Polish)
```

Phases 3, 4, and 5 are *parallel-safe* in terms of file ownership (different paths) but have a logical ordering for *integration smoke*: shipping Phase 3 alone gives you a half-product (ingest works, no self-serve install); shipping Phase 4 alone gives a self-serve install with no follow-up ack; shipping Phase 5 alone is dead code without inbound traffic. The recommended ordering for incremental delivery is the slice order in plan.md: 3 → 4 → 5 → 6.

Within Phase 5, T032 (chokepoint function) MUST land before T037-T040 (chokepoint tests), and T033-T036 (client) MUST land before T037-T044 (any tests that exercise the client). The dependency graph inside the phase is: T032 ∥ (T033 → T034 → T035 → T036) → (T037-T044).

## Parallel Execution Examples

Within Phase 2: T004, T005, T006 are all `[P]` — different files, no inter-dependencies.

Within Phase 6: T045, T046, T047, T048, T050 are all `[P]` — different files. T049 is intentionally NOT `[P]` because it runs the full test suite and must observe the final committed state of all earlier tasks.

Within Phase 3: T009-T012 are sequential because they all edit (or extend) the same `test_ingest_discord.py` file. T007 + T008 are tightly coupled (same file) so are sequential.

Within Phase 4: T013-T017 are sequential (all build up `oauth.py`); T018 [P] vs T013-T017 (different file); T019 depends on T013-T018; T021 + T022 depend on T019; tests T023-T031 are all in `test_oauth_*.py` so are sequential within themselves but can run AFTER the implementation tasks.

## Implementation Strategy

**MVP (one slice)**: Phase 2 + Phase 3 alone. This delivers "Slack/Discord servers that have already been manually installed via `webhook_install.py` get their /fyralis invocations as Observations." That's enough to demo the integration to a single workspace without yet requiring the self-serve flow. The cross-tenant collision + state token + outbound client all remain unimplemented in this MVP slice.

**Recommended ordering for full delivery**:

1. Phase 2 (substrate verification + error classes) — ~half day
2. Phase 3 (US1: ingest) — ~1 day
3. Phase 4 (US2 + US4: OAuth + commands) — ~1.5 days
4. Phase 5 (US3 + US5: chokepoint + client) — ~1 day
5. Phase 6 (polish + regression) — ~0.5 day

**Total: ~4 days** (vs the 3.25 day estimate in source.md; the extra time absorbs the regression sweep T049 and the contract test polish in T045-T048).
