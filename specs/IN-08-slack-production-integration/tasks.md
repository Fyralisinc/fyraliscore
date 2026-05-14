---

description: "Task list for IN-08 Slack production integration"
---

# Tasks: Slack Production Integration — OAuth Install, DB-Backed Secrets, Customer Self-Serve

**Input**: Design documents from `specs/IN-08-slack-production-integration/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/)

**Tests**: Integration tests are MANDATORY per Constitution §IV (real Postgres + Ollama; HTTP boundaries to Slack may be mocked with `respx`). Every user story has at least one integration test slice. Test tasks are listed BEFORE their implementation tasks within each story so TDD failure → green is observable.

**Organization**: Tasks are grouped by user story. Migrations and the foundational secret-store module ship in Phase 2 BEFORE any user story can be tested (Constitution §IX: migrations first → dual-write writers → reader cutover).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable (different files, no dependency on incomplete tasks in this phase)
- **[Story]**: maps task to a user story (US1..US6) from [spec.md](./spec.md). Setup / Foundational / Polish phases have no story label.
- Paths are repo-root-relative.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create empty package skeletons so subsequent tasks can drop files into known locations.

- [X] T001 Create the `services/integrations/` package skeleton: `services/integrations/__init__.py`, `services/integrations/slack/__init__.py`, `services/integrations/tests/__init__.py`, `services/integrations/tests/conftest.py` (empty `conftest.py` reuses parent `services/conftest.py`).
- [X] T002 [P] Create the `lib/shared/secrets/` package skeleton: `lib/shared/secrets/__init__.py` (empty), `lib/shared/secrets/tests/__init__.py`, `lib/shared/secrets/tests/conftest.py`.
- [X] T003 [P] Add structured-error subclasses to `lib/shared/errors.py`: `SecretStoreError` (code `secret_store_unavailable`), `SecretNotFoundError` (code `secret_not_found`), `InstallationCollisionError` (code `installation_collision`), `StateTokenInvalidError` (code `state_token_invalid` with `reason` context field). Export from `lib/shared/errors.py::__all__`.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the migrations and the secret-store backend that every user story depends on. No user-story tasks run until this phase is green.

**⚠️ CRITICAL**: Per Constitution §IX, migrations land first. The next-free migration numbers (verified during planning) are **0040** and **0041**.

- [X] T004 Write `db/migrations/0040_slack_installation_tokens.sql` creating BOTH `encrypted_secrets` AND `oauth_install_states` per [data-model.md](./data-model.md) §1 and §2. Idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `DO $$ … duplicate_object` for policies). Single `BEGIN; … COMMIT;`. Includes `ENABLE ROW LEVEL SECURITY` + `FORCE` + `tenant_isolation` policy on both tables. Tenant-prefixed indexes: `idx_encrypted_secrets_tenant`, `idx_oauth_install_states_tenant_expires`. `UNIQUE (nonce)` on `oauth_install_states`.
- [X] T005 [P] Write `db/migrations/0041_installation_audit_log.sql` creating `installation_audit_log` per [data-model.md](./data-model.md) §3. Idempotent. CHECK constraints on `action` and `status`. Tenant-prefixed index `idx_installation_audit_log_tenant_created`. Partial index `idx_installation_audit_log_installation` on `(installation_row_id) WHERE installation_row_id IS NOT NULL`. RLS as above.
- [X] T006 Implement `lib/shared/secrets/store.py::FernetSecretStore` per [contracts/module-secret-store.md](./contracts/module-secret-store.md). Methods: `async put / get / rotate / delete`. Allocates row UUIDs via `lib.shared.ids.uuid7()`. Hand-rolled `WHERE tenant_id = $...` on every operation. Uses `cryptography.fernet.Fernet` for symmetric envelope. Constructor accepts either `master_kek: bytes` or `multi_fernet: MultiFernet`. Raises `SecretNotFoundError` on missing ref-for-tenant; `SecretStoreError` on backend / decrypt failures.
- [X] T007 [P] Implement `lib/shared/secrets/__init__.py` public surface: `SecretStore` Protocol, `build_secret_store(pool, master_kek_loader=None) → SecretStore` factory, re-export of `SecretStoreError`, `SecretNotFoundError`. Factory reads `MASTER_KEK` env var by default; production-env missing/empty → fail-fast; dev → generate one-shot in-memory key with a structured warning (per [research.md](./research.md) R1).
- [X] T008 [P] Write `lib/shared/secrets/tests/test_store.py` integration tests against `fresh_db` (real Postgres on `localhost:5433`), one test per row of the test-plan table in [contracts/module-secret-store.md](./contracts/module-secret-store.md): `test_put_returns_uuid_ref`, `test_get_after_put_roundtrip`, `test_get_unknown_ref_raises_not_found`, `test_get_wrong_tenant_raises_not_found`, `test_rotate_preserves_ref`, `test_delete_then_get_raises_not_found`, `test_delete_unknown_is_noop`, `test_decrypt_failure_raises_store_error`, `test_db_unavailable_raises_store_error`, `test_rls_isolates_tenants`. All `@pytest.mark.integration`.
- [X] T009 Wire `app.state.secret_store = build_secret_store(pool)` into `services/gateway/main.py::build_app` lifespan, AFTER pool wiring and BEFORE `app.include_router(...)` calls. The wiring is conditional on `app.state.pool is not None` to preserve the test-path that constructs the app synchronously.
- [X] T010 [P] Run `python scripts/check_schema_drift.py` after T004+T005 apply against the dev DB. Zero-exit required. If non-zero, debug and resolve before moving to Phase 3.

**Checkpoint**: Migrations applied, `encrypted_secrets` / `oauth_install_states` / `installation_audit_log` exist with RLS + tenant-prefixed indexes. `SecretStore` is callable from any code path that holds `app.state`.

---

## Phase 3: User Story 1 — Production-Grade Per-Installation Secret Storage (Priority: P1) 🎯 MVP

**Goal**: Webhook signature verification reads its signing secret from `encrypted_secrets` via `app.state.secret_store`, not from env vars. Env-var path remains as an explicitly opt-in dev fallback. Demonstrates that **no plaintext secret material lives in env vars in any production environment** (SC-002).

**Independent Test**: Insert a `provider_installations` row whose `secret_ref` points at a known `encrypted_secrets` ref (created via `SecretStore.put`). Fire a properly-signed Slack webhook for that workspace. Verify HMAC validation succeeds with no env-var lookup. Confirm that with `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=0` (or unset) and no env var, a webhook for a workspace whose `secret_ref` does not resolve produces the IN-07 `unknown_installation` shape. Existing IN-07 admin tests in `services/webhooks/tests/test_tenant_resolver_admin.py` continue to pass unchanged (SC-008).

### Tests for User Story 1 ⚠️ (Write FIRST, observe FAIL, then implement)

- [X] T011 [P] [US1] Write `services/webhooks/tests/test_secrets_db_backed.py` integration tests: (a) `test_load_secrets_resolves_via_secret_store` — insert installation + secret, assert `load_secrets` returns the plaintext via the store; (b) `test_load_secrets_env_fallback_off_returns_empty` — with `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=0`, installation present but ref unresolvable, `load_secrets` returns `[]`; (c) `test_load_secrets_env_fallback_on_uses_env` — with `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`, no ref, env var present, returns env value; (d) `test_load_secrets_prod_with_fallback_set_fails_startup` — `FYRALIS_ENV=prod` + fallback flag set → `assert_prod_safety_invariants()` raises. All `@pytest.mark.integration`.
- [X] T012 [P] [US1] Write `services/webhooks/tests/test_verifier_slack_db_backed.py`: end-to-end signed Slack webhook with installation row whose `secret_ref` is a real `encrypted_secrets` UUID. Asserts 200 / 201 ingest result and that `ctx.secret_label` matches the label set during the `SecretStore.put`.

### Implementation for User Story 1

- [X] T013 [US1] Rewrite `services/webhooks/secrets.py::load_secrets` to: (a) if `tenant_id` and `provider_installations.secret_ref` are present for this `(provider, tenant_id)`, call `app.state.secret_store.get(ref, tenant_id=tenant_id)` and return the resolved secret as a `Secret` record; (b) fall through to the env-var path ONLY when `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set; (c) the function signature gains `app_state: Any | None = None` so callers can pass it explicitly in tests, with `None` falling back to a module-level globals-free helper that pulls deps from the caller. Keep the existing `Secret` shape so `services/webhooks/verifier.py` doesn't change.
- [X] T014 [US1] Add `services/webhooks/secrets.py::assert_prod_safety_invariants()`: reads `FYRALIS_ENV` and `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW`; raises `RuntimeError` if `FYRALIS_ENV='prod'` and the fallback flag is truthy. Documented in module docstring.
- [X] T015 [US1] Wire the `assert_prod_safety_invariants()` call into `services/gateway/main.py::build_app` startup (called once during lifespan, BEFORE the secret store is constructed — failing fast is the point).
- [X] T016 [US1] Update `services/webhooks/signatures/slack.py` to accept the per-installation `Secret` records produced by the new `load_secrets`. (Most likely a no-op since the `Verifier` protocol already takes `secrets: Sequence[Secret]`; this task verifies that.)
- [X] T017 [US1] Update `services/webhooks/router.py::receive` to pass `app_state=request.app.state` into `load_secrets` so the DB-backed lookup is reachable. This is a minimal-surface change at the call site; the rest of router behavior is unchanged in US1.

**Checkpoint**: Acceptance scenarios 1–4 from US1 pass. SC-002 holds in dev (no env var needed when `secret_ref` is set). The existing IN-07 admin tests (`test_tenant_resolver_admin.py`) pass unchanged (SC-008).

---

## Phase 4: User Story 2 — Webhook Router Uses TenantResolver Exclusively (Priority: P2)

**Goal**: `services/webhooks/router.py` resolves tenants only via the IN-07 `app.state.tenant_resolver.resolve(...)`. The legacy `services.webhooks.tenant_resolution` import is removed entirely from the router. Forged `team_id` → 401 `unknown_installation` with no log leak (SC-007). A grep of the router for `services.webhooks.tenant_resolution` returns zero (SC-005).

**Independent Test**: Construct a request whose `team_id` matches an enabled `provider_installations` row → 2xx with resolved tenant on the request scope. Construct a forged-`team_id` request → 401 with IN-07 `unknown_installation` and zero `team_id` substrings in the captured structured logs. Static `grep services.webhooks.tenant_resolution services/webhooks/router.py` returns zero matches.

### Tests for User Story 2 ⚠️ (Write FIRST)

- [ ] T018 [P] [US2] Write `services/webhooks/tests/test_router_uses_tenant_resolver.py`: (a) `test_router_resolves_via_app_state_resolver` — a Slack webhook with a known `team_id` resolves to the tenant via `app.state.tenant_resolver`; (b) `test_forged_team_id_returns_401_unknown_installation_no_leak` — captured `caplog`/`structlog` lines contain neither the forged `team_id` nor the substring after the call returns 401; (c) `test_payload_missing_returns_400` — Slack payload with no `team_id` → HTTP 400 with PayloadMissing-shaped error; (d) `test_router_has_zero_legacy_resolver_imports` — opens the router source file and asserts `'services.webhooks.tenant_resolution'` not in source.

### Implementation for User Story 2

- [ ] T019 [US2] Refactor `services/webhooks/router.py`: remove the `from services.webhooks.tenant_resolution import resolve_tenant` import; replace both call sites (lines ~162 and ~219) with `outcome = await request.app.state.tenant_resolver.resolve(provider, payload_dict, headers)` followed by a `match outcome:` translating `Resolved` → `tenant_id_uuid = outcome.tenant_id`, `UnknownInstallation` → 401 with code `unknown_installation`, `PayloadMissing` → 400 with code `payload_missing`. Preserve the existing pattern of running signature verification BEFORE issuing tenant_not_resolved errors (so an attacker probing tenant ids sees signature failures first). For the Slack URL-verification handshake path, the resolver is bypassed (handshake doesn't carry `team_id` reliably).
- [ ] T020 [US2] Leave `services/webhooks/tenant_resolution.py` in place untouched (it is referenced by an `__init__.py` docstring in `services/webhooks/signatures/`, and the ClickUp body specifies a 24-h staging soak before deletion). Add a top-of-file deprecation docstring noting "scheduled for deletion after 24h staging soak post-IN-08 — see specs/IN-08…/tasks.md T035".
- [ ] T021 [US2] Update `services/webhooks/signatures/__init__.py` docstring to point at `services/webhooks/tenant_resolver.py` (the IN-07 DB-backed resolver) instead of `tenant_resolution.py`. This is a docstring-only change.
- [ ] T022 [US2] Add a one-line metric instrumentation in `router.py`: emit `webhook_resolver_outcomes_total{provider, outcome}` from the router after the resolve call (the resolver already does this internally per IN-07, but document the contract surface for SC-006). If the IN-07 resolver already emits this metric (verified during planning — it does, via `record_resolver_outcome`), this task becomes a no-op and is checked off after confirmation.

**Checkpoint**: All US2 acceptance scenarios pass. SC-005, SC-007 hold. The IN-07 resolver's existing metric `webhook_resolver_outcomes_total{provider="slack", outcome="resolved"}` is now driven by real webhook traffic on this code path.

---

## Phase 5: User Story 3 — Slack Workspace Admin Completes OAuth Install End-to-End (Priority: P3)

**Goal**: A Slack workspace admin can GET `/integrations/slack/install` with a Bearer token, follow the 302 to Slack, consent, and land back at `/integrations/slack/installed?team=<short_hash>` with `provider_installations` row created and tokens persisted in `encrypted_secrets`. First subsequent Slack message arrives as an `Observation` under the correct `tenant_id` within 30 s (SC-001).

**Independent Test**: With a Bearer-authenticated session, GET `/integrations/slack/install` — assert 302 to `https://slack.com/oauth/v2/authorize?…` with a state token. Inject a `respx`-mocked Slack `oauth.v2.access` response. GET `/integrations/slack/callback?code=<fake>&state=<the state>` — assert 302 to `/integrations/slack/installed?team=…`, `provider_installations` has the row, `encrypted_secrets` has 2–3 rows, `installation_audit_log` has the `install`/`ok` row, `oauth_install_states.consumed_at` is set. Send a `slack:message` webhook for the new `team_id` — assert an `Observation` is persisted under the tenant.

### Tests for User Story 3 ⚠️ (Write FIRST)

- [ ] T023 [P] [US3] Write `services/integrations/tests/test_oauth_install.py`: (a) `test_install_redirect_to_slack` — 302, Location starts with `https://slack.com/oauth/v2/authorize`, query params include `client_id`, `scope`, `state`, `redirect_uri`; `oauth_install_states` has one new row with the issued nonce; (b) `test_install_requires_bearer` — 401 missing_bearer when no Authorization header.
- [ ] T024 [P] [US3] Write `services/integrations/tests/test_oauth_callback.py` covering every row in the [contracts/http-integrations-slack.md](./contracts/http-integrations-slack.md) Route 2 test plan: `test_callback_state_invalid_hmac`, `test_callback_state_expired`, `test_callback_state_consumed_replay`, `test_callback_success_fresh_install`, `test_callback_slack_oauth_error`, `test_callback_no_team_id_in_logs`, `test_callback_secret_store_unavailable`, plus an explicit `test_callback_state_unknown_nonce` (nonce never issued). All use `respx` to stub the Slack `oauth.v2.access` endpoint.
- [ ] T025 [P] [US3] Write `services/integrations/tests/test_install_end_to_end.py`: full Step 4 + Step 5 flow from [quickstart.md](./quickstart.md). Drives the install via the test client, then sends a `slack:message` webhook signed with the now-stored signing secret, asserts an `observations` row appears under the install's `tenant_id` within an internal `freezegun`-controlled window.

### Implementation for User Story 3

- [ ] T026 [US3] Implement `services/integrations/slack/oauth.py::issue_state_token(tenant_id, pool) -> str` and `verify_state_token(state, pool) -> tuple[UUID, dict]` per [data-model.md](./data-model.md) §2 and [contracts/http-integrations-slack.md](./contracts/http-integrations-slack.md). `issue` inserts an `oauth_install_states` row via `uuid7()` and returns `base64url(payload).base64url(hmac(SERVER_HMAC_KEY, payload))`. `verify` parses + HMAC-checks + atomic `UPDATE … RETURNING` consume. Raises `StateTokenInvalidError` with `reason` ∈ `{state_invalid, state_expired, state_consumed}` distinguishing the failure modes.
- [ ] T027 [US3] Implement `services/integrations/slack/oauth.py::install_handler(request) -> Response` (the GET `/integrations/slack/install` route). Reads `request.state.tenant_id` from the Bearer-resolved session, calls `issue_state_token`, constructs the Slack `oauth/v2/authorize` URL with scopes from a module-level constant `_SLACK_SCOPES` matching FR-013, returns 302.
- [ ] T028 [US3] Implement `services/integrations/slack/oauth.py::callback_handler(request) -> Response` (the GET `/integrations/slack/callback` route). Step sequence per [contracts/http-integrations-slack.md](./contracts/http-integrations-slack.md) Route 2 "Handler steps (happy path)": verify state → consume → POST to `oauth.v2.access` via `httpx.AsyncClient` → `secret_store.put` for bot/user/signing → UPSERT `provider_installations` with the cross-tenant collision-detecting `WHERE provider_installations.tenant_id = EXCLUDED.tenant_id` → audit row → invalidate resolver cache → metric → 302. Every failure branch returns the appropriate `<reason>` + HTTP status per the failure table. Success redirect uses `services.integrations.slack.oauth.short_team_hash(team_id)` (a 16-hex `blake2b(digest_size=8)` of the team_id) so the URL doesn't leak the raw team_id.
- [ ] T029 [US3] Implement `services/integrations/router.py::build_integrations_router() -> APIRouter`. Mounts `/integrations/slack/install` (GET, requires Bearer) and `/integrations/slack/callback` (GET, no Bearer). The router is a thin factory matching the existing webhook router pattern; both handlers live in `services/integrations/slack/oauth.py`.
- [ ] T030 [US3] Update `services/gateway/main.py`: (a) add `"/integrations/slack/callback"` to `_PUBLIC_PATHS` (the exact-match frozenset). DO NOT add `/integrations/` as a prefix — single-route, not blanket public, per ClickUp `Security/constitution notes`. (b) `from services.integrations.router import build_integrations_router; app.include_router(build_integrations_router())` inside `build_app`, immediately after the webhook router mount.
- [ ] T031 [US3] Add the `oauth_install_states` sweep lifespan task to `services/gateway/main.py`. Pattern: an `asyncio.Task` started by the lifespan handler, runs every 5 min, executes `DELETE FROM oauth_install_states WHERE expires_at < now() - INTERVAL '1 hour' OR (consumed_at IS NOT NULL AND consumed_at < now() - INTERVAL '1 hour') LIMIT 1000`, logs the deleted count via structlog. Task is cancelled cleanly on shutdown.
- [ ] T032 [US3] Add `services/integrations/slack/metrics.py` with `record_install_outcome(outcome: str)`, `record_uninstall_outcome(outcome: str)`, `observe_install_duration(seconds: float)` per [research.md](./research.md) R5. Wired into the OAuth handler at each success / failure branch.

**Checkpoint**: All US3 acceptance scenarios pass. SC-001, SC-009 hold. The `webhook_resolver_outcomes_total{provider="slack", outcome="resolved"}` metric is non-zero against a real Slack message sent through an installed workspace (SC-006 dry-run on dev).

---

## Phase 6: User Story 4 — Workspace Uninstall Disables Installation and Zeroes Token Material (Priority: P4)

**Goal**: An `app_uninstalled` / `tokens_revoked` event for an installed workspace disables the `provider_installations` row, deletes the associated `encrypted_secrets` rows, writes an `installation_audit_log` row with `action='uninstall'`, and ensures the very next webhook for that `team_id` returns 401 `unknown_installation` (SC-003).

**Independent Test**: With an enabled installation in DB and corresponding `encrypted_secrets` rows, fire an `app_uninstalled` event webhook. Verify `provider_installations.enabled = false`, `encrypted_secrets` rows for that team are gone, `installation_audit_log` has the uninstall row, and a subsequent `slack:message` webhook for the same `team_id` returns 401 `unknown_installation`.

### Tests for User Story 4 ⚠️ (Write FIRST)

- [ ] T033 [P] [US4] Write `services/integrations/tests/test_uninstall.py` covering [contracts/http-webhooks-slack-events.md](./contracts/http-webhooks-slack-events.md) test plan: `test_uninstall_disables_row`, `test_uninstall_zeros_secrets`, `test_uninstall_writes_audit`, `test_uninstall_next_webhook_returns_401`, `test_uninstall_unknown_team`, `test_uninstall_partial_failure_audit`, `test_tokens_revoked_equivalence`.

### Implementation for User Story 4

- [ ] T034 [US4] Implement `services/integrations/slack/uninstall.py::handle_app_uninstalled(deps, payload, resolved) -> None` and `::handle_tokens_revoked(deps, payload, resolved) -> None`. Same flow per [contracts/http-webhooks-slack-events.md](./contracts/http-webhooks-slack-events.md): `disable_installation(installation_row_id)` (existing IN-07 admin action), tenant-scoped `SELECT id FROM encrypted_secrets WHERE tenant_id = $1 AND label LIKE 'slack_%token:' || $2`, `secret_store.delete` each ref (tolerant of missing), audit insert with `action='uninstall'`, cache invalidate. Both branches share an inner helper; the public functions are thin wrappers that record the event-type label.
- [ ] T035 [US4] Extend `services/ingestion/handlers/slack_message.py` (existing IN-06 handler) to inspect `payload.event.type`. For `'app_uninstalled'` route to `handle_app_uninstalled`; for `'tokens_revoked'` route to `handle_tokens_revoked`; for everything else fall through to the existing observation-producing path. The dispatch is a single `match` statement at the top of the handler; reuse the existing signature-verification + resolver path so the handler runs ONLY for valid, tenant-resolved webhooks.

**Checkpoint**: All US4 acceptance scenarios pass. SC-003 holds.

---

## Phase 7: User Story 5 — Re-Install After Uninstall Reuses the Same Installation Row (Priority: P5)

**Goal**: After uninstall, a fresh OAuth install for the same `team_id` updates the existing `provider_installations` row (preserving its `id`) instead of failing on the `(provider, installation_id)` unique constraint or duplicating the row. `secret_ref` is rotated to a new bot-token row; the prior bot-token ref is best-effort cleaned up (SC-004).

**Independent Test**: After the US4 test scenario (uninstalled state), run the full US3 install flow for the same `team_id`. Verify exactly one `provider_installations` row exists for `(slack, team_id)`, `provider_installations.id` equals the original install's id, `enabled=true`, `secret_ref` points at a NEW `encrypted_secrets.id`, and no orphan `encrypted_secrets` rows remain for the old tokens.

### Tests for User Story 5 ⚠️ (Write FIRST)

- [ ] T036 [P] [US5] Write `services/integrations/tests/test_reinstall.py`: (a) `test_reinstall_preserves_row_id` — install, uninstall, install-again, assert row count == 1 and `id` unchanged; (b) `test_reinstall_rotates_secret_ref` — `secret_ref` post-reinstall differs from pre-uninstall ref; (c) `test_reinstall_cleans_prior_secrets` — old bot/user-token `encrypted_secrets` rows are deleted; (d) `test_reinstall_cross_tenant_rebind_rejected` — install for tenant A, uninstall, attempt install for tenant B with same `team_id` → 409 `installation_collision` (the `WHERE provider_installations.tenant_id = EXCLUDED.tenant_id` clause is the gate even when `enabled=false`).

### Implementation for User Story 5

- [ ] T037 [US5] Extend `services/integrations/slack/oauth.py::callback_handler` (already exists from T028) to, on the re-install detection path (UPSERT returns `was_inserted = FALSE`), call a new helper `_cleanup_prior_secrets(tenant_id, team_id, new_bot_ref)` that selects old token refs by `label LIKE 'slack_%token:' || $team_id` excluding the new ref, and `secret_store.delete`s each. Tolerant of missing rows.
- [ ] T038 [US5] Update the `installation_audit_log` context fields in `callback_handler` to include `was_reinstall: bool` and `prior_installation_row_id: UUID | null` so post-mortem diagnosis can distinguish fresh installs from re-installs. Reflect this in the existing T028 audit-insert SQL.

**Checkpoint**: All US5 acceptance scenarios pass. SC-004 holds.

---

## Phase 8: User Story 6 — Per-Installation Outbound Slack API Calls (Priority: P6)

**Goal**: Provide an async outbound client wrapping `chat.postMessage`, `users.info`, `conversations.info`. Each call uses the per-installation bot token resolved via `SecretStore`. 429 responses honor `Retry-After` with bounded retry. Becomes the substrate for Slack-outbound Acts in IN-10.

**Independent Test**: Given an installed workspace and a `respx`-mocked Slack endpoint, call `client.users_info(user_id)` and assert the request bears `Authorization: Bearer xoxb-…` (the installation's bot token). Mock a 429 with `Retry-After: 1` and assert the client waits 1 s and retries. Mock continuous 429s and assert the client surfaces a structured error after exhausting the retry budget.

### Tests for User Story 6 ⚠️ (Write FIRST)

- [ ] T039 [P] [US6] Write `services/integrations/tests/test_client.py`: `test_users_info_uses_installation_bot_token`, `test_chat_postmessage_serialization`, `test_conversations_info_returns_record`, `test_429_retry_after_honored`, `test_429_budget_exhausted_raises`, `test_transport_error_retries_with_backoff`. All `respx`-mocked.

### Implementation for User Story 6

- [ ] T040 [US6] Implement `services/integrations/slack/client.py::SlackClient`. Constructor takes `secret_store: SecretStore`, `pool: asyncpg.Pool`, `tenant_id: UUID`, `installation_row_id: UUID`. Methods: `async chat_postMessage(channel, text, **kwargs) -> dict`, `async users_info(user_id) -> dict`, `async conversations_info(channel_id) -> dict`. Each method (a) reads the installation row's `secret_ref` if not cached on `self`, (b) resolves the bot token via `secret_store.get`, (c) issues `httpx.AsyncClient` POST/GET with `Authorization: Bearer <bot_token>`, (d) on 429 reads `Retry-After`, sleeps, retries up to 3 attempts within a 30 s wall budget, (e) on non-OK Slack response (`ok=false`) raises a structured `SlackApiError` (new subclass of `CompanyOSError`, code `slack_api_error`).
- [ ] T041 [US6] Optional integration hook: update the existing `services/ingestion/handlers/slack_message.py` to enrich observations with the `users.info` display name via the new client. This is a soft enrichment — failure here MUST NOT block the `Observation` write. Wrapped in a `try/except CompanyOSError` that logs and continues.

**Checkpoint**: All US6 acceptance scenarios pass. The outbound client is ready to be the substrate for IN-10's Slack-outbound Acts.

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Hygiene gates and follow-up tracking.

- [ ] T042 [P] Run `ruff check services/integrations lib/shared/secrets services/webhooks services/gateway` — zero errors.
- [ ] T043 [P] Run `python scripts/check_schema_drift.py` — zero exit.
- [ ] T044 [P] Run the full quickstart from [quickstart.md](./quickstart.md) Steps 1-9 against a local dev stack with a Slack dev workspace. Capture any deviation as a new task before merging.
- [ ] T045 [P] Confirm `grep -rn 'services.webhooks.tenant_resolution' services/webhooks/router.py` returns zero matches (SC-005). If non-zero, find the leftover reference and remove it.
- [ ] T046 [P] Confirm `grep -rn 'uuid.uuid4()' services/integrations lib/shared/secrets` returns zero matches (Constitution §VII).
- [ ] T047 [P] Confirm `grep -rn 'print(' services/integrations lib/shared/secrets` returns zero matches (Constitution Stack Constraints).
- [ ] T048 Update [CODEBASE-ARCHITECTURE.md](../../CODEBASE-ARCHITECTURE.md) §13 (or the closest "Integrations / webhook" section) with a one-paragraph note about the new `services/integrations/` package, the `encrypted_secrets` / `oauth_install_states` / `installation_audit_log` tables, and the env-var-fallback gate. Keep it descriptive (not prescriptive — prescriptive lives in the constitution).
- [ ] T049 Open a follow-up issue (or TODO marker tracked in `specs/IN-08-slack-production-integration/source.md` "Out of scope" section) for: (a) deletion of `services/webhooks/tenant_resolution.py` after a 24 h staging soak, (b) per-`MASTER_KEK`-rotation operational procedure (out of scope for IN-08; flagged in research R1).
- [ ] T050 Write the PR description for this branch including: (a) SC-001..SC-010 mapping, (b) constitution checks performed, (c) the deferred items from T049, (d) a **post-merge watch-list** explicitly naming SC-006: "within 1 h of merge to staging, confirm `webhook_resolver_outcomes_total{provider='slack', outcome='resolved'}` is non-zero; if zero, halt promotion and investigate." The PR title is `IN-08: Slack production integration — OAuth, DB-backed secrets, self-serve install`.

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (Phase 1)**: no dependencies — can start immediately. T001 / T002 / T003 are independent of each other and may be parallelized.
- **Foundational (Phase 2)**: blocks every user story. Within Phase 2: T004 ↔ T005 may be parallel (different files), but T006 / T007 / T008 cannot start until T004 has been applied to the dev DB (T010 verifies). T009 depends on T006 + T007.
- **User Stories (Phase 3+)**: ordered by priority. US1 must land before US3 starts implementation (the OAuth callback's `secret_store.put` calls require US1's wiring). US2 must land before US3's test that fires a real webhook through the installed workspace (the router refactor is what makes that test pass). US4 → US5 share `services/integrations/slack/uninstall.py` and the `oauth.py` re-install path respectively. US6 is independent of US3/US4/US5 once US1 is done (it just needs the secret store).
- **Polish (Phase N)**: depends on US1–US6 being complete.

### Within each user story

- Tests must FAIL before implementation begins (TDD discipline per Constitution §IV).
- File-isolated tasks marked `[P]` can run in parallel.
- Cross-file tasks (e.g., T028 touches `oauth.py`, T030 touches `gateway/main.py`) can be parallelized across files within the same story but must converge at the checkpoint.

### Parallel opportunities (worth highlighting)

- **Phase 1**: T001 + T002 + T003 in parallel (different files, no dep).
- **Phase 2**: T004 + T005 in parallel (different migration files).
- **Phase 3 tests**: T011 + T012 in parallel (different test files).
- **Phase 5 tests**: T023 + T024 + T025 in parallel.
- **Phase 8 vs. Phase 5/6/7**: US6 (outbound client) is independent of US3/US4/US5 once Phase 2 is done and US1 (secret store) ships — could be parallelized to a different developer.
- **Polish**: T042–T047 in parallel.

---

## Parallel Example: User Story 1

```bash
# Tests first (in parallel — different test files):
Task: "Write services/webhooks/tests/test_secrets_db_backed.py (T011)"
Task: "Write services/webhooks/tests/test_verifier_slack_db_backed.py (T012)"

# Then implementation, mostly sequential within services/webhooks/secrets.py:
Task: "Rewrite load_secrets in services/webhooks/secrets.py (T013)"
Task: "Add assert_prod_safety_invariants in services/webhooks/secrets.py (T014)"
Task: "Wire startup invariant check in services/gateway/main.py (T015)"
```

---

## Implementation Strategy

### MVP First (US1 only)

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational — migrations + secret store.
3. Complete Phase 3: User Story 1 — DB-backed signing-secret resolution.
4. **STOP and VALIDATE**: webhook with installation row whose `secret_ref` is set verifies HMAC correctly via the secret store, no env vars required. The IN-07 admin tests pass unchanged.
5. Ship this slice — it's the substrate. The next slices ride on top.

### Incremental delivery

1. **Slice 1 (US1, ~1.5 d)** — Foundation + secret store + signing-secret read path. Ship to staging behind a deploy of just this slice. Soak.
2. **Slice 2 (US2, ~0.5 d)** — Router cutover. Ship to staging. Watch `webhook_resolver_outcomes_total{outcome="resolved"}` and `{outcome="unknown_installation"}` — both should be non-zero against existing manually-inserted installations within 1 h (SC-006 dry-run).
3. **Slice 3 (US3, ~2 d)** — OAuth install flow. The headline feature. Ship to staging; complete the full quickstart locally first (Steps 1-5 of [quickstart.md](./quickstart.md)).
4. **Slice 4 (US4, ~0.5 d)** — Uninstall handling. Ship.
5. **Slice 5 (US5, ~0.25 d)** — Re-install reuse. Ship.
6. **Slice 6 (US6, ~1 d)** — Outbound client. Independent slice; can be in flight in parallel with US4 + US5.

Total ≈ 5.75 d of focused work, which fits the ClickUp 6-day estimate.

### Constitution check gate (before each slice ships)

- §III: every new tenant-scoped table query carries `WHERE tenant_id = $...`.
- §VII: `uuid7()` for every new substrate-adjacent row.
- §IV: integration tests touch real Postgres on `localhost:5433`.
- §X: no new abstractions beyond `SecretStore` Protocol (which earns its keep — see [plan.md](./plan.md) Constitution Check).
- No `print()`, no `uuid.uuid4()`, no mocked Postgres.

---

## Notes

- `[P]` tasks = different files, no incomplete-task dependency in the same phase.
- `[Story]` label = `[US1]`..`[US6]`; setup/foundational/polish phases carry no story label.
- Each user story is independently completable AND independently shippable (see Implementation Strategy).
- Verify tests fail before writing implementation; commit after each task or logical group.
- Per Constitution §II, migrations 0040 and 0041 are NEVER edited after merge — corrections ship as 0042+.
- Per Constitution §IX, the order migrations → dual-write path (US1's `secrets.py` reads both DB and env) → reader cutover (US2's router) is hardwired into the phase ordering above and MUST NOT be reordered.
