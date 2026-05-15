---
description: "Task list for IN-13 GitHub Production Integration"
---

# Tasks: GitHub Production Integration — App Install, Webhook Ingest, Single App-Level Secret with Tenant Routing, Uninstall Chokepoint

**Input**: Design documents from `specs/IN-13-github-integration/`
**Prerequisites**: plan.md, spec.md (with US1–US7), research.md (R1–R10), data-model.md, contracts/{http-integrations-github.md, http-webhooks-github-events.md, module-github-client.md, module-github-lifecycle.md}

**Tests**: Integration tests are MANDATORY per Constitution §IV (live Postgres, real Fernet, respx for `api.github.com` only). Tests are listed alongside implementation.

**Organization**: Tasks are grouped by user story (P1 → P2 → P3) to enable independent implementation. Within each story, infra is implemented first, then unit/contract tests, then integration tests against the wired-up router.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on incomplete tasks in same phase)
- **[Story]**: User-story label (US1–US7). No label = Setup / Foundational / Polish.
- All paths absolute from repo root.

## Path Conventions

- Source: `services/integrations/github/`, `services/webhooks/`, `services/integrations/`, `lib/shared/`, `db/migrations/`
- Tests: `services/integrations/tests/test_*_github.py`, `services/webhooks/tests/test_verifier_github*.py`
- Spec: `specs/IN-13-github-integration/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Confirm reusable substrate is intact, add the single new migration, and pull in PyJWT.

- [ ] T001 Write `db/migrations/0042_provider_installations_selected_repositories.sql` — single idempotent `ALTER TABLE provider_installations ADD COLUMN IF NOT EXISTS selected_repositories JSONB DEFAULT NULL` wrapped in `BEGIN/COMMIT` with the leading-comment header per Constitution §II.5 (additive-only; no staged plan required).
- [ ] T002 [P] Add `pyjwt[crypto]>=2.8` to `[project.dependencies]` in `pyproject.toml`. Run `pip install -e .[dev]` in dev to refresh the venv (CI gets it via the next image build).
- [ ] T003 [P] Add `services/integrations/github/__init__.py` (empty package marker) and `services/integrations/github/tests/` directory placeholder. The shared test conftest from IN-08/IN-09 at `services/integrations/tests/conftest.py` is reused — do NOT create a parallel conftest under `github/`.
- [ ] T004 [P] Read-only assertion test `services/integrations/tests/test_substrate_preconditions_github.py::test_provider_installations_has_selected_repositories_column` — after T001 is applied to the test DB, assert the column exists with type `jsonb`.
- [ ] T005 [P] Read-only assertion test `services/integrations/tests/test_substrate_preconditions_github.py::test_oauth_install_states_provider_column_present` (R4 verification) — confirm `oauth_install_states.provider TEXT` exists and the state-token consume path keys on `(provider, nonce)`.
- [ ] T006 [P] Read-only assertion test `services/integrations/tests/test_substrate_preconditions_github.py::test_observations_unique_index_present` — confirm `observations_source_channel_external_id_occurred_at_key` unique index exists.

**Checkpoint**: Setup ready — migration applied to the test DB, deps installed, package skeleton present. User-story work can now begin.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: App-JWT minting + outbound REST client + installation-access-token cache. Required by US2's callback (it must mint an installation token to call `GET /installation/repositories`) and by US4's outbound chokepoint.

**⚠️ CRITICAL**: No user-story work can ship without these.

- [ ] T007 Add `GithubJWTError`, `GithubApiError`, `GithubOAuthError` to `lib/shared/errors.py`. Subclass `CompanyOSError`. Define `default_code` and document the `context` keys per `contracts/module-github-client.md`.
- [ ] T008 Create `services/integrations/github/jwt.py::mint_app_jwt` per `contracts/module-github-client.md`. Reads `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY` OR `GITHUB_APP_PRIVATE_KEY_PATH` (exactly one), parses with `cryptography.hazmat.primitives.serialization.load_pem_private_key`, signs with PyJWT `algorithm='RS256'`. NO in-process key cache. Raises the four documented `GithubJWTError` shapes.
- [ ] T009 [P] [test] `services/integrations/tests/test_jwt_github.py::test_mint_smoke` — mint a JWT with a test RSA keypair, verify with the matching public key. Uses `cryptography` to generate the test key in-test.
- [ ] T010 [P] [test] `test_jwt_github.py::test_missing_app_id_raises` and `test_missing_key_raises` and `test_conflicting_keys_raises` — assert `GithubJWTError(reason=...)` for each misconfig.
- [ ] T011 [P] [test] `test_jwt_github.py::test_rotation_transparent` — set env to key A, mint, swap env to key B, mint again, verify new JWT validates with key B.
- [ ] T012 [P] [test] `test_jwt_github.py::test_private_key_never_logged` — capture structlog output; assert no PEM material or JWT signature bytes appear in any log line.
- [ ] T013 Create `services/integrations/github/client.py::GithubClient` per `contracts/module-github-client.md`. Constructor takes `GithubClientDeps`. Implements `mint_installation_token(installation_id)` with per-installation `asyncio.Lock` around cache-miss path (Risk #3). Implements `list_installation_repositories(installation_id)` with 3-page cap (R8). Implements `_maybe_disable_on_revocation(installation_id, response)` matching the exact 401-Bad-credentials AND 404-doc_url shapes from R2.
- [ ] T014 [P] [test] `services/integrations/tests/test_client_github.py::test_mint_caches_within_ttl` — two mint calls within TTL → 1 mocked POST. Uses respx to count requests.
- [ ] T015 [P] [test] `test_client_github.py::test_mint_remints_near_expiry` — mocked response says `expires_at=T0+30s`; second call at T0+10s triggers re-mint (cache eviction at 60s-before-expiry threshold).
- [ ] T016 [P] [test] `test_client_github.py::test_concurrent_mint_serialized` — two concurrent `asyncio.gather` calls for same installation_id → 1 POST.
- [ ] T017 [P] [test] `test_client_github.py::test_401_bad_credentials_triggers_chokepoint` — mocked 401 with `{"message":"Bad credentials",...}` → `_disable_installation_github` fires once + `GithubApiError(reason='unauthorized')` raised. (Chokepoint behavior verified end-to-end in T037.)
- [ ] T018 [P] [test] `test_client_github.py::test_404_apps_doc_url_triggers_chokepoint` — mocked 404 with `documentation_url` matching `/rest/apps/(apps|installations)` → chokepoint + GithubApiError.
- [ ] T019 [P] [test] `test_client_github.py::test_404_other_doc_url_does_not_trigger` — mocked 404 with unrelated `documentation_url` → GithubApiError but NO chokepoint.
- [ ] T020 [P] [test] `test_client_github.py::test_list_repos_pagination_3_page_cap` — 4 pages of mocked responses; client reads 3 pages, sets `last_repos_truncated=True`, returns 90 repos with the structured-warning log emitted.
- [ ] T021 [P] [test] `test_client_github.py::test_list_repos_all_mode` — response indicates `repository_selection='all'` → return `None` (NULL semantics).
- [ ] T022 [P] [test] `test_client_github.py::test_no_secrets_in_logs` — capture structlog; assert no JWT, no installation access token, no PEM appears.

**Checkpoint**: Outbound client + JWT minter ready. US2's callback can now mint tokens and fetch repos; US4's chokepoint can disable on outbound revocation.

---

## Phase 3: User Story 1 — Webhook Deliveries Land as Observations (Priority: P1) 🎯 MVP

**Goal**: Verified GitHub deliveries from selected repositories land as Observations under the correct tenant within 3 seconds, for `pull_request`, `push`, `issues`, `issue_comment`, `pull_request_review`, `check_run` events.

**Independent Test**: With an installation row seeded and `selected_repositories=['org/a']`, POST a signed `pull_request.opened` payload for `org/a` → 201 + one observation under the correct tenant with the existing handler's `_shape_pull_request` shape.

- [ ] T023 [US1] Extend `services/webhooks/secrets.py::load_secrets` GitHub branch per FR-007 / T023 in plan.md: prefer secret-store entry referenced by `GITHUB_APP_WEBHOOK_SECRET_REF`; include `GITHUB_APP_WEBHOOK_SECRET_PREV_REF` as a second list element for rotation; fall back to `GITHUB_WEBHOOK_SECRET` env var only when no ref is set AND `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`; raise `SecretNotFoundError(provider='github')` otherwise. The `tenant_id` parameter is ignored for GitHub.
- [ ] T024 [US1] Extend `services/gateway/main.py::_assert_prod_safety_invariants` per FR-007 + Risk #1 in plan: under `FYRALIS_ENV=prod`, require either `GITHUB_APP_WEBHOOK_SECRET_REF` non-empty OR (`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` AND `GITHUB_WEBHOOK_SECRET` non-empty). Fail-fast with a clear error message.
- [ ] T025 [P] [US1] [test] `services/webhooks/tests/test_verifier_github.py::test_db_backed_app_secret_resolves_e2e` — secret-store entry resolves via the new loader; signed delivery verifies.
- [ ] T026 [P] [US1] [test] `test_verifier_github.py::test_rotation_overlap` — secret A used to sign; verifier loaded with `[A, B]` → verify OK. Verifier loaded with `[B, C]` only (A retired) → same delivery fails `signature_mismatch`.
- [ ] T027 [P] [US1] [test] `test_verifier_github.py::test_prod_startup_invariants_fail_fast` — set `FYRALIS_ENV=prod` with neither env-var nor secret-ref set → startup assertion raises.
- [ ] T028 [US1] [test] `services/integrations/tests/test_router_github_integration.py::test_verified_pull_request_lands_as_observation` — full integration: seeded installation row + selected_repositories=['org/a'], POST signed `pull_request.opened` for `org/a` → 201, one observation with `source_channel='github:webhook'`, `external_id=<PR node_id>`, `tenant_id` correct, content_text per existing `_shape_pull_request`.
- [ ] T029 [P] [US1] [test] `test_router_github_integration.py::test_verified_push_lands_with_after_sha_external_id` — push event → observation with `external_id=<repo>@<after_sha>`, `trust_tier='authoritative'`.
- [ ] T030 [P] [US1] [test] `test_router_github_integration.py::test_verified_issue_comment_inferential` — issue_comment event → observation with `trust_tier='inferential'`, content_text truncated at 200 chars.
- [ ] T031 [P] [US1] [test] `test_router_github_integration.py::test_verified_pr_review_approved_authoritative` — PR review with `state='approved'` → `trust_tier='authoritative'`, `kind='state_change'`.
- [ ] T032 [P] [US1] [test] `test_router_github_integration.py::test_duplicate_external_id_idempotent` — second delivery for same PR.node_id within `occurred_at` window → 200 `deduped=true`, no new observation row.

**Checkpoint**: US1 ships independently. Operator can hand-seed a `provider_installations` row + repo allowlist and GitHub deliveries flow end-to-end. (US2 onboards customers self-serve; US1 doesn't require it.)

---

## Phase 4: User Story 2 — Self-Serve OAuth Install (Priority: P1)

**Goal**: A GitHub org admin clicks "Add Fyralis to GitHub", consents to the App install in GitHub's UI, and lands back on Fyralis with a `provider_installations` row written + `selected_repositories` seeded — zero operator action.

**Independent Test**: From an authenticated Fyralis tenant session, `GET /integrations/github/install` → 302 to GitHub. Simulate callback with respx-mocked GitHub APIs → row written, audit row, redirect to success page.

- [ ] T033 [US2] If `services.integrations.slack.oauth.issue_state_token` is still slack-namespaced (verify in-place), move it to `services/integrations/oauth_state.py::issue_state_token` and add a thin compatibility re-export in `services/integrations/slack/oauth.py` to keep IN-08 tests green. If already provider-neutral, no-op this task.
- [ ] T034 [US2] Create `services/integrations/github/oauth.py::install_handler` (Bearer-authed) per `contracts/http-integrations-github.md`. Mints state token bound to `request.app.state.tenant.id` via `oauth_state.issue_state_token(provider='github', tenant_id=..., expiry_seconds=600)`. INSERTs `oauth_install_states` row. 302 to `https://github.com/apps/{GITHUB_APP_SLUG}/installations/new?state=<token>`.
- [ ] T035 [US2] Create `services/integrations/github/oauth.py::callback_handler` (public; state-token-authed) per `contracts/http-integrations-github.md`. Steps: (a) verify state HMAC; (b) atomic UPDATE `oauth_install_states SET consumed_at=now() WHERE provider='github' AND nonce=$1 AND consumed_at IS NULL AND expires_at > now() RETURNING tenant_id`; (c) UPSERT `provider_installations` with the cross-tenant collision guard in the WHERE clause; (d) mint installation access token; (e) call `GithubClient.list_installation_repositories(installation_id)`; (f) write `selected_repositories`; (g) INSERT `installation_audit_log` row; (h) 302 to `/integrations/github/installed?installation=<short-hash>` OR `/integrations/github/install-error?reason=...`.
- [ ] T036 [US2] Mount `/integrations/github/install` (Bearer-authed) and `/integrations/github/callback` (public) in `services/integrations/router.py::build_integrations_router`. Pattern after the existing Slack/Discord sub-routes.
- [ ] T037 [US2] Add `/integrations/github/callback` to `_PUBLIC_PATHS` exact-match set in `services/gateway/main.py`. (The `/integrations/github/install` route stays Bearer-authed.)
- [ ] T038 [US2] Add Bearer-required exclusion guards for `/integrations/github/installed` and `/integrations/github/install-error` static landing pages (public, no DB).
- [ ] T039 [US2] [test] `services/integrations/tests/test_oauth_install_github.py::test_install_302_to_github` — Bearer-authed GET → 302 to `https://github.com/apps/{slug}/installations/new?state=<token>` with state bound to the authenticated tenant.
- [ ] T040 [P] [US2] [test] `test_oauth_install_github.py::test_install_writes_oauth_state_row` — after install GET, `oauth_install_states` has the new row with `provider='github', consumed_at=NULL`.
- [ ] T041 [P] [US2] [test] `test_oauth_install_github.py::test_install_requires_bearer` — unauthenticated GET → 401 from the gateway middleware (handler not reached).
- [ ] T042 [US2] [test] `test_oauth_callback_github.py::test_first_install_end_to_end` — full happy path with respx-mocked `POST /app/installations/<id>/access_tokens` and `GET /installation/repositories`. Assertions: row inserted with `enabled=TRUE`, `selected_repositories=['org/a','org/b']`, audit row `action='install', status='ok'`, 302 to success page.
- [ ] T043 [P] [US2] [test] `test_oauth_callback_github.py::test_state_token_expired` — manually set `expires_at < now()` → 302 to `install-error?reason=state_expired`, no DB writes after the state-check.
- [ ] T044 [P] [US2] [test] `test_oauth_callback_github.py::test_state_token_consumed` — pre-consume the row → 302 to `install-error?reason=state_consumed`.
- [ ] T045 [P] [US2] [test] `test_oauth_callback_github.py::test_state_token_hmac_mismatch` — tampered state → 302 to `install-error?reason=state_invalid`.
- [ ] T046 [P] [US2] [test] `test_oauth_callback_github.py::test_cross_tenant_collision` — seed installation_id mapped to tenant A; callback as tenant B → 302 to `install-error?reason=installation_collision`, audit row `status='rejected_collision'`, **foreign tenant id absent from response body, redirect Location, and all log lines**.
- [ ] T047 [P] [US2] [test] `test_oauth_callback_github.py::test_reinstall_same_tenant` — pre-seed disabled row for same tenant_id + installation_id → callback succeeds, row's `id` reused, `enabled=TRUE`, audit `action='reinstall'`.
- [ ] T048 [P] [US2] [test] `test_oauth_callback_github.py::test_token_mint_failure_still_writes_row` — `POST /app/installations/<id>/access_tokens` returns 500 → row still written, `selected_repositories=NULL`, audit row `status='error'` with `context.github_status_code=500`, 302 to success page (recoverable via the lifecycle webhook).
- [ ] T049 [P] [US2] [test] `test_oauth_callback_github.py::test_repo_fetch_failure_records_unknown_flag` — `GET /installation/repositories` returns 500 → `selected_repositories=NULL`, audit `context.selected_repositories_unknown=true`.
- [ ] T050 [P] [US2] [test] `test_oauth_callback_github.py::test_setup_action_update_refreshes_repos_only` — `setup_action='update'` callback → refreshes `selected_repositories` only, audit `action='update'`, no new install audit.

**Checkpoint**: US2 ships independently. Customers can self-serve install; rows are written correctly. US1 + US2 together = "operator registers App once, customers install, deliveries flow."

---

## Phase 5: User Story 3 — Single App-Level Webhook Secret with Payload-Based Tenant Routing (Priority: P1)

**Goal**: A single App-level webhook secret verifies EVERY delivery; per-tenant isolation is structural via `installation.id → tenant_id` resolution.

**Independent Test**: Two tenants installed. Deliveries signed with the App secret carrying tenant A's `installation.id` produce only-A observations; with tenant B's `installation.id`, only-B; with a non-existent `installation.id`, 401 unknown_installation; with a forged signature, 401 signature_mismatch.

**Note**: T023 (FR-007 secret-loader extension) already shipped in US1 because US1 cannot test signature verification without it. US3 is a posture-assertion phase that exercises the cross-tenant routing scenarios end-to-end.

- [ ] T051 [P] [US3] [test] `test_router_github_integration.py::test_cross_tenant_routing_two_tenants_isolated` — seed tenant A and tenant B installations. POST delivery signed with App secret + tenant A's `installation.id` → observation under tenant A only (zero rows under tenant B). Repeat with tenant B's id → observation under tenant B only.
- [ ] T052 [P] [US3] [test] `test_router_github_integration.py::test_forged_signature_401_no_tenant_consulted` — sign with wrong secret → 401 `signature_mismatch`, zero observations, zero tenant-resolver calls (assert via mock or counter).
- [ ] T053 [P] [US3] [test] `test_router_github_integration.py::test_unknown_installation_id_401` — App secret valid + installation_id not in `provider_installations` → 401 `unknown_installation`, zero observations.
- [ ] T054 [P] [US3] [test] `test_router_github_integration.py::test_disabled_installation_collapses_to_unknown` — installation_id exists with `enabled=FALSE` → 401 `unknown_installation` (same outcome as never-registered; FR-005 of IN-07 holds).
- [ ] T055 [P] [US3] [test] `test_router_github_integration.py::test_no_raw_installation_id_in_logs` — across all four scenarios (T051-T054), capture structlog output; assert no log line contains the literal `installation.id` string (only `installation_id_hash` short-hash).

**Checkpoint**: US3 ships independently — the security posture is verified end-to-end. The App-level secret model is the only path; no per-installation secret code exists to maintain.

---

## Phase 6: User Story 4 — Uninstall Detection via Dual Signal (Priority: P2)

**Goal**: Either an inbound `installation.deleted` webhook OR an outbound 404/401 chokepoint disables the row and writes an audit. Both signals are idempotent and converge on the same private function.

**Independent Test**: Seed an enabled installation. Path A: POST signed `installation.deleted` → row disabled. Reset. Path B: stub outbound `mint_installation_token` to return 404 with the apps-not-found doc_url → identical row-disabled outcome. Path C: fire both concurrently → identical final state + ≤ 2 audit rows + no exception.

- [ ] T056 [US4] Create `services/integrations/github/uninstall.py::_disable_installation_github` per `contracts/module-github-lifecycle.md`. Atomic: `UPDATE provider_installations SET enabled=FALSE WHERE id=$1`; `INSERT installation_audit_log (action, status, context)` with `context.reason=<caller-supplied>`; cache-invalidate the installation access token (if cache provided and entry exists). Lock-free; double-fire is documented behavior. **Does NOT touch any encrypted_secrets row** (FR-012).
- [ ] T057 [US4] Create `services/integrations/github/lifecycle.py::dispatch` per `contracts/module-github-lifecycle.md`. Dispatch table:
  - `('installation', 'created')` → no-op if row exists; raise ValidationError if not.
  - `('installation', 'deleted')` → `_disable_installation_github(reason='installation_deleted_webhook')`.
  - `('installation', 'suspend')` → same as `deleted` in effect, `audit_action='suspend'`.
  - `('installation', 'unsuspend')` → `UPDATE provider_installations SET enabled=TRUE`, audit `action='unsuspend'`.
  - `('installation_repositories', *)` → routed to T067 in US5.
- [ ] T058 [US4] Modify `services/webhooks/router.py::receive` GitHub branch — add a lifecycle event check AFTER signature verification AND tenant-resolution AND replay-cache pass-through. If `X-GitHub-Event ∈ {installation, installation_repositories}`, hand off to `services.integrations.github.lifecycle.dispatch(...)` and return the dict-encoded response with HTTP 200. The existing handler at `services/ingestion/handlers/github.py` is NOT invoked for lifecycle events. (Mirrors the existing Slack lifecycle branch.)
- [ ] T059 [US4] Wire the outbound chokepoint into `GithubClient`. Inside `_maybe_disable_on_revocation`, on R2 trigger conditions, call `_disable_installation_github(reason='outbound_401_or_404_chokepoint')` exactly once per coroutine (idempotent on DB row), then raise `GithubApiError`.
- [ ] T060 [US4] [test] `services/integrations/tests/test_lifecycle_github.py::test_installation_deleted_disables_row_and_keeps_secret` — POST signed `installation.deleted` → `enabled=FALSE`, audit `action='uninstall'`, **the App-level secret row in `encrypted_secrets` is still present** (verify by direct DB read).
- [ ] T061 [P] [US4] [test] `test_lifecycle_github.py::test_installation_suspend_disables_unsuspend_enables_roundtrip` — POST `suspend` → FALSE, POST `unsuspend` → TRUE, two audit rows recorded.
- [ ] T062 [P] [US4] [test] `test_lifecycle_github.py::test_installation_created_existing_row_noop` — pre-seed installation row; POST `installation.created` → no row change, audit row `action='installation_created_noop'`.
- [ ] T063 [P] [US4] [test] `test_lifecycle_github.py::test_installation_created_missing_row_raises` — no pre-seed; POST `installation.created` → 400/401 path (the marketplace direct-install case; FR-009 final clause).
- [ ] T064 [P] [US4] [test] `services/integrations/tests/test_uninstall_github.py::test_outbound_404_apps_doc_url_disables_row` — stub `mint_installation_token` HTTP to return 404 with the apps-not-found doc_url → row disabled, audit row, GithubApiError propagated.
- [ ] T065 [P] [US4] [test] `test_uninstall_github.py::test_outbound_401_bad_credentials_disables_row` — analogous, 401 path.
- [ ] T066 [US4] [test] `test_uninstall_github.py::test_concurrent_uninstall_is_idempotent_under_race` — `asyncio.gather` the inbound dispatch path AND the outbound chokepoint path simultaneously → final state `enabled=FALSE`, exactly 2 audit rows (one per path), no exception. Verifies SC-005.
- [ ] T067 [P] [US4] [test] `test_uninstall_github.py::test_post_disable_inbound_delivery_returns_unknown_installation` — after disable, POST signed delivery for same `installation_id` → 401 `unknown_installation`, zero observations.

**Checkpoint**: US4 ships independently. The dual-signal uninstall model is verified — inbound webhooks, outbound chokepoints, and concurrent races all converge correctly.

---

## Phase 7: User Story 5 — Per-Repository Selection Enforced on Deliveries (Priority: P2)

**Goal**: Webhook deliveries for repositories outside the installation's `selected_repositories` allowlist are dropped with HTTP 200 + `handled='filtered_repo'` + metric increment. The allowlist mutates via `installation_repositories.added`/`removed` events.

**Independent Test**: Install with `selected_repositories=['org/a']`. Deliver `pull_request` for `org/c` → 200 dropped. Deliver `installation_repositories.added` adding `org/c` → allowlist updated. Re-deliver `pull_request` for `org/c` → 201 + observation.

- [ ] T068 [US5] Extend `services/integrations/github/lifecycle.py::dispatch` to handle `('installation_repositories', 'added' | 'removed')` per `contracts/module-github-lifecycle.md`. Mode-flip logic: if payload root `repository_selection='all'` and column is non-NULL → set to NULL; if `repository_selection='selected'` and column is NULL → seed to `[]`. Then merge added/removed lists. Idempotent on repo full-names. Single audit row per webhook.
- [ ] T069 [US5] Modify `services/webhooks/router.py::receive` GitHub branch — AFTER lifecycle dispatch check (so lifecycle events skip this), AFTER tenant resolution, BEFORE ingestion: read `selected_repositories` from the resolver's row (or a per-delivery DB read), check `payload.repository.full_name`, short-circuit with HTTP 200 + `{handled: 'filtered_repo'}` + `github_webhook_filtered_repo_total{reason='not_selected'}` increment if not in the list. If column is NULL, skip the check (all-repos mode).
- [ ] T070 [P] [US5] [test] `test_lifecycle_github.py::test_installation_repositories_added_seeds_from_null` — column NULL → POST `installation_repositories.added` with `repository_selection='selected'` and `repositories_added=[{full_name:'org/a'}]` → column now `['org/a']`.
- [ ] T071 [P] [US5] [test] `test_lifecycle_github.py::test_installation_repositories_added_idempotent` — re-add same repo → list unchanged, audit row written.
- [ ] T072 [P] [US5] [test] `test_lifecycle_github.py::test_installation_repositories_removed_drops_repo` — `['org/a','org/b']` → remove `org/a` → `['org/b']`.
- [ ] T073 [P] [US5] [test] `test_lifecycle_github.py::test_repository_selection_flip_to_all_sets_null` — payload root `repository_selection='all'` → column NULL.
- [ ] T074 [US5] [test] `test_router_github_integration.py::test_repo_filter_drops_unlisted` — `selected_repositories=['org/a']`, POST delivery for `org/c` → 200 `handled='filtered_repo'`, zero observations, metric increments.
- [ ] T075 [P] [US5] [test] `test_router_github_integration.py::test_repo_filter_null_allows_all` — `selected_repositories=NULL`, POST delivery for `org/anything` → 201 + observation.
- [ ] T076 [P] [US5] [test] `test_router_github_integration.py::test_repo_added_then_delivered` — full round-trip: deliver for `org/c` (dropped), POST `installation_repositories.added` for `org/c` (200), re-deliver for `org/c` (201 + observation).

**Checkpoint**: US5 ships independently. Repository selection is honored on every delivery; lifecycle mutations are reflected on the next delivery.

---

## Phase 8: User Story 6 — Replay Protection (Priority: P3)

**Goal**: A re-delivered `(installation_id, X-GitHub-Delivery)` pair within 5 minutes is dropped with HTTP 200 + zero post-commit work, even though the observation-layer dedup would also catch it.

**Independent Test**: POST signed delivery → 201, observation + trigger_queue row. POST same delivery again within 60 s → 200 `handled='replay'`, zero new observations, zero new trigger_queue rows. Wait 5 minutes (or fast-forward); POST again → processed normally + deduped at observation layer.

- [ ] T077 [US6] Create `services/integrations/github/replay_cache.py::ReplayCache` per data-model.md. `OrderedDict`-backed TTL LRU keyed on `(installation_id, delivery_id)`. Public methods: `seen(installation_id, delivery_id, now) -> bool` (atomic insert-and-return-prior-seen), `bypass_count` (for the cache-failure-fallback metric). Singleton factory `make_replay_cache(max_entries=4096, ttl_seconds=300.0)`; instance lives on `request.app.state.github_replay_cache` initialized in the gateway lifespan.
- [ ] T078 [US6] Wire `replay_cache.seen(...)` into `services/webhooks/router.py::receive` GitHub branch — AFTER signature verification succeeds AND BEFORE tenant-resolution outcome enforcement (per Clarifications Q4 ordering). On hit → HTTP 200 `{handled: 'replay'}` + `github_webhook_replay_dropped_total` increment. On miss → proceed.
- [ ] T079 [US6] Wire the singleton into the gateway lifespan in `services/gateway/main.py` — initialize on startup, no teardown needed (in-process state).
- [ ] T080 [P] [US6] [test] `services/integrations/tests/test_replay_cache_github.py::test_first_call_misses_second_hits` — first `seen(...)` returns False; second within TTL returns True.
- [ ] T081 [P] [US6] [test] `test_replay_cache_github.py::test_ttl_expiry_releases_key` — after TTL, same key returns False.
- [ ] T082 [P] [US6] [test] `test_replay_cache_github.py::test_lru_eviction_over_capacity` — fill to `max_entries`, insert one more → oldest entry evicted.
- [ ] T083 [P] [US6] [test] `test_replay_cache_github.py::test_missing_delivery_id_bypasses_cache` — `delivery_id=None` → `seen()` returns False without inserting; `bypass_count` increments.
- [ ] T084 [US6] [test] `test_router_github_integration.py::test_replay_short_circuit_within_5_min` — POST → 201; POST same `X-GitHub-Delivery` 60 s later → 200 `handled='replay'`, zero new observations, zero new trigger_queue rows.
- [ ] T085 [P] [US6] [test] `test_router_github_integration.py::test_replay_cache_expired_falls_back_to_observation_dedup` — POST → 201; fast-forward TTL+1 (test uses a fake clock injected into the cache); POST again → processed, deduped at observation layer (200 `deduped=true`).

**Checkpoint**: US6 ships independently. Re-deliveries don't trigger duplicate post-commit work.

---

## Phase 9: User Story 7 — Operational Observability (Priority: P3)

**Goal**: All metric counters from FR-017 are observable and non-decreasing; log lines carry `installation_row_id` and `installation_id_hash` instead of raw `installation_id`.

**Independent Test**: Run a synthetic workload (100 valid, 10 signature-fail, 5 replays, 20 repo-filtered, 1 deleted). Scrape metrics; assert per-counter floors. Grep logs; assert no raw `installation_id` appears.

- [ ] T086 [US7] Create `services/integrations/github/metrics.py` with all counters defined in FR-017. Aggregate-only labels per Clarifications Q5. Each counter has a documented module-level helper (`record_received()`, `record_verified(result)`, etc.) so callers don't construct Prometheus objects directly. Test-only `noop_metrics()` returns no-op variants.
- [ ] T087 [US7] Wire metrics into `services/webhooks/router.py` GitHub branch — `received_total` on entry, `verified_total{result}` after signature step, `signature_failure_total{reason}` on signature failure, `replay_dropped_total` on replay hit, `replay_cache_bypass_total` on `seen()` exception path, `filtered_repo_total{reason}` on filter hit, `lifecycle_total{event,action}` on lifecycle dispatch.
- [ ] T088 [US7] Wire metrics into `services/integrations/github/oauth.py::callback_handler` — `install_callback_total{outcome}` for each terminal outcome.
- [ ] T089 [US7] Wire metrics into `services/integrations/github/client.py` — `installation_token_mint_total{result}` on every `mint_installation_token` exit; `outbound_request_total{path,status}` on every outbound; `outbound_chokepoint_total{reason}` when the chokepoint fires.
- [ ] T090 [US7] Add structured-logging helpers: `_log_with_installation(log, installation_row_id, installation_id, **extra)` that emits `installation_id_hash` (BLAKE2b 8-byte hex) instead of `installation_id`. Audit all log call sites in `services/integrations/github/` to use this helper (or pass `installation_row_id` directly). No raw `installation_id` strings in any log call.
- [ ] T091 [US7] [test] `test_router_github_integration.py::test_metrics_increment_synthetic_workload` — run the synthetic workload, scrape the Prometheus registry via the test helper, assert each counter family is present and incremented to the documented floors.
- [ ] T092 [P] [US7] [test] `test_router_github_integration.py::test_no_raw_installation_id_in_any_log` — capture structlog output across 100 deliveries; grep for the literal `installation.id` value; assert zero matches (must only see the 8-byte hex hash).
- [ ] T093 [P] [US7] [test] `test_router_github_integration.py::test_installation_id_hash_is_blake2b_8byte` — log a delivery; parse one log line; assert `installation_id_hash` is a 16-char hex string (8 bytes BLAKE2b digest).

**Checkpoint**: US7 ships independently. Operability is verified.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, regression coverage of IN-08 and IN-09 (SC-011), and final sweep.

- [ ] T094 Update `CODEBASE-ARCHITECTURE.md` — append §17 documenting IN-13 (mirror §15 IN-09 + §16 IN-12 shapes). Cover: new package layout, new migration, App-level secret model, dual-signal uninstall, replay cache.
- [ ] T095 Run the **full IN-08 test suite** (`pytest -m integration services/integrations/tests/test_*_slack.py services/webhooks/tests/test_verifier_slack*`) with no test file modifications. Assert all pass. (SC-011 verification — zero changes to Slack code path.)
- [ ] T096 Run the **full IN-09 test suite** (`pytest -m integration services/integrations/tests/test_*_discord.py services/webhooks/tests/test_verifier_discord*`) with no test file modifications. Assert all pass. (SC-011 verification — zero changes to Discord code path.)
- [ ] T097 Run `python scripts/check_schema_drift.py` — assert zero drift after migration 0042 is applied. (SC-010 verification.)
- [ ] T098 [P] Run `git diff --stat main...feat/IN-13-github-integration` — visually verify SC-011 file-list constraint (only files listed under plan.md's "Changed files" section + the new `services/integrations/github/` package + the new migration + the new spec docs + tests).
- [ ] T099 [P] Add a `quickstart.md`-derived smoke test `scripts/dogfood_github_smoke.sh` (or document the manual steps in quickstart.md) — operator can run it after a fresh deploy to verify the integration end-to-end against a sandbox GitHub App.
- [ ] T100 Final review of FR-001 through FR-022 — for each functional requirement, confirm the implementing task(s) ship the behavior and the test(s) cover it. Document any unmapped FR in a final spec-update commit (which should be zero).

---

## Dependencies

```
Phase 1 (Setup, T001–T006)
   ↓
Phase 2 (Foundational: JWT + Client, T007–T022)  ← BLOCKING for US2 and US4
   ↓
   ├─ Phase 3 (US1, T023–T032)  ← T023 is shared infra used by US1+US3, ships in US1
   ├─ Phase 4 (US2, T033–T050)  ← can run parallel to US1 once Phase 2 ships
   ├─ Phase 5 (US3, T051–T055)  ← depends on US1's T023 secret-loader
   ├─ Phase 6 (US4, T056–T067)  ← depends on Phase 2's client + chokepoint integration
   ├─ Phase 7 (US5, T068–T076)  ← depends on US4's T057 lifecycle.dispatch
   ├─ Phase 8 (US6, T077–T085)  ← can run parallel to US4/US5 once US1 ships
   └─ Phase 9 (US7, T086–T093)  ← can run parallel to US1–US6 (instrumentation only)

Phase 10 (Polish, T094–T100)  ← LAST: regression + drift + docs
```

## Parallel Execution Examples

**Phase 2 (Foundational)** — once T007 (errors) and T008 (jwt) are in, the test suite parallelizes:
```bash
pytest services/integrations/tests/test_jwt_github.py    # T009-T012 all [P]
pytest services/integrations/tests/test_client_github.py # T014-T022 all [P]
```

**Phase 3 (US1)** — once T023+T024 are in, US1 test files parallelize:
```bash
pytest services/webhooks/tests/test_verifier_github.py     # T025-T027 all [P]
pytest services/integrations/tests/test_router_github_integration.py::test_verified_*  # T029-T032 all [P]
```

**Phase 4 (US2)** — once T034+T035 are in, the test suite parallelizes heavily:
```bash
pytest services/integrations/tests/test_oauth_install_github.py    # T040-T041 [P]
pytest services/integrations/tests/test_oauth_callback_github.py   # T043-T050 [P]
```

**Phase 6+7 (US4 + US5)** — can develop and test in parallel once Phase 2 is in:
- Developer A: T056-T067 (US4 uninstall)
- Developer B: T068-T076 (US5 repo filter) — note T068 depends on T057 being merged

**MVP Scope**: Phases 1+2+3 (Setup + Foundational + US1) deliver the ingestion contract against operator-seeded rows. Phase 4 (US2) is required for self-serve onboarding. Phase 5 (US3) ships as posture-validation tests with no new code beyond T023. Phases 6–9 are quality-of-implementation phases that can ship behind feature gates if desired (US4 + US5 SHOULD ship together; US6 + US7 are safe to ship in a follow-up PR).

## Implementation Strategy

1. **Slice 1 (MVP, ~1 day)**: T001–T006 (setup) + T007–T013 (errors + JWT + client skeleton) + T023–T024 (secret loader). Operator can manually seed an installation row and verify deliveries land via T028.
2. **Slice 2 (Self-Serve, ~1 day)**: T033–T038 (oauth wiring) + T039–T050 (oauth tests). Customers can self-serve install.
3. **Slice 3 (Posture, ~half day)**: T051–T055 (US3 cross-tenant + secret-misuse tests). No new code, validates Slice 1+2 jointly.
4. **Slice 4 (Lifecycle + Uninstall, ~1 day)**: T056–T067 (US4) + T068–T076 (US5). Lifecycle webhooks flow correctly; repo selection enforced.
5. **Slice 5 (Replay + Obs, ~half day)**: T077–T085 (US6) + T086–T093 (US7). Production-grade defense-in-depth + observability.
6. **Polish (~half day)**: T094–T100. Regression confirmation + docs + drift check.

**Estimated end-to-end**: ~4 working days for a single engineer; parallelizable to ~2 calendar days with two engineers (one on US1+US2+US3, one on Phase 2 + US4+US5+US6).
