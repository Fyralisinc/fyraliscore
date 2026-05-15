# Implementation Plan: GitHub Production Integration — App Install, Webhook Ingest, Single App-Level Secret with Tenant Routing, Uninstall Chokepoint

**Branch**: `feat/IN-13-github-integration` | **Date**: 2026-05-15 | **Spec**: [./spec.md](./spec.md)
**Input**: Feature specification from [./spec.md](./spec.md)

## Summary

After IN-08 closed the Slack production integration with a generic, reusable substrate (`lib/shared/secrets`, `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, the `build_integrations_router()` factory) and IN-09 ported the pattern to Discord, IN-13 extends that substrate to GitHub — the third OAuth-installing provider — with one critical shape difference: **a single App-level webhook secret** (Clarifications Q1) instead of per-installation secrets. The shape change is structural, not procedural: GitHub Apps publish exactly one webhook secret in developer settings, and per-tenant isolation is achieved via `installation.id` payload-based routing through the existing `services/webhooks/tenant_resolver.py::_extract_github` extractor.

This task adds:

- A GitHub-side App install/callback path under `/integrations/github/*` mounted into the existing `services/integrations/router.py` factory.
- A new `services/integrations/github/` package with `oauth.py`, `jwt.py` (App-JWT minting), `client.py` (outbound REST + installation-access-token cache), `uninstall.py` (chokepoint without secret zeroization), `lifecycle.py` (handles `installation` and `installation_repositories` webhook events), `metrics.py`.
- A lifecycle-event routing branch in `services/webhooks/router.py` parallel to the existing Slack `app_uninstalled`/`tokens_revoked` branch — intercepts `installation` and `installation_repositories` events BEFORE the ingestion handler is called.
- An in-process replay LRU keyed on `(installation_id, X-GitHub-Delivery)` with 5-minute TTL inserted between signature verification and ingestion (Clarifications Q4).
- A per-installation `selected_repositories` allowlist read from a new JSONB column added by migration `0042_provider_installations_selected_repositories.sql` (Clarifications Q2) and enforced at the router layer before ingestion.

**Existing assets that are reused unchanged:**

- `services/webhooks/signatures/github.py::GitHubVerifier` — already iterates over the list returned by `load_secrets(...)` for rotation overlap. We extend `services/webhooks/secrets.py` so that the GitHub branch returns the App-level secret as a single-element (or two-element during rotation) list.
- `services/ingestion/handlers/github.py` — its `_EVENT_SHAPERS` for `pull_request`, `push`, `issues`, `issue_comment`, `pull_request_review`, `check_run` are correct and stable; this task does NOT modify them. The lifecycle events `installation` / `installation_repositories` are intercepted at the router layer BEFORE the handler is invoked, so the handler's existing `ValidationError("unsupported github event type")` for those events is never reached in practice but is preserved as a defensive backstop.
- `services/webhooks/router.py` — modified minimally: add the replay-cache check after signature verification, add the GitHub lifecycle-event branch parallel to the Slack one, add the repo-filter check before ingestion. No changes to the Slack/Discord/Linear/Stripe paths.
- `services/webhooks/tenant_resolver.py::_extract_github` — already extracts `installation.id` from the payload. Used unchanged.

**One new migration.** `0042_provider_installations_selected_repositories.sql` is a single `ALTER TABLE provider_installations ADD COLUMN selected_repositories JSONB NULL` with `IF NOT EXISTS` semantics (Constitution §II). No new tables.

**Zero changes to `services/integrations/slack/*` and `services/integrations/discord/*`** (FR-019 / SC-011 — verified by re-running both IN-08 and IN-09 suites as part of IN-13 CI).

The OAuth flow mirrors IN-08 and IN-09's exactly: signed state token bound to the authenticated tenant via HMAC over `{tenant_id, nonce, expiry_ts}` with single-use enforcement via atomic `UPDATE oauth_install_states ... WHERE consumed_at IS NULL RETURNING ...`. The cross-tenant collision detection (US2.4) reuses the same `ON CONFLICT` shape. The uninstall chokepoint (US4) converges inbound `installation.deleted` and outbound 404/401 onto a single private function `_disable_installation_github`, structurally similar to IN-09 but **without secret deletion** (the App-level secret is shared and must outlive any single tenant's uninstall).

## Technical Context

**Language/Version**: Python 3.11+ (project uses 3.12 in `.venv`).

**Primary Dependencies**:
- **PyJWT** (`jwt.encode` / RS256 via `cryptography`) — required for App-JWT minting (FR-011). PyJWT is NOT currently a project dependency. The project already vendors `cryptography` (used by `lib/shared/secrets/fernet.py` and PyNaCl indirectly), so we have the underlying primitives. **Decision**: add `pyjwt[crypto]>=2.8` to `pyproject.toml` (one new direct dependency; resolved in research.md R1).
- **httpx** — async HTTP client for the GitHub REST API (already in project for Slack/Discord OAuth).
- **asyncpg** — DB driver, factory-injected via `request.app.state.pool`.
- **cryptography** — already in project; used here to load the PEM private key for JWT signing (`cryptography.hazmat.primitives.serialization.load_pem_private_key`).
- **FastAPI** — `APIRouter` extended within `build_integrations_router()`.

**Storage**: Postgres 16 + pgvector — reused tables (`provider_installations`, `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `observations`). One new column on `provider_installations` (`selected_repositories JSONB NULL`).

**Testing**: pytest with `integration` marker (live Postgres + Ollama per Constitution §IV). `respx` for mocking `api.github.com` and `github.com` HTTP calls in unit and integration tests. No mocking of the Postgres or `lib/shared/secrets` boundary.

**Target Platform**: Linux server (docker-compose deploy).

**Project Type**: Web service (backend-only for this task; UI is separate).

**Performance Goals**:
- US1 webhook delivery ingest p95 ≤ 1.5 s wall (GitHub does not enforce a hard ack window like Discord, but it retries on >5xx; we keep p95 well under 3 s).
- US2 OAuth callback wall time ≤ 2 s under live GitHub mocks (state-token consume + `GET /installation/repositories` round-trip + INSERT + redirect).
- Replay-cache lookup ≤ 1 ms (in-process LRU; no DB).
- App-JWT mint + installation-token exchange ≤ 500 ms (single round-trip to GitHub; token is cached for ~1 h).
- Uninstall chokepoint ≤ 50 ms (no row lock; same posture as IN-08 / IN-09).

**Constraints**:
- App private key is read from env on every JWT mint (FR-020) — no in-process cache that would block rotation. Parse cost (`load_pem_private_key`) is ~1 ms in CPython; acceptable.
- The single App-level webhook secret loader must support **rotation overlap** via the existing `GitHubVerifier` multi-secret iteration. We extend `services/webhooks/secrets.py` to return both the current and previous secrets when a `GITHUB_APP_WEBHOOK_SECRET_PREV` env var (or secret-store entry) is set.
- `FYRALIS_ENV=prod` + `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` MUST fail-fast at startup (FR-007 continuation of IN-08's `assert_prod_safety_invariants()` — we extend the assertion to require either `GITHUB_APP_WEBHOOK_SECRET_REF` (secret store) OR `GITHUB_WEBHOOK_SECRET` + the explicit env-fallback flag).
- Replay-cache memory cap: 4096 entries × ~64 bytes per entry ≈ 256 KB. Negligible.

**Scale/Scope**: Per-tenant GitHub installs are expected to number in the low hundreds in steady state (GitHub orgs in the deployment's customer base). Per-installation webhook delivery rate is typically <1/sec for an active dev team; the platform must absorb periodic 10×–100× bursts (e.g., a CI tool generating bulk `check_run` events). The replay-cache size of 4096 entries gives ≥4096 / 1Hz / 300s × 1 customer ≈ 13 customers of headroom at the burst rate before LRU eviction — adequate for v1; revisit if customer count × burst rate exceeds.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-evaluated at end of Phase 1.*

| Principle | Status | Notes |
|---|---|---|
| §I Four Foundations distinct | PASS | GitHub webhook events land as **Observations** with `kind ∈ {signal, state_change}`, `trust_tier ∈ {authoritative, inferential}` per the existing handler's per-event shaping. No new Model / Act / Resource writes. `provider_installations` is a per-feature side table for tenant routing — explicitly permitted under §I. |
| §II Append-only migrations | PASS | One new migration: `0042_provider_installations_selected_repositories.sql` — single idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS selected_repositories JSONB`. No existing migration is edited. The deduplication unique index on `observations(source_channel, external_id, occurred_at)` already enforces FR-001 idempotency by construction. |
| §III Tenant isolation structural | PASS | No new tenant-scoped tables. The new `selected_repositories` column on `provider_installations` inherits the table's existing RLS + tenant_id FK + tenant-prefixed index from migration 0039. All new queries written with hand-rolled `WHERE tenant_id = $1` paired with `current_setting('app.current_tenant')` via `tenant_transaction`. |
| §IV Integration tests, real DB | PASS | Plan mandates live Postgres for all `services/integrations/github/tests/test_*.py` files; `respx` only for `api.github.com` HTTP mocks. The `lib/shared/secrets/` boundary is real Fernet via real Postgres. |
| §V Reasoning vs rendering | N/A | This task is integration plumbing — no Think or Rendering changes. The Observations produced will trigger Think downstream via the existing `think_trigger_queue` plumbing in `services/ingestion/core.py::ingest()`. |
| §VI Trust/confidence/falsifiers | PASS | Observations from GitHub carry `trust_tier` per the existing handler's matrix (authoritative for merges/check_runs/approved-reviews/push/issue-state-change; inferential for comments and non-approved reviews). No Model writes, so no falsifier obligations. |
| §VII Determinism + audit | PASS | Every install / reinstall / uninstall / suspend / unsuspend / repo_change writes an `installation_audit_log` row. `uuid7()` for every new substrate row (`oauth_install_states`, `installation_audit_log`). Observation idempotency via the existing UNIQUE index. Replay-cache is in-process only (deliberate — FR-014 documents it as a defense-in-depth layer, not a correctness gate). |
| §VIII Structured errors | PASS | New exception classes derive from the existing IN-08/09 hierarchy: `GithubOAuthError`, `GithubApiError`, `GithubJWTError`. Existing `InstallationCollisionError`, `StateTokenInvalidError`, `SecretStoreError`, `SecretNotFoundError` are reused unchanged. The existing `GithubSignatureError` from `services/ingestion/handlers/github.py` is preserved. |
| §IX Dual-write until proven | PASS — N/A | The new `selected_repositories` column has no existing data and no parallel-write substitute; it is a clean addition. Reader (router) and writer (oauth callback + lifecycle handler) are introduced together in this PR. The column defaults to `NULL` meaning "all repositories" so a deploy that adds the column but lags on writes does NOT break existing installations. |
| §X Simplicity / YAGNI | PASS | Single global App. No per-org App. No GraphQL. No GHES. No user-level OAuth. No outbound product features (only the chokepoint-driver outbound call). No Redis. No durable replay cache. Each item is explicitly deferred to a follow-up with rationale in spec's Out-of-Scope. |

**No NON-NEGOTIABLE violations.** Complexity Tracking table below is empty.

### Complexity Tracking

(none — no deviations from the constitution require justification)

## Project Structure

### Documentation (this feature)

```text
specs/IN-13-github-integration/
├── plan.md              # This file
├── research.md          # Phase 0: dependency confirmations, mechanism choices
├── data-model.md        # Phase 1: entities, column additions, label conventions
├── quickstart.md        # Phase 1: end-to-end install + delivery + uninstall flow
├── contracts/           # Phase 1: HTTP route specs + module contracts
│   ├── http-integrations-github.md
│   ├── http-webhooks-github-events.md
│   ├── module-github-client.md
│   └── module-github-lifecycle.md
└── tasks.md             # Phase 2: produced by /speckit-tasks (NOT this file)
```

### Source Code (repository root)

New files (all under `services/integrations/github/`):

```text
services/integrations/github/
├── __init__.py
├── oauth.py            # GET install / GET callback handlers + state-token + cross-tenant collision
├── jwt.py              # App-JWT mint (RS256) from env-supplied private key; per-call, no key cache
├── client.py           # async outbound GitHub REST client; installation-access-token cache (in-process)
├── lifecycle.py        # handle installation.{created,deleted,suspend,unsuspend} + installation_repositories.{added,removed}
├── uninstall.py        # _disable_installation_github (idempotent disable; NO secret deletion — App-level secret is shared)
├── replay_cache.py     # in-process LRU keyed on (installation_id, delivery_id), 5-minute TTL
└── metrics.py          # github_webhook_* + github_install_* + github_installation_token_mint_total counters
```

Plus the colocated tests:

```text
services/integrations/tests/    (existing shared dir from IN-08/09)
├── test_jwt_github.py                  # RS256 mint + private-key rotation behaviour + missing-key error
├── test_oauth_install_github.py        # 302 to github.com/apps/<slug>/installations/new with state
├── test_oauth_callback_github.py       # callback: state consume + UPSERT row + GET /installation/repositories + audit + redirect; collision; state-token failures
├── test_lifecycle_github.py            # installation.{created,deleted,suspend,unsuspend} + installation_repositories.{added,removed}; idempotency
├── test_uninstall_github.py            # outbound 404/401 → disable + concurrent-uninstall idempotency; inbound + outbound convergence
├── test_client_github.py               # 200 / 401 / 404 / 429 paths + installation-token cache hit/miss + private-key rotation transparency
├── test_replay_cache_github.py         # cache hit / miss / TTL expiry / capacity LRU eviction / cache-bypass on failure
└── test_router_github_integration.py   # end-to-end: ping → 200; verified delivery → observation; replay → 200 dropped; repo-filter → 200 dropped; lifecycle event → handled (no observation)
```

Changed files:

```text
services/integrations/router.py         # add /integrations/github/install + /callback sub-routes
services/gateway/main.py                # add /integrations/github/callback to _PUBLIC_PATHS exact-match set
services/webhooks/router.py             # add the github lifecycle branch + replay-cache + repo-filter, parallel to slack lifecycle
services/webhooks/secrets.py            # extend github branch to load GITHUB_APP_WEBHOOK_SECRET_REF (secret store) with GITHUB_WEBHOOK_SECRET env fallback + optional _PREV for rotation
services/gateway/main.py                # _assert_prod_safety_invariants extended for GITHUB_APP_WEBHOOK_SECRET_REF
services/ingestion/handlers/github.py   # ZERO logic changes; preserved as backstop for lifecycle events that bypass router-layer interception
lib/shared/errors.py                    # add GithubOAuthError, GithubApiError, GithubJWTError
db/migrations/0042_provider_installations_selected_repositories.sql  # NEW: single ALTER TABLE
pyproject.toml                          # add pyjwt[crypto]>=2.8
CODEBASE-ARCHITECTURE.md                # append §17 documenting IN-13 (mirror §15 IN-09 + §16 IN-12 shapes)
```

**Structure Decision**: Mirror IN-08/IN-09's directory layout under `services/integrations/github/`. Tests live in `services/integrations/tests/` (shared conftest). No top-level reorg; everything slots into the provider-namespaced shape IN-08 established.

## Phase Ordering (per Constitution §IX)

Constitution §IX mandates migrations → dual-write → reader cutover for substrate-shape changes on hot paths. IN-13 has **one new column** (`selected_repositories`) but no existing data, no parallel writer to coordinate, no read-path divergence to manage. The reader and writer are introduced together in this PR. Phase order collapses to functionality-first:

**Slice 1 (foundational, gated on migration)** — confirm reusable substrate is intact + add the new column:
- T001: Write `db/migrations/0042_provider_installations_selected_repositories.sql` — single `ALTER TABLE provider_installations ADD COLUMN IF NOT EXISTS selected_repositories JSONB DEFAULT NULL`. Include the migration's leading comment per Constitution §II.5 (this is additive, non-destructive — no staged plan required).
- T002: Verify `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `provider_installations` exist and have the expected columns / RLS / indexes from IN-08 migrations 0039 / 0040 / 0041 (read-only assertion test).
- T003: Verify `observations_source_channel_external_id_occurred_at_key` UNIQUE index exists.
- T004: Add `pyjwt[crypto]>=2.8` to `pyproject.toml`. Run `pip install -e .[dev]` in dev to refresh the venv. Add an import smoke test.

**Slice 2 (App-JWT + outbound client + token cache)** — outbound foundations needed by Slice 3's callback:
- T005: Create `services/integrations/github/jwt.py::mint_app_jwt(now: float | None = None) -> str` — reads `GITHUB_APP_ID` from env, reads private key from `GITHUB_APP_PRIVATE_KEY` (or `GITHUB_APP_PRIVATE_KEY_PATH` file content) on every call. Signs `{iat, exp=iat+600, iss=app_id}` with RS256. Raises `GithubJWTError(reason='no_private_key' | 'no_app_id' | 'malformed_key')` on misconfig.
- T006: `test_jwt_github.py::test_mint_smoke` — mint a JWT with a known test private key, verify signature with the matching public key.
- T007: `test_jwt_github.py::test_missing_key_raises` — unset both env vars → `GithubJWTError(reason='no_private_key')`.
- T008: `test_jwt_github.py::test_rotation_transparent` — set `GITHUB_APP_PRIVATE_KEY` to key A, mint → swap env to key B, mint again → new JWT verifies with key B (no in-process cache binding to key A).
- T009: Create `services/integrations/github/client.py::GithubClient` with `mint_installation_token(installation_id: str)` (POSTs to `/app/installations/{installation_id}/access_tokens` with the App JWT, returns `(token, expires_at)`). Token cache is `dict[str, CachedToken]` keyed on `installation_id`, evicts on expiry.
- T010: `GithubClient.list_installation_repositories(installation_id)` — GET `/installation/repositories` with a per-call freshly-resolved installation access token (cached). Returns `list[str]` of `<owner>/<repo>` full names. Pagination handled (GitHub default 30/page; for v1 read up to 3 pages = 90 repos and warn if more).
- T011: `test_client_github.py::test_install_token_mint_caches` — first call to `mint_installation_token` triggers POST; second call within TTL returns cached; expired entry triggers re-mint.
- T012: `test_client_github.py::test_401_triggers_chokepoint` — outbound 401 with `message=Bad credentials` triggers `_disable_installation_github` exactly once + raises `GithubApiError(reason='unauthorized')`.
- T013: `test_client_github.py::test_404_documentation_url_triggers_chokepoint` — outbound 404 with `documentation_url` matching the apps-not-found pattern triggers chokepoint.

**Slice 3 (US2: OAuth install + selected-repos seed)** — self-serve onboarding:
- T014: Create `services/integrations/github/__init__.py`, `oauth.py`. Implement `install_handler` (Bearer-authed): mint state token via the existing `services.integrations.oauth_state.issue_state_token` (rename from `services.integrations.slack.oauth.issue_state_token` if still slack-namespaced — see IN-09 Slice 3 T008's compatibility shim). Implement `callback_handler` (public): verify state, atomic nonce consume, UPSERT row, mint installation token, GET `/installation/repositories`, write `selected_repositories`, write audit row, redirect.
- T015: Mount `/integrations/github/install` (Bearer-authed) + `/integrations/github/callback` (public) in `services/integrations/router.py`.
- T016: Add `/integrations/github/callback` to `_PUBLIC_PATHS` exact-match set in `services/gateway/main.py`.
- T017: `test_oauth_install_github.py::test_302_to_app_install_url` — 302 to `https://github.com/apps/<slug>/installations/new?state=<token>`, state bound to authenticated tenant.
- T018: `test_oauth_callback_github.py::test_first_install` — end-to-end with respx-mocked GitHub: state consumed, UPSERT row, mock `POST /app/installations/<id>/access_tokens` returns a token, mock `GET /installation/repositories` returns `['org/a','org/b']`, `selected_repositories` is persisted as JSONB `["org/a","org/b"]`, audit row written, 302 to success page.
- T019: `test_oauth_callback_github.py::test_state_token_failures` — expired / invalid / consumed each routes to `/integrations/github/install-error?reason=...`.
- T020: `test_oauth_callback_github.py::test_cross_tenant_collision` — same `installation_id` already mapped to a different tenant → 302 to `install-error?reason=installation_collision`, audit row `status='rejected_collision'`, foreign tenant id absent from logs.
- T021: `test_oauth_callback_github.py::test_reinstall_same_tenant` — same `installation_id`, same tenant, prior `enabled=FALSE` → row id reused, `enabled=TRUE`, no duplicate row, audit row `action='reinstall'`.
- T022: `test_oauth_callback_github.py::test_repository_fetch_failure_does_not_block_install` — `GET /installation/repositories` returns 5xx → install still completes with `selected_repositories=NULL` (treated as "all" — same operational posture as a successful install whose admin granted all-repos access), audit row context records the fetch failure.

**Slice 4 (US3 + FR-007: Single App-level webhook secret loader extension)** — secret-loader change so webhook deliveries verify:
- T023: Extend `services/webhooks/secrets.py::load_secrets` GitHub branch to: (a) prefer the secret-store entry referenced by env var `GITHUB_APP_WEBHOOK_SECRET_REF` if set; (b) include `GITHUB_APP_WEBHOOK_SECRET_PREV_REF` (or `GITHUB_WEBHOOK_SECRET_PREV` env var) as a second list element to support rotation overlap; (c) fall back to `GITHUB_WEBHOOK_SECRET` env var if no ref is set AND `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`; (d) raise `SecretNotFoundError(provider='github')` otherwise. The `tenant_id` argument is ignored for GitHub (App-level, not per-tenant).
- T024: Extend `services/gateway/main.py::_assert_prod_safety_invariants` to require at least one of `GITHUB_APP_WEBHOOK_SECRET_REF` (any non-empty value) OR `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` paired with `GITHUB_WEBHOOK_SECRET` set, under `FYRALIS_ENV=prod`. Fail-fast at startup otherwise.
- T025: `services/webhooks/tests/test_verifier_github.py::test_db_backed_app_secret` (new file or extend existing) — secret-store entry resolves end-to-end via the new loader path.
- T026: `services/webhooks/tests/test_verifier_github.py::test_rotation_overlap` — secret A used to sign, secrets [A, B] loaded → verify OK; secrets [B, C] loaded (A retired) → same delivery fails (`signature_mismatch`).

**Slice 5 (US1 + US6: Webhook router — replay + ingest)** — wire it all together:
- T027: Create `services/integrations/github/replay_cache.py::ReplayCache` — `OrderedDict`-backed LRU with TTL, public methods `seen(installation_id: str, delivery_id: str, now: float) -> bool` (returns True and inserts on first call; True if hit-and-not-expired on subsequent calls until expiry; the seen-check + insert is atomic per Python's GIL).
- T028: `test_replay_cache_github.py` — first call returns False; second returns True; after TTL returns False; over-capacity evicts LRU; internal exception in `seen()` doesn't raise (returns False, increments `_bypass_count`).
- T029: Modify `services/webhooks/router.py::receive` GitHub branch to: (a) handle `X-GitHub-Event: ping` BEFORE the unknown-installation check (200 OK, no Observation, no replay-cache entry); (b) AFTER signature verification succeeds AND before tenant-resolution-outcome enforcement, call `replay_cache.seen(installation_id, delivery_id, now)` and short-circuit with HTTP 200 + `{handled: 'replay'}` if True; (c) on lifecycle events (`X-GitHub-Event ∈ {installation, installation_repositories}`), route to `services.integrations.github.lifecycle.dispatch(...)` instead of ingestion; (d) for non-lifecycle events, after tenant resolution, check `selected_repositories`; if not NULL and `payload.repository.full_name` not in the list, short-circuit with HTTP 200 + `{handled: 'filtered_repo'}`; (e) otherwise, call `ingest(...)` as today.
- T030: `test_router_github_integration.py::test_ping_returns_200_no_observation` — `X-GitHub-Event: ping`, valid signature, no `installation.id` → 200, zero observations.
- T031: `test_router_github_integration.py::test_verified_pull_request_lands_as_observation` — full happy path. Re-uses the existing handler's shaping; no changes.
- T032: `test_router_github_integration.py::test_replay_short_circuit` — second delivery with same `X-GitHub-Delivery` → 200 dropped, zero new observations.
- T033: `test_router_github_integration.py::test_repo_filter_drops_unlisted` — `selected_repositories=['org/a']`, delivery for `org/c` → 200 dropped, `github_webhook_filtered_repo_total` increments.
- T034: `test_router_github_integration.py::test_signature_failure_first` — invalid signature → 401 `signature_mismatch`, no tenant resolved, no replay-cache touched.

**Slice 6 (US4 + US5: Lifecycle handler + uninstall chokepoint)** — installation.* and installation_repositories.* events:
- T035: Create `services/integrations/github/uninstall.py::_disable_installation_github(pool, installation_row_id, *, reason: str, audit_status: str = 'ok')` — atomic `UPDATE provider_installations SET enabled=FALSE WHERE id=$1` + cached-installation-token-invalidate + `INSERT installation_audit_log`. No secret deletion (FR-012). Lock-free.
- T036: Create `services/integrations/github/lifecycle.py::dispatch(payload, tenant_id, installation_row_id, pool, secret_store)` — dispatch on `(event_type, action)`:
  - `('installation', 'created')`: no-op if row exists; raise `unknown_installation` if not (FR-009).
  - `('installation', 'deleted')`: call `_disable_installation_github(reason='installation_deleted_webhook')`.
  - `('installation', 'suspend')`: same as deleted in effect (`enabled=FALSE`).
  - `('installation', 'unsuspend')`: `UPDATE provider_installations SET enabled=TRUE`. Audit row `action='unsuspend'`.
  - `('installation_repositories', 'added')` / `('installation_repositories', 'removed')`: merge into `selected_repositories` (initialize from `NULL` if "all" → `repositories` from the payload's `repositories` field). Idempotent. Single audit row per webhook arrival with `context.{added, removed}` lists.
- T037: Wire the outbound chokepoint into `GithubClient` — on 401 `Bad credentials` OR 404 with apps-not-found `documentation_url`, call `_disable_installation_github(reason='outbound_401_or_404_chokepoint')` exactly once per Python coroutine then raise `GithubApiError(reason='unauthorized' | 'not_found')` upstream.
- T038: `test_lifecycle_github.py::test_installation_deleted_disables_row` — POST signed `installation.action=deleted` → row `enabled=FALSE`, audit row, secret NOT deleted (verify by hashing or by direct DB read of the App-secret store entry — still present).
- T039: `test_lifecycle_github.py::test_installation_suspend_disables_unsuspend_enables` — round-trip.
- T040: `test_lifecycle_github.py::test_installation_repositories_added_updates_allowlist` — add `org/c` → `selected_repositories` JSONB now contains it; subsequent delivery for `org/c` ingests (verifies router-side enforcement).
- T041: `test_lifecycle_github.py::test_installation_repositories_removed_updates_allowlist` — remove `org/a` → subsequent delivery for `org/a` is dropped with `filtered_repo`.
- T042: `test_lifecycle_github.py::test_lifecycle_dispatch_idempotent` — second `installation.deleted` for same installation is a no-op on the already-disabled row.
- T043: `test_uninstall_github.py::test_concurrent_uninstall_is_idempotent` — inbound webhook AND outbound 404 race; both await; ≤ 2 audit rows; final state enabled=FALSE; no exception escapes.

**Slice 7 (Polish + observability)** — metrics + docs + regression:
- T044: Create `services/integrations/github/metrics.py` with all counters from FR-017. Aggregate-only labels per Clarifications Q5.
- T045: `test_router_github_integration.py::test_metrics_increment` — synthetic workload exercises each counter (verified, signature_failure, replay_dropped, filtered_repo, lifecycle).
- T046: Update `CODEBASE-ARCHITECTURE.md` §17 documenting IN-13 (mirror §15 IN-09 + §16 IN-12 shapes).
- T047: **Re-run full IN-08 and IN-09 test suites** with no test file modifications to satisfy SC-011 (zero changes to Slack and Discord integration packages).

**No reader cutover phase, no dual-write phase beyond the single new column.**

## Risk Register

1. **App-level webhook secret rotation operationally**: Rotating the App webhook secret requires a coordinated two-step in GitHub's developer settings + Fyralis's secret-store: (1) deploy Fyralis with `[A, B]` loaded as `[GITHUB_APP_WEBHOOK_SECRET_REF, GITHUB_APP_WEBHOOK_SECRET_PREV_REF]`; (2) update GitHub's App settings to secret `B`; (3) wait for in-flight deliveries to drain; (4) remove `GITHUB_APP_WEBHOOK_SECRET_PREV_REF` from Fyralis env. The verifier's existing multi-secret iteration covers the overlap. **Mitigation**: documented in `quickstart.md`; tested in T026.
2. **App private key file vs env var**: Some operators prefer mounting the key as a file (Kubernetes Secret → file projection) rather than as an env var. **Mitigation**: support both via `GITHUB_APP_PRIVATE_KEY_PATH` (file path) as an alternative to `GITHUB_APP_PRIVATE_KEY` (multi-line PEM). Exactly one must be set at startup; both set is a fail-fast config error.
3. **Installation-access-token cache stampede**: Two concurrent outbound calls for the same `installation_id` whose cached token expired could each trigger a fresh `POST /access_tokens`. **Mitigation**: per-installation `asyncio.Lock` around the cache miss path. Bounded contention (low call rate; one lock per installation; locks evicted when entries expire).
4. **`selected_repositories=NULL` ambiguity**: NULL legitimately means "all repositories"; but a row created by the OAuth callback before `GET /installation/repositories` succeeded would also be NULL. **Mitigation**: the callback (T014/T022) records the fetch result distinctly: a successful list (even empty) is written as a JSONB array; a fetch failure is recorded as NULL with an audit-row context field `selected_repositories_unknown=true`. Operators reading audit logs can disambiguate.
5. **GitHub's at-least-once retry within ≤ 1 hour**: If an installation is disabled mid-delivery and a retry arrives, the retry must 401 `unknown_installation` cleanly without thrashing. **Mitigation**: tenant-resolver returns `UnknownInstallation` for disabled rows (already true in IN-07); the router returns 401 with no further work. Replay-cache TTL is 5 minutes so the retry doesn't get short-circuited as a replay either — it gets the 401 GitHub expects.
6. **`installation_repositories` payload size for large additions**: A customer adding 1000 repos in one click would deliver one webhook with a 1000-element `repositories` array. The 1 MB body cap from IN-01 likely accommodates this (GitHub's payload is bounded). **Mitigation**: cap `selected_repositories` JSONB column writes at 10000 elements with a structured-error log if exceeded (no silent truncation).

## Open Questions

(none — all locked in spec's Clarifications)
