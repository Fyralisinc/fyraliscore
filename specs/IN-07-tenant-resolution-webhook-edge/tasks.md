---

description: "Task list for IN-07 — Tenant Resolution at Webhook Edge"
---

# Tasks: IN-07 — Tenant Resolution at Webhook Edge

**Input**: Design documents from `specs/IN-07-tenant-resolution-webhook-edge/`
**Prerequisites**: [plan.md](./plan.md) (required), [spec.md](./spec.md) (required), [research.md](./research.md)

**Tests**: REQUIRED. The plan's Test Strategy table mandates 5 test modules
(extractor, lookup, cache, admin, security). Unit tests for extractor and
cache are co-located with their implementing tasks; integration tests for
admin, lookup, security run LAST per Constitution §IX migration-ordering
discipline and the orchestrator's strict ordering.

**Organization**: Tasks are ordered by the orchestrator's six-stage rule
(migration → resolver core → admin → metrics → rename → integration tests),
then mapped to user stories within each stage. Story labels (`[US1]`..`[US6]`)
track traceability back to the spec. Two stories (US1, US2) are co-equal P1 —
both must ship for the feature to function; US3 is also P1 but is a
test-only assertion of FR-005 behavior built in US1.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies on unfinished tasks)
- **[Story]**: Maps to spec.md user story for traceability
- Each task lists: target file(s), FR(s)/SC(s) covered, and the validation gate

## Path Conventions

Paths are repo-root relative. Source under `services/webhooks/`, tests under
`services/webhooks/tests/`, migrations under `db/migrations/`, scripts under
`scripts/`. Constitution §III boundary: every new tenant-scoped row needs
`tenant_id` FK + RLS + tenant-prefixed index.

---

## Phase 1: Setup

No setup tasks. The repo, venv, Postgres container, and Ollama are already
provisioned per `MEMORY.md`. `services/webhooks/` already exists from IN-06.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the migration and the type-only skeleton that every later
task imports from. **No story work can begin until Phase 2 is complete.**

- [ ] T001 Write migration `db/migrations/0039_provider_installations.sql` with: `CREATE TABLE IF NOT EXISTS provider_installations(...)` per plan §"Data Model" (8 columns, app-allocated `uuid7` PK, `tenant_id` FK DEFERRABLE INITIALLY IMMEDIATE, `UNIQUE (provider, installation_id)`, `secret_ref` NULLable), `CREATE INDEX IF NOT EXISTS idx_provider_installations_tenant_provider`, `ALTER TABLE ... ENABLE/FORCE ROW LEVEL SECURITY`, and the `tenant_isolation` policy guarded by `DO $$ ... pg_policies ... $$`. Covers: FR-001, FR-002, FR-010 (RLS), FR-012, FR-013 (id allocation app-side). Gate: re-run the file against a fresh DB and a DB-with-table; both must succeed silently (idempotency check). The conftest `fresh_db` fixture will apply it.

- [ ] T002 Run `python scripts/check_schema_drift.py` and confirm zero exit. Gate: non-zero exit blocks Phase 2. Covers: Constitution §II.4 drift check obligation.

- [ ] T003 [P] Add two error classes to `lib/shared/errors.py`: `class InstallationConflictError(CompanyOSError)` with code `"installation_conflict"` and `class InstallationNotFoundError(CompanyOSError)` with code `"installation_not_found"`. Both must follow the existing `to_dict()` shape. Covers: FR-014, Constitution §VIII. Gate: `ruff lib/shared/errors.py`; `python -c "from lib.shared.errors import InstallationConflictError, InstallationNotFoundError"`.

- [ ] T004 [P] Create `services/webhooks/tenant_resolver.py` with the type-only skeleton from plan §"Python entity types": `ResolverProvider` Literal, `Installation` Pydantic model (extra="forbid"), `Resolved`/`UnknownInstallation`/`PayloadMissing` discriminated-union members, the `ResolverOutcome` Annotated union, the `RegisterInstallationRequest` Pydantic model, and the `TenantResolverDeps`/`ResolverMetrics` NamedTuple stubs. NO methods, NO module-level globals beyond Pydantic models. `from __future__ import annotations` at the top. Covers: FR-014 (outcome shape), FR-016 (no module-level globals), Constitution stack constraints. Gate: `ruff services/webhooks/tenant_resolver.py`; `python -c "from services.webhooks.tenant_resolver import ResolverOutcome, Resolved, UnknownInstallation, PayloadMissing"`.

**Checkpoint**: Migration applied, drift clean, error classes importable, skeleton compiles.

---

## Phase 3: User Story 1 — Slack webhook routes to Acme tenant (Priority: P1) 🎯 MVP

**Goal**: Given an `enabled=true` installation row mapping `(slack, T_ACME_123) → acme_tenant_id`, `resolve()` returns `Resolved(tenant_id=acme_uuid, ...)` for a Slack payload carrying that team_id. Disabled/missing rows return `UnknownInstallation`.

**Independent Test** (post-Phase 9 integration run): `pytest services/webhooks/tests/test_tenant_resolver_lookup.py::test_slack_resolved_path -m integration` — inserts a row, calls `resolve()`, asserts `outcome == "resolved"` and `tenant_id` matches.

### Implementation for User Story 1

- [ ] T005 [P] [US1] Drop a recorded Slack `event_callback` JSON fixture into `services/webhooks/tests/samples/slack_event_callback.json` containing a representative `team_id` (e.g. `"team_id": "T_ACME_FIXTURE"`). Pure fixture file, no Python. Covers: testability of FR-003 Slack rule.

- [ ] T006 [P] [US1] Add `PROVIDER_EXTRACTORS` static table and `_str_or_none(value)` helper to `services/webhooks/tenant_resolver.py` exactly per plan §"Resolver Module API" (Slack rule wired; the other four providers' extractors land in T015). Slack rule extracts `payload["team_id"]`. Covers: FR-003 (Slack rule), FR-006 (PayloadMissing on absent/empty). Gate: `ruff services/webhooks/tenant_resolver.py`.

- [ ] T007 [P] [US1] Create `services/webhooks/tests/test_tenant_resolver_extract.py` with `test_slack_extracts_team_id` (uses fixture from T005) and `test_slack_missing_team_id_returns_none` (asserts `_str_or_none` behavior for missing/empty/non-string `team_id`). Pure unit, no `@integration`. Covers: FR-003 (Slack), FR-006. Gate: `pytest services/webhooks/tests/test_tenant_resolver_extract.py -k slack`.

- [ ] T008 [US1] Add `InstallationCache` class to `services/webhooks/tenant_resolver.py` per plan §"Cache Shape": `__init__(*, max_entries=4096, ttl_seconds=300.0)`, `get(key, now)`, `put(key, value, now)`, `invalidate(key)`, `stats()`. Use `collections.OrderedDict`; LRU eviction; TTL via stored timestamp + `now: float` argument (from `time.monotonic` injected via deps). Include `CacheHit` and `CacheNegative` sentinel value types. NO module-level state. Covers: FR-009, FR-010, FR-011 (cache structure). Gate: `ruff services/webhooks/tenant_resolver.py`. (Depends on T004.)

- [ ] T009 [P] [US1] Create `services/webhooks/tests/test_tenant_resolver_cache.py` with `test_cache_get_returns_put_value`, `test_cache_returns_none_on_missing_key` — happy-path only, comprehensive eviction tests land in T017. Pure unit. Covers: FR-009. Gate: `pytest services/webhooks/tests/test_tenant_resolver_cache.py -k "get_returns_put_value or returns_none"`.

- [ ] T010 [US1] Implement `class TenantResolver` in `services/webhooks/tenant_resolver.py` with `async def resolve(provider, payload, headers) -> ResolverOutcome` and the `build_tenant_resolver(deps) -> TenantResolver` factory. Flow: (1) call `PROVIDER_EXTRACTORS[provider](payload, headers)`; if `None`, return `PayloadMissing`. (2) Attempt `cache.get(...)` **inside a try/except**: on `CacheHit` return `Resolved`; on `CacheNegative` return `UnknownInstallation`; on **any exception** raised by the cache backend, log via `structlog` (warning level, no installation_id), record that this resolution is on the `bypass` path (T020 will increment `cache_total{result='bypass'}` here), and continue to step 3 — never abort the request. (3) On cache miss (or cache bypass), run a single asyncpg query `SELECT id, tenant_id, secret_ref FROM provider_installations WHERE provider = $1 AND installation_id = $2 AND enabled = true LIMIT 1` (per plan: no `WHERE tenant_id` because the lookup IS the tenant identification; uniqueness on `(provider, installation_id)` makes this safe). (4) On row found, attempt `cache.put(..., CacheHit)` **inside a try/except** (same swallow-and-log policy as step 2 — a cache write failure must not corrupt the resolution path) and return `Resolved`. (5) On no row (or disabled row excluded by `enabled = true`), attempt `cache.put(..., CacheNegative)` under the same try/except (negative cache, FR-009 rationale) and return `UnknownInstallation` — same outcome for never-registered and disabled rows (FR-005, SC-003). NO metric calls yet; T019 wires them. Use `structlog` only; no `print()`. Covers: FR-001, FR-003 (Slack), FR-004, FR-005, FR-009, FR-011 (explicit cache-backend-failure fallback), FR-015, FR-016, US-1 acceptance scenarios 1/2/3, US-5 acceptance scenario 3. Gate: `ruff services/webhooks/tenant_resolver.py`; `python -c "from services.webhooks.tenant_resolver import build_tenant_resolver, TenantResolver"`. (Depends on T006, T008.)

**Checkpoint**: `resolve()` works end-to-end for Slack against a real (or asyncpg-mocked-at-DSN) Postgres. Integration test in Phase 9 validates it on live DB.

---

## Phase 4: User Story 2 — Administrator registers an installation (Priority: P1)

**Goal**: An operator can register a new `(provider, installation_id, tenant_id, secret_ref?)` tuple via CLI; subsequent `resolve()` calls reflect it within 5 s (SC-006). Disable, re-enable, and update-secret-ref all work; duplicate registration is refused with `InstallationConflictError`.

**Independent Test** (post-Phase 9): `pytest services/webhooks/tests/test_tenant_resolver_admin.py -m integration`.

### Implementation for User Story 2

- [ ] T011 [US2] Add `async def register_installation(self, req: RegisterInstallationRequest) -> Installation` to `TenantResolver` in `services/webhooks/tenant_resolver.py`. Allocate `id = uuid7()` (NEVER `uuid4`, Constitution §VII). Insert into `provider_installations`. Catch `asyncpg.UniqueViolationError` and re-raise as `InstallationConflictError(code="installation_conflict", context={"provider": req.provider, "installation_id": req.installation_id})`. On success, `cache.invalidate((req.provider, req.installation_id))` to clear any prior `CacheNegative`. Covers: FR-001, FR-002, FR-007, FR-010 (cache invalidation), FR-013 (uuid7), FR-014, US-2 acceptance 1/2, SC-006. Gate: `ruff`. (Depends on T010.)

- [ ] T012 [US2] Add `async def disable_installation(self, installation_row_id: UUID) -> None` and `async def enable_installation(self, installation_row_id: UUID) -> None` to `TenantResolver` in `services/webhooks/tenant_resolver.py`. SQL: `UPDATE provider_installations SET enabled = $2 WHERE id = $1 RETURNING provider, installation_id`. If no row returned, raise `InstallationNotFoundError`. Use the returned `(provider, installation_id)` to call `cache.invalidate(...)`. Covers: FR-008 (disable/re-enable), FR-010 (cache invalidation), FR-014, US-2 acceptance 3, SC-006. Gate: `ruff`. (Depends on T010.)

- [ ] T013 [US2] Add `async def update_secret_ref(self, installation_row_id: UUID, new_secret_ref: str | None) -> None` to `TenantResolver`. SQL: `UPDATE provider_installations SET secret_ref = $2 WHERE id = $1 RETURNING provider, installation_id`. Raise `InstallationNotFoundError` on no row. Invalidate cache for `(provider, installation_id)` so the next `resolve()` re-reads the new pointer (per Clarification Q1). Covers: FR-008 (update-secret-ref), FR-010, FR-014, Clarifications session 2026-05-13 Q1. Gate: `ruff`. (Depends on T010.)

- [ ] T014 [P] [US2] Create `scripts/webhook_install.py` — a `python scripts/webhook_install.py {register|disable|enable|update-secret-ref}` CLI using `argparse`. Each subcommand: parse args → instantiate a pool via `lib.shared.db` → call the corresponding `TenantResolver` method → print JSON result to stdout. On `InstallationConflictError` or `InstallationNotFoundError`, print the error's `to_dict()` to stderr and `sys.exit(1)`. NO `print()` in production code paths — this is a CLI entry point under `scripts/`, where structlog is overkill and `print` is the script's stdout/stderr contract. Covers: FR-007, FR-017 (operator-shell auth). Gate: `ruff scripts/webhook_install.py`; `python scripts/webhook_install.py --help` exits zero. (Depends on T011, T012, T013.)

**Checkpoint**: Admin actions work in-process and via CLI. Integration test in Phase 9 validates against live DB.

---

## Phase 5: User Story 4 — Resolver supports all five launch providers (Priority: P2)

**Goal**: GitHub, Linear, Stripe, Discord payloads each resolve via the same `resolve()` function, picking up their provider-native id from the right place.

**Independent Test**: Run extractor unit tests; resolver lookup paths for each provider are exercised by Phase 9 integration tests.

### Implementation for User Story 4

- [ ] T015 [P] [US4] Drop vendor-sample JSON fixtures into `services/webhooks/tests/samples/` — `github_webhook.json` (with `installation.id`), `linear_webhook.json` (with `organizationId`), `discord_interaction.json` (with `guild_id` and a separate `discord_global_command.json` with only `application_id`). For Stripe, no body fixture is needed — the header is the lookup key; document this in a `services/webhooks/tests/samples/README.md`. Covers: FR-003 testability.

- [ ] T016 [P] [US4] Extend `services/webhooks/tenant_resolver.py` `PROVIDER_EXTRACTORS` with the four remaining entries per plan §"Resolver Module API" — `github` reads `payload["installation"]["id"]` (stringified, handles missing `installation` key), `linear` reads `payload["organizationId"]`, `stripe` reads `headers.get("Stripe-Account")` (case-insensitive via lower-case fallback), `discord` reads `payload.get("guild_id") or payload.get("application_id")`. Covers: FR-003 (all five rules), Spec edge cases (Stripe header, Discord fallback). Gate: `ruff`.

- [ ] T017 [P] [US4] Extend `services/webhooks/tests/test_tenant_resolver_extract.py` with `test_github_extracts_installation_id`, `test_github_missing_installation_block_returns_none`, `test_linear_extracts_organization_id`, `test_stripe_extracts_account_header_case_insensitive`, `test_discord_prefers_guild_id_falls_back_to_application_id`, `test_discord_missing_both_returns_none`. Unit only. Covers: FR-003 (all rules), FR-006, US-4 acceptance scenarios 2/3/4/5/6, SC-005. Gate: `pytest services/webhooks/tests/test_tenant_resolver_extract.py`.

**Checkpoint**: All five providers' extractors are tested in isolation; the full resolver lookup pipeline for non-Slack providers is exercised in Phase 9.

---

## Phase 6: User Story 5 — Hot-path resolution does not hit the database every time (Priority: P2)

**Goal**: The TTL LRU cache handles TTL expiry, LRU eviction, invalidation, and negative caching correctly.

**Independent Test**: `pytest services/webhooks/tests/test_tenant_resolver_cache.py` (unit). Latency and cache-backend-unavailable behavior are integration concerns deferred to Phase 9.

### Implementation for User Story 5

- [ ] T018 [P] [US5] Extend `services/webhooks/tests/test_tenant_resolver_cache.py` with: `test_cache_expires_after_ttl` (advance injected clock past TTL, assert `get` returns `None`); `test_cache_evicts_lru_when_full` (max_entries=4, insert 5 keys, assert first key evicted, fifth retained); `test_cache_invalidate_removes_entry`; `test_cache_negative_entry_returns_cache_negative_sentinel`; `test_cache_invalidate_clears_negative_entry`. Inject `time.monotonic`-style clock via a small fake. Pure unit, no DB. Covers: FR-009, FR-010, FR-011 (cache structure for fallback). Gate: `pytest services/webhooks/tests/test_tenant_resolver_cache.py`.

**Checkpoint**: Cache passes all eviction/TTL/invalidation/negative-cache scenarios.

---

## Phase 7: Metrics wiring (orchestrator step 4)

**Purpose**: Define the three FR-018 metric instances and wire `.inc()` / `.observe()` calls into `resolve()` and admin actions. Per orchestrator's strict ordering, metrics land after admin and before rename.

- [ ] T019 [P] Append three metric instances to `services/webhooks/metrics.py` exactly per plan §"Metric Registration" — `webhook_resolver_outcomes_total` (Counter, labels `provider, outcome`), `webhook_resolver_cache_total` (Counter, labels `provider, result`), `webhook_resolver_duration_seconds` (Histogram, label `provider`, buckets `(0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250)`). Covers: FR-018, SC-009 bucket placement. Gate: `ruff services/webhooks/metrics.py`; `python -c "from services.webhooks.metrics import webhook_resolver_outcomes_total, webhook_resolver_cache_total, webhook_resolver_duration_seconds"`.

- [ ] T020 Wire metric increments into `services/webhooks/tenant_resolver.py`: (a) in `resolve()`, wrap the body in a `with self._deps.metrics.duration.labels(provider).time():` block; increment `cache.labels(provider, result).inc()` for `result ∈ {hit, miss, bypass}` (the `bypass` value is emitted on the FR-011 cache-unavailable fallback path); increment `outcomes.labels(provider, outcome).inc()` for the three outcome literals. (b) Update `build_tenant_resolver` to accept and store the `ResolverMetrics` NamedTuple from deps. Covers: FR-018, SC-002 (counter assertion), SC-004 (hit-rate assertion), SC-005, SC-009 (histogram). Gate: `ruff`; existing unit tests still pass with a throwaway-counters dep. (Depends on T010, T019.)

**Checkpoint**: Every `resolve()` and every admin mutation emits the right metric series.

---

## Phase 8: Rename (orchestrator step 5)

**Purpose**: Retire the IN-06 stub `tenant_resolution.py` and point the IN-06 router at the new module name.

- [x] T021 ~~Delete `services/webhooks/tenant_resolution.py`. Update the import line in `services/webhooks/router.py` ...~~ **DEFERRED to IN-06 by user decision during Phase 6 implementation.** The new resolver's signature change (sync env-var lookup → async DB-backed lookup with injected deps + discriminated-union outcome) is much larger than the plan anticipated — fully honoring it requires touching `router.py` call sites + `gateway/main.py` lifespan wiring, which is outside `source.md`'s "Files relevant" boundary. IN-07 ships `tenant_resolver.py` as a standalone, fully-tested module; the IN-06 webhook router cutover becomes IN-06's responsibility. Recorded in spec Assumption A2 (which already noted this divergence as plan-time-deferred).

**Checkpoint**: The old file is gone; the IN-06 router imports the new module cleanly.

---

## Phase 9: Integration tests (orchestrator step 6 — LAST)

**Purpose**: All `@integration`-marked tests, run against real Postgres via the `db_pool` / `fresh_db` fixtures. Constitution §IV: no mocked Postgres.

- [ ] T022 [P] [US1] [US4] Create `services/webhooks/tests/test_tenant_resolver_lookup.py` marked `@pytest.mark.integration`, with: `test_resolved_path` **parametrized over all 5 providers** via `pytest.mark.parametrize("provider, fixture_path, header_overrides", [...])` driving each of `slack`, `github`, `linear`, `stripe`, `discord` against its recorded vendor-sample (`samples/slack_event_callback.json`, `samples/github_webhook.json`, `samples/linear_webhook.json`, `samples/discord_interaction.json`, and a synthetic Stripe header bag) — each parametrization inserts a matching `provider_installations` row, calls `resolve`, asserts `outcome == "resolved"` and `tenant_id` matches; `test_unknown_installation_path` (also parametrized over the 5 providers, no row inserted → assert `UnknownInstallation`); `test_disabled_row_returns_unknown_installation_indistinguishable` (insert `enabled=false` row → assert `UnknownInstallation` with byte-for-byte equal serialization to the never-registered case — feeds SC-003); `test_resolve_emits_outcome_counter_per_branch` (assert `webhook_resolver_outcomes_total` increments with the right `outcome` label per case — feeds SC-002, SC-005). Uses `db_pool` + `fresh_db`. Covers: US-1, US-3 partial, US-4 (end-to-end per-provider resolve), FR-001/FR-003/FR-004/FR-005, FR-018, SC-002/SC-003/SC-005. Gate: `pytest services/webhooks/tests/test_tenant_resolver_lookup.py -m integration`.

- [ ] T023 [P] [US2] Create `services/webhooks/tests/test_tenant_resolver_admin.py` marked `@pytest.mark.integration`, with: `test_register_then_resolve_returns_resolved`, `test_register_duplicate_raises_conflict`, `test_disable_then_resolve_returns_unknown`, `test_enable_after_disable_restores_resolve`, `test_update_secret_ref_changes_resolved_value`, `test_consistency_within_5_seconds` (assert `resolve` reflects admin action within 5 s — feeds SC-006). Uses `db_pool` + `fresh_db`. Covers: US-2, FR-002, FR-007, FR-008, FR-010, FR-014, SC-001, SC-006, Clarifications Q1. Gate: `pytest services/webhooks/tests/test_tenant_resolver_admin.py -m integration`.

- [ ] T024 [P] [US3] [US6] Create `services/webhooks/tests/test_tenant_resolver_security.py` marked `@pytest.mark.integration`, with: `test_disabled_and_never_registered_responses_are_byte_equal` (canonicalize outcome JSON for both cases, assert equal hash — feeds SC-003); `test_log_records_never_contain_installation_id` (capture structlog output via `structlog.testing.capture_logs`, assert no log entry contains the test installation_id string — feeds SC-008); `test_rls_blocks_cross_tenant_reads` (insert rows for two tenants; under `tenant_transaction(tenant_a)`, assert tenant_b's rows are invisible to `SELECT *` from `provider_installations` — feeds US-6, SC-006-RLS variant). Uses `db_pool` + `fresh_db` + `lib.shared.tenant_context.tenant_transaction`. Covers: US-3, US-6, FR-005, FR-012, FR-015, SC-003, SC-006, SC-008. Gate: `pytest services/webhooks/tests/test_tenant_resolver_security.py -m integration`.

- [ ] T025 [P] [US5] Add `test_resolve_falls_back_to_db_when_cache_raises` to `services/webhooks/tests/test_tenant_resolver_cache.py` (marked `@pytest.mark.integration`): inject a cache that raises on `.get()`; assert `resolve` still returns `Resolved` via direct DB lookup and that `webhook_resolver_cache_total{result="bypass"}` increments. Covers: FR-011, SC-007. Gate: `pytest services/webhooks/tests/test_tenant_resolver_cache.py -m integration`.

- [ ] T026 [US5] Add `test_resolve_latency_within_slo` to `services/webhooks/tests/test_tenant_resolver_lookup.py`, marked `@pytest.mark.slow` AND `@pytest.mark.integration`: warm the cache with one resolve, then 200 hot-path resolves; read p95 from `webhook_resolver_duration_seconds` histogram; assert p95 ≤ 0.002 s (hit) and ≤ 0.025 s (after invalidation, miss path). Per plan, one-retry budget on flakiness. Covers: SC-009. Gate: `pytest services/webhooks/tests/test_tenant_resolver_lookup.py::test_resolve_latency_within_slo -m "integration and slow"`. (Depends on T022.)

- [ ] T026b [US5] Add `test_cache_hit_rate_above_threshold_after_warmup` to `services/webhooks/tests/test_tenant_resolver_lookup.py`, marked `@pytest.mark.slow` AND `@pytest.mark.integration`: insert 10 enabled installation rows; for each, run one warmup resolve (cache cold → DB hit, populates cache); then run 200 resolves over the same 10 keys; read `webhook_resolver_cache_total{result='hit'}` and `{result='miss'}` and `{result='bypass'}` counters; assert `hit / (hit + miss + bypass) ≥ 0.95`. Pinned to SC-004's "representative 10-minute load window" target re-cast as a finite-N CI assertion. One-retry budget on flakiness (same as T026). Covers: SC-004. Gate: `pytest services/webhooks/tests/test_tenant_resolver_lookup.py::test_cache_hit_rate_above_threshold_after_warmup -m "integration and slow"`. (Depends on T022 + T020.)

**Checkpoint**: Every SC has at least one assertion in an integration test.

---

## Phase 10: Polish & Validation

**Purpose**: Final hygiene + the gate suite the SDD orchestrator wants to see green before declaring done.

- [ ] T027 [P] Run `ruff` on every file this branch touches: `ruff services/webhooks/tenant_resolver.py services/webhooks/metrics.py services/webhooks/router.py services/webhooks/tests/ scripts/webhook_install.py lib/shared/errors.py db/migrations/0039_provider_installations.sql`. Gate: zero violations.

- [ ] T028 [P] Run `python scripts/check_schema_drift.py`. Gate: zero exit.

- [ ] T029 Run `pytest services/webhooks/ -m integration`. Gate: all tests pass against the live Docker Postgres on `localhost:5433`.

- [ ] T030 Run `pytest services/webhooks/` (full suite — unit + integration). Gate: all tests pass.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 2 (Foundational)**: T001 blocks T002; T001 blocks all DB-touching tasks downstream. T003 and T004 are independent of T001 and run in parallel.
- **Phase 3 (US1)**: Requires T004 (skeleton) and T001 (table). T005/T006/T007 parallelizable; T008 depends on T004; T009 parallelizable with T008; T010 depends on T006 and T008.
- **Phase 4 (US2)**: T011/T012/T013 sequential within the same file. T014 (CLI) depends on T011/T012/T013.
- **Phase 5 (US4)**: T015/T016/T017 parallelizable.
- **Phase 6 (US5)**: T018 standalone.
- **Phase 7 (Metrics)**: T019 parallelizable; T020 depends on T010 + T019.
- **Phase 8 (Rename)**: T021 depends on Phase 7 complete (so the new file is fully functional before the old one is removed).
- **Phase 9 (Integration tests)**: All run only after Phase 8. T022/T023/T024/T025 parallel; T026 depends on T022.
- **Phase 10 (Polish)**: T027/T028 parallel; T029 then T030.

### Per-Story Dependencies

- **US1 (Slack routing)**: Phase 2 → Phase 3 → metric wiring (T020) → integration test (T022). MVP slice.
- **US2 (Admin)**: Phase 2 + US1 baseline (T010) → Phase 4 → metric wiring (T020) → integration test (T023).
- **US3 (Indistinguishability)**: Behavior built in T010 (US1); validated by T024 only.
- **US4 (Five providers)**: Phase 2 + T006 (US1) → Phase 5.
- **US5 (Caching)**: Cache class in T008 (US1) → Phase 6 unit tests; T025 + T026 integration tests in Phase 9.
- **US6 (Cross-tenant RLS)**: Table-level RLS in T001 (Phase 2) → T024 integration test.

### Parallel Opportunities

```bash
# Phase 2 — three parallel tracks:
Task: "T003 — add error classes to lib/shared/errors.py"
Task: "T004 — create tenant_resolver.py skeleton"
# (T001 / T002 sequential)

# Phase 3 — within US1, the extractor / cache / fixture work parallelizes:
Task: "T005 — Slack vendor sample fixture"
Task: "T006 — PROVIDER_EXTRACTORS + helpers"
Task: "T007 — Slack extractor unit test"
Task: "T009 — Cache happy-path unit test (after T008)"

# Phase 5 — all three US4 tasks parallel:
Task: "T015 — vendor sample fixtures"
Task: "T016 — extend PROVIDER_EXTRACTORS"
Task: "T017 — extractor unit tests for 4 providers"

# Phase 9 — integration tests parallel except T026 (depends on T022):
Task: "T022 — test_tenant_resolver_lookup.py"
Task: "T023 — test_tenant_resolver_admin.py"
Task: "T024 — test_tenant_resolver_security.py"
Task: "T025 — cache fallback integration test"
```

---

## Implementation Strategy

### MVP first (US1 alone, shippable feature)

Phases 1 → 2 → 3 → Phase 7 (metrics wiring) → Phase 8 (rename) → Phase 9 T022 only → Phase 10. Slack workspaces route correctly; other four providers are 401 (UnknownInstallation) until US4 lands. This is a viable demo target.

### Full P1 (US1 + US2 + US3)

Add Phase 4 between Phase 3 and Phase 7; add T023 + T024 to the Phase 9 slice. Now admin path works end-to-end, indistinguishability is asserted, security gates are green.

### Full launch (P1 + P2 = all six stories)

All phases. Five providers covered, latency SLO asserted, cross-tenant RLS verified, cache-backend-unavailable fallback proven.

---

## Constitution Check (post-task-decomposition)

Per Constitution §IX migration-ordering discipline:

- **Migration first**: T001 lands before all other DB-touching tasks. ✓
- **Dual-write / sidecar writers**: N/A — this is not a dual-write feature.
- **Reader cutover and tests last**: T021 (rename = reader cutover for IN-06's router) precedes Phase 9 (integration tests). ✓

Per Constitution §III tenant-isolation triad on every new tenant-scoped table:

- **FK + DEFERRABLE INITIALLY IMMEDIATE**: T001. ✓
- **RLS ENABLE + FORCE + permissive policy**: T001. ✓
- **Tenant-prefixed index**: T001 (`idx_provider_installations_tenant_provider`). ✓

Per Constitution §VII uuid7 obligation: T011 explicit on `uuid7()`. ✓

Per Constitution §IV no-mocked-Postgres-in-integration: every `@integration` task uses `db_pool` / `fresh_db`. ✓

Per Constitution §VIII structured-error obligation: T003 (error classes), T011/T012/T013 (raise paths). ✓

No constitution exceptions are required by this task plan.

---

## Notes

- Total tasks: **31** (T001 through T030, with T026b inserted between T026 and T027 after the speckit-analyze recommendations).
- Tests are not optional here — the plan mandates 5 test modules. Unit tests
  for extractor (T007, T017) and cache (T009, T018) are co-located with
  their implementing US per the orchestrator's exception clause. Integration
  tests (T022–T026) all land in Phase 9 per the orchestrator's strict
  ordering.
- Story labels (`[US1]`..`[US6]`) trace back to spec.md user stories.
- Every task names: target file(s), FR/SC coverage, and validation gate.
- Commit after each task or at logical Phase boundaries.
- Stop at the MVP checkpoint (end of Phase 3 + T020 + T021 + T022 + Phase 10)
  to validate the Slack-only happy path before pressing on.
