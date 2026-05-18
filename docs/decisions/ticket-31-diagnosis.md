# Ticket #31 — Diagnosis (Phase 1)

**Branch:** `fix/test-ingest-core-ci` (off `376cdda`, A6 merge HEAD).
**Status:** Phase 1 read-only. No code changes yet.
**Date:** 2026-05-18.

---

## TL;DR

The M1 closeout diagnosis is **accurate** — line numbers shifted by 1, root cause unchanged. Fix shape is one fixture change + one new CI workflow. No schema changes, no test-code rewrites, no architectural surprises.

---

## 1. Test inventory in `test_ingest_core.py`

**Total: 34 test functions** at [services/ingestion/tests/test_ingest_core.py](../../services/ingestion/tests/test_ingest_core.py).

| Category | Count | Status | Why |
|---|---|---|---|
| Pure unit (no DB) | 14 | All pass | Slack signature, phrase extraction, handler registry, channel trust map — no tenant FK |
| DB-touching, fail-fast | 3 | All pass | `test_unknown_channel_raises_handler_not_found`, `test_malformed_slack_payload_raises_validation`, `test_oversized_payload_rejected` — they raise BEFORE the INSERT that would touch the FK |
| DB-touching, hits FK | **17** | **15 FAIL + 1 ERROR** = 16 not-passing | The 15 failures + 1 error are the #31 backlog |

Total accounted for: 14 + 3 + 17 = 34. ✓

The 1 ERROR vs. 15 FAIL distinction matters: `test_slack_happy_path_creates_observation` errors in fixture setup (`seeded_actor` inserts into `actors` which has FK to `tenants` — fails before the test body runs), so pytest categorizes it ERROR not FAIL. The remaining 15 fail in test body when `ingest()` tries to write to `observations_<partition>` (FK `observations_tenant_fk`).

### The 16 not-passing tests (all same root cause)

```
ERROR test_slack_happy_path_creates_observation       (actors_tenant_fk in seeded_actor fixture)
FAIL  test_dedup_same_slack_message_twice             (observations_tenant_fk)
FAIL  test_unknown_actor_ref_records_unresolved_marker
FAIL  test_entity_alias_fast_path_resolves
FAIL  test_unresolved_entity_phrase_queued_in_content
FAIL  test_embedding_fallback_on_ollama_error
FAIL  test_trust_tier_slack_is_attested_agent
FAIL  test_notify_fires_post_commit
FAIL  test_think_trigger_enqueued_on_new_observation
FAIL  test_system_state_change_ingest
FAIL  test_internal_channel_accepts_null_external_id
FAIL  test_successive_ingests_have_monotonic_ids
FAIL  test_50_concurrent_ingests_same_external_id_dedup_to_one
FAIL  test_replay_events_dedups_to_zero_new_rows
FAIL  test_fuzz_slack_payload_never_500s
FAIL  test_real_ollama_embedding_stored
```

Verbatim representative failure:

```
asyncpg.exceptions.ForeignKeyViolationError: insert or update on table
"observations_2026_05" violates foreign key constraint
"observations_tenant_fk"
DETAIL: Key (tenant_id)=(019e3a15-5818-7000-93d7-6710b77a5f06) is
not present in table "tenants".
```

And for the ERROR-category test (fixture setup):

```
asyncpg.exceptions.ForeignKeyViolationError: insert or update on table
"actors" violates foreign key constraint "actors_tenant_fk"
DETAIL: Key (tenant_id)=(019e3a15-541a-7000-967d-d26b6eaacc17) is
not present in table "tenants".
```

**All 16 are the same root cause.** No tests fail for a different reason; no hidden bug in `ingest()` is being masked.

---

## 2. Current fixture shape in `conftest.py`

[services/ingestion/tests/conftest.py](../../services/ingestion/tests/conftest.py):

| Fixture | Line | Behavior |
|---|---|---|
| `gateway_pool` | [159-182](../../services/ingestion/tests/conftest.py#L159-L182) | Function-scoped asyncpg pool against `DATABASE_URL`. Runs migrations + TRUNCATE on entry. Terminate (not graceful close) on teardown. |
| `tenant_id` | [185-187](../../services/ingestion/tests/conftest.py#L185-L187) | Returns a fresh `uuid7()`. **Does NOT insert into `tenants`.** This is the bug. |
| `seeded_actor` | [190-201](../../services/ingestion/tests/conftest.py#L190-L201) | Inserts into `actors` with `tenant_id` from the fixture above. Because `tenant_id` was never inserted into `tenants`, the FK `actors_tenant_fk` fires immediately. |

The M1 closeout said line 186; current line is 185-187 (3-line def, the M1 reference was the `def` line which is now 186). **Accurate.**

**No `seed_tenant` or equivalent fixture exists.** The M1 diagnosis's prescription ("add `seed_tenant` fixture") is still applicable as-is — there is nothing to dust off.

---

## 3. FK constraint mode

Migration [db/migrations/0037_tenant_fks.sql](../../db/migrations/0037_tenant_fks.sql) is the master file. Every tenant-scoped table's FK to `tenants(id)` is:

```sql
FOREIGN KEY (tenant_id) REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE
```

Verbatim from the migration's design comment (lines 8-16):

> Why DEFERRABLE INITIALLY IMMEDIATE
> IMMEDIATE = production code that forgets to register a tenant fails loudly on the first INSERT, not silently with orphaned rows.
> DEFERRABLE = tests that wrap the body in a transaction and ROLLBACK can `SET CONSTRAINTS ALL DEFERRED` so the FK is checked only at COMMIT (which never fires for tests). This means existing tests that generate tenant_id via uuid7() without inserting a tenants row keep working unchanged, as long as the test transaction is rolled back.

**The current tests do NOT wrap in a transaction.** They use `gateway_pool.execute(...)` and `pool.fetchval(...)` which auto-commit each statement. So the `IMMEDIATE` mode fires before any opportunity to defer.

**Two possible fixes:**

| Approach | Pros | Cons |
|---|---|---|
| **A. Seed the tenant.** Insert a `tenants` row in the `tenant_id` fixture before returning the UUID. | One-line change. Tests stay unchanged. Matches production code's "register a tenant first" pattern. | Tests now leave one extra row per test (`gateway_pool` truncates on entry so cleanup is automatic). |
| **B. Wrap each test in a transaction + `SET CONSTRAINTS ALL DEFERRED` + rollback.** | The legacy convention the migration was designed for. | Requires significant test code changes — every test would need a transaction wrapper. The `ingest()` function manages its own transactions, so wrapping the test in an outer transaction may conflict with `ingest()`'s connection management. |

**Recommendation: Approach A.** Smaller change, matches the migration's IMMEDIATE-fail-loud design intent for production. The migration's "tests with rollback" note describes a path tests *could* take, not one they *must* take.

---

## 4. CI workflow inventory

Current contents of `.github/workflows/`:

| File | What it does | Runs ingestion tests? |
|---|---|---|
| [deploy-production.yml](../../.github/workflows/deploy-production.yml) | SSH-deploys to AWS Lightsail on push to `production`. No tests. | **No.** |
| [real-llm-nightly.yml](../../.github/workflows/real-llm-nightly.yml) | Cron-triggered (07:00 UTC) DeepSeek smoke suite at `tests/real_llm/`. | **No.** |

**No CI workflow runs `services/ingestion/tests/test_ingest_core.py`, or `services/ingestion/tests/` at all, or any test under `services/`.** The M1 closeout finding is still accurate.

The `real-llm-nightly.yml` workflow is a usable template for the new ingestion CI:
- Python 3.14 + `pip install -e ".[dev]"` venv pattern.
- `docker compose up -d postgres ollama` for service spin-up (project-standard).
- `for i in $(seq 1 60); do pg_isready ...; done` readiness pattern.
- `set -o pipefail; pytest ... | tee log` pattern.

The new workflow should not import `real-llm-nightly.yml` verbatim — that runs `tests/real_llm/` and pulls embedding models. The new workflow's scope is `services/ingestion/tests/` (the 34 tests of `test_ingest_core.py` plus any other files in that directory).

---

## 5. Confirmation/correction of the M1 closeout diagnosis

| M1 closeout claim | Current state |
|---|---|
| `tenant_id` fixture at `conftest.py:186` generates `uuid7()` without seeding `tenants` | ✓ Accurate (line 185-187 in current code; the `def tenant_id()` line is 186). |
| FK constraints are `DEFERRABLE INITIALLY IMMEDIATE` | ✓ Confirmed in [0037_tenant_fks.sql:5-6](../../db/migrations/0037_tenant_fks.sql#L5-L6). |
| Tests use `gateway_pool.execute(...)` which auto-commits, so FK fires immediately | ✓ Confirmed — `seeded_actor` calls `pool.execute(...)` directly, no transaction; `ingest()` calls also auto-commit per row at the connection level. |
| Fix: add `seed_tenant` fixture in `conftest.py:186`; convert `gateway_pool.execute` calls in test setup to wrapped transactions | **Refined:** add the seed in the existing `tenant_id` fixture itself (Approach A above) — simpler than a separate `seed_tenant` + test rewrites. The transaction-wrapping suggestion is approach (B) which is the more invasive alternative. |

**Diagnosis stands.** The fix is small and the failure mode is uniform across the 16 tests.

---

## 6. Phase 2 plan (preview, not executed)

1. Modify `tenant_id` fixture in `conftest.py` to insert into `tenants` before returning the UUID. Use the fresh `uuid7()` as the inserted row's `id`. Depend on `gateway_pool` so the insert is bound to the same pool the tests use.

2. No other code changes expected. The 16 failing tests should all pass with just this fixture fix.

3. Run after each fixture-touch:
   ```
   pytest services/ingestion/tests/test_ingest_core.py -v
   ```
   Expected: 34/34 pass (assuming Ollama is reachable at OLLAMA_URL for the one Ollama-integration test; otherwise that one skips).

4. **If any test fails for a non-FK reason** after the seed fix, STOP — that's a real bug in `ingest()` worth surfacing.

## 7. Phase 3 plan (preview, not executed)

1. Create `.github/workflows/ingestion-tests.yml`.
2. Trigger: `push` and `pull_request` to `integration/ingestion-hardening` and `main`.
3. Postgres via `docker compose up -d postgres` (project standard).
4. `pip install -e ".[dev]"` venv pattern from `real-llm-nightly.yml`.
5. `pytest services/ingestion/tests/ -v --tb=short`.
6. Non-superuser role question: confirmed in Phase 1 that the local dev role bypasses RLS, which causes [test_migrations.py:190](../../services/ingestion/tests/test_migrations.py#L190) to skip. The CI workflow can either (a) use the same superuser role (RLS test stays skipped — matches local) or (b) create a non-superuser role (RLS test might run). Decision deferred to Phase 3.
7. The RLS test is **out of scope** per the work-unit prompt (ticket #32 territory). #31 fix is the 15 FK-violation failures; RLS is a free-win-or-deferral.

---

## What this work-unit is not (per the prompt)

- **Not** a rewrite of `test_ingest_core.py`. Tests stay; setup gets fixed.
- **Not** a review of `ingest()` correctness. The tests will verify it; this work-unit makes them runnable.
- **Not** a schema migration. No `DEFERRABLE INITIALLY IMMEDIATE → DEFERRED` change. If one became necessary, that would be an architectural decision exceeding this scope.
- **Not** ticket #32 (RLS hardening).
