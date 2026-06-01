# Fyraliscore — Engineering Hardening Backlog

> Audit performed against `prajwal-dev` branch (HEAD: `da45445`).
> **599 Python files · ~137K LoC · 25 services**
> Stack: FastAPI + asyncpg + pgvector + Ollama + DeepSeek/Anthropic/OpenAI

---

## Table of Contents

- [P0 — Critical (ship-blocking)](#p0--critical-ship-blocking)
- [P1 — High](#p1--high)
- [P2 — Medium](#p2--medium)
- [P3 — Low](#p3--low)
- [Cross-Cutting Reports](#cross-cutting-reports)
- [Quick Wins](#quick-wins)
- [High-Risk Areas](#high-risk-areas)
- [Test Harness Recommendations](#test-harness-recommendations)
- [Summary & Sprint Plan](#summary--sprint-plan)

---

## P0 — Critical (ship-blocking)

> Immediate action required. These are production blockers.

---

### P0-1: Slack signature verification effectively bypassed when secret is unset

**Priority:** P0 · **Effort:** S

**Problem**

`services/gateway/main.py:598-610` gates Slack HMAC verification on `channel == "slack:message"`. When `deps.slack_signing_secret` is missing, the code passes `secret or ""` to `verify_slack_signature`. With an empty key, HMAC is computed against the empty secret and the request is silently accepted as valid — enabling unauthenticated webhook injection from anyone who can reach `/ingest`.

**Impact**

Reliability + Security: arbitrary, attacker-controlled signals injected directly into the substrate (observations/triggers). Pollutes think pipeline, models, recommendations.

**Proposed Change**

- If `COMPANY_OS_ENV != "dev"` and `slack_signing_secret` is unset → return `503 (slack_signing_secret_not_configured)`
- In dev, log a loud warning and require an explicit `INSECURE_SLACK_INGEST=1` opt-in
- Update `verify_slack_signature` to refuse empty secrets defensively

**Acceptance Criteria**

- `POST /ingest channel slack:message` in prod with no secret → `503`
- Same call in prod with valid signature → `200`
- Same call in prod with bad signature → `401`
- Same call in dev without `INSECURE_SLACK_INGEST=1` → `503`

**Test Plan**

- Unit: `test_slack_signature_required_in_prod` (3 paths above)
- Integration: send a forged Slack POST against the gateway with no secret; expect `503`

---

### P0-2: Duplicated migration prefix `0014_*` produces non-deterministic schema

**Priority:** P0 · **Effort:** S

**Problem**

`db/migrations/0014_access_control.sql` and `db/migrations/0014_customer_commitments_superset.sql` share the `0014_` prefix. `scripts/docker-migrate.sh` iterates over filenames; ordering depends on `ls` sort. Different environments may apply them in different orders. `check_schema_drift.py` is permissive enough that the divergence is invisible.

**Impact**

Correctness: cross-environment schema divergence; downstream queries depending on column order/indexes can silently miss rows. Hard to reproduce bugs in prod.

**Proposed Change**

- Rename `0014_customer_commitments_superset.sql` → `0025_customer_commitments_superset.sql`
- Update `check_schema_drift.py`'s expected order
- Add a `lib/shared/db.py` boot assertion that scans `db/migrations/*.sql` and fails on duplicate prefixes
- Run `check_schema_drift.py` in CI before deploy and fail on drift

**Acceptance Criteria**

- `ls db/migrations/` shows no duplicate numeric prefix
- All envs report the same `schema_migrations` row count
- CI fails if a future PR introduces another duplicate prefix

**Test Plan**

- Unit: `test_migration_filenames_have_unique_prefix`
- CI step: run drift check against a freshly-migrated test DB

---

### P0-3: Bearer tokens leak into structured logs via `request.headers` capture

**Priority:** P0 · **Effort:** S

**Problem**

`services/gateway/main.py:626` passes `dict(request.headers)` (including `Authorization`, `X-Bootstrap-Secret`) to `ingest()`. Any downstream `log.exception` that includes the headers leaks the bearer token verbatim. Since structlog is JSON-rendering, tokens land in centralized logging searchable to anyone with log access.

**Impact**

Security: replayable session tokens in logs; expands blast radius of any log-store compromise; breaks compliance posture.

**Proposed Change**

- Build a `safe_headers` dict that strips `authorization`, `x-bootstrap-secret`, `cookie`, `x-slack-signature` (case-insensitive) before passing or logging
- Add a structlog processor that redacts the same keys defensively for any dict it sees in event payloads

**Acceptance Criteria**

- Grep over a captured log sample shows zero `Bearer ...` tokens
- Unit test asserts `safe_headers` removes each banned header
- Processor test demonstrates redaction even if a developer accidentally logs `request.headers`

**Test Plan**

- Unit: `test_safe_headers_strips_sensitive`
- Unit: `test_log_processor_redacts_authorization_in_arbitrary_dict`

---

### P0-4: Multi-instance scheduler stampede (no leader election)

**Priority:** P0 · **Effort:** M

**Problem**

`services/greeting/scheduler.py:173-191`: each gateway replica with `GATEWAY_START_GRT_SCHEDULER=1` (the docker-compose default) spawns its own refresh / TOD-boundary / post-commit-listener loops. The `_started` flag is in-process only.

Under any horizontal scale-out (or even an accidental rolling restart with overlap), the same tenants' greetings render N× simultaneously: cache stampede, duplicate LLM cost, log noise.

**Impact**

Reliability + Cost: O(replicas) duplicated LLM/render spend; cache thrash; potential row contention on `view_ceo_cache`.

**Proposed Change**

- Add `scheduler_leader (singleton_key TEXT PRIMARY KEY, instance_id TEXT, leased_until TIMESTAMPTZ)`
- Acquire / renew lease via `INSERT ... ON CONFLICT (singleton_key) DO UPDATE SET ... WHERE leased_until < now() OR instance_id = $1` every 10s; lease expires at 30s
- Only the leaseholder runs the refresh loops

**Acceptance Criteria**

- Two gateway containers can run; only one performs refreshes at a time
- Killing the leader causes another to pick up within 30s (verified in test)

**Test Plan**

- Integration: spin two scheduler instances against the same DB; assert `view_ceo_cache` shows refresh count = 1, not 2, over a 90s window
- Integration: kill leader; assert second instance becomes leader within lease TTL

---

### P0-5: LLM and embedding network calls held inside DB transactions

**Priority:** P0 · **Effort:** L

**Problem**

`services/think/reason.py:182-195, 584`: the entire think transaction (with the region lock) is held while `llm_reason()` runs, often 5–30s.

`services/models/repo.py:444-448, 507, 635`: `ModelsRepo.insert()` opens a transaction, then calls `OllamaClient.embed()` for the natural-language form — another 100–500ms+ of network I/O inside the lock.

**Impact**

Reliability + Performance: pool exhaustion under any LLM/Ollama latency spike; region lock contention blocks all other tenants' triggers; cascading 503s. **This is the single largest stability risk in the system.**

**Proposed Change**

- Restructure `think.reason`: do retrieval inside tx → commit → run LLM outside tx → second tx for apply + post-commit enqueue (the apply already has its own idempotency key, so the split is safe)
- Restructure `ModelsRepo.insert`: compute the embedding before opening the transaction; pass the precomputed vector into `_insert_core`
- Add a cluster-wide `SET statement_timeout = '15s'` default on the gateway pool to fail fast if any tx accidentally re-introduces the antipattern

**Acceptance Criteria**

- `git grep -nE 'await.*(llm|embed|httpx).*' -- services/` shows no occurrences inside `async with conn.transaction():` blocks
- Pool wait p99 under load test improves measurably

**Test Plan**

- Integration: throttle Ollama (sleep 5s); verify the gateway still serves `/api/today` within request budget instead of 503-ing
- Unit (regression): a static check (custom AST walker) asserts no `await` to known-network helpers inside `transaction()` context managers

---

### P0-6: `applied_triggers` double-apply race causes stuck triggers

**Priority:** P0 · **Effort:** S

**Problem**

`services/think/applier.py:102-124`: SELECT-then-INSERT against `applied_triggers` (`trigger_id` PK). Two concurrent workers processing the same `trigger_id` collide on the PRIMARY KEY. The `IntegrityError` is unhandled, the trigger is marked failed, retries hit the same race.

**Impact**

Correctness: idempotency contract broken; triggers can be permanently dead-lettered while having actually applied successfully on the racing worker.

**Proposed Change**

Replace the read+insert with `INSERT ... ON CONFLICT (trigger_id) DO NOTHING RETURNING outcome`. If `RETURNING` returns nothing (conflict), `SELECT outcome FROM applied_triggers WHERE trigger_id = $1` to read the winner's outcome and treat the current call as already-applied.

**Acceptance Criteria**

Concurrent test with N=20 workers all calling `apply_diff(same_trigger)` produces one applied row and N-1 idempotent acknowledgements; zero failures, zero dead-letters.

**Test Plan**

Unit: simulate concurrency with `asyncio.gather(*[apply_diff(...) for _ in range(20)])` against a real test DB.

---

## P1 — High

---

### P1-1: `GATEWAY_MOUNT_SIM=1` enabled in production docker-compose

**Priority:** P1 · **Effort:** S

**Problem**

`docker-compose.yml:47` hardcodes `GATEWAY_MOUNT_SIM: "1"`. This mounts `services/synthetic/` authoring endpoints (personas, channels, message inject) on the public gateway. Code default in `services/gateway/main.py:1877-1879` would have set `0` for prod, but the env var overrides it. Synthetic service has zero tests and bypasses dedup/T1 enqueue checks.

**Impact**

Security: untrusted authoring surface in prod; can corrupt the substrate.

**Proposed Change**

- Set `GATEWAY_MOUNT_SIM: "0"` in `docker-compose.yml`
- In `main.py`, refuse to mount sim if `COMPANY_OS_ENV == "prod"` regardless of env override, with a startup error

**Acceptance Criteria**

`/sim/*` returns `404` in prod. Startup logs the mount decision explicitly.

**Test Plan**

Integration: prod-mode app fixture asserts `/sim/personas` is `404`.

---

### P1-2: `think_trigger_queue` lease has no timeout or heartbeat

**Priority:** P1 · **Effort:** M

**Problem**

`services/think/worker.py:321-331`: rows are leased via `locked_by = worker_id, locked_at = now()`. There is no expiry filter on the poll query and no in-flight heartbeat. A crashed or hung worker leaves the row locked forever; new workers don't see it.

**Impact**

Reliability: silent queue stalls; throughput drops to zero for affected partitions; only manual SQL recovery.

**Proposed Change**

- Add `AND (locked_at IS NULL OR locked_at < now() - interval '5 minutes')` to the poll query
- In `_process_trigger`, spawn a background heartbeat task that updates `locked_at = now()` every 30s and is cancelled on completion
- Worker startup runs `UPDATE think_trigger_queue SET locked_by = NULL, locked_at = NULL WHERE locked_at < now() - interval '10 minutes' AND completed_at IS NULL` to recover orphans

**Acceptance Criteria**

- Killing a worker mid-think and restarting it recovers the row within 5 min
- Unit test simulates a hung worker; second worker re-acquires after timeout

**Test Plan**

Integration: spawn worker → simulate hang via `await asyncio.sleep(600)` → second worker takes over.

---

### P1-3: `revenue_at_risk_usd NUMERIC` silently truncates cents

**Priority:** P1 · **Effort:** S

**Problem**

`db/migrations/0014_customer_commitments_superset.sql:50`: `NUMERIC` (no precision) defaults to integer-only on inserts via Python `Decimal('123.45')`. Other money columns explicitly use `NUMERIC(10,6)` (`0018_view_render_costs.sql`).

**Impact**

Correctness: financial data loss; commitments view shows wrong dollar values.

**Proposed Change**

New migration `0026_fix_revenue_numeric.sql`: `ALTER TABLE customer_commitments ALTER COLUMN revenue_at_risk_usd TYPE NUMERIC(14,2)`. Backfill existing rows from source observation payloads if available; otherwise null and log a one-time migration report.

**Acceptance Criteria**

New inserts preserve cents; `psql \d customer_commitments` shows `numeric(14,2)`.

**Test Plan**

Unit: insert `Decimal('123.45')`, read back `Decimal('123.45')`.

---

### P1-4: Auth `/auth/session` does not cross-check `X-Tenant-Id` against body

**Priority:** P1 · **Effort:** S

**Problem**

`services/gateway/main.py:559-566`: the session-mint endpoint trusts `tenant_id` from JSON body without comparing to the header. Combined with P1-5 below, this expands the bootstrap-secret blast radius.

**Impact**

Security: weakens defense-in-depth on session creation.

**Proposed Change**

If `X-Tenant-Id` header present, require it to equal body `tenant_id`; otherwise `400`.

**Acceptance Criteria**

Unit tests for match/mismatch/absent header.

**Test Plan**

`test_auth_session_rejects_tenant_mismatch`

---

### P1-5: No CI job runs unit/integration tests on PRs

**Priority:** P1 · **Effort:** M

**Problem**

`.github/workflows/` contains only `deploy-production.yml`, `enforce-main-source.yml`, and `real-llm-nightly.yml`. No PR-triggered job runs pytest. PRs can merge to main with broken tests; only the nightly cron exposes the failure.

**Impact**

Reliability: regressions ship unnoticed for up to 24h; "merge to main" provides no signal.

**Proposed Change**

New `.github/workflows/test.yml` triggered on `pull_request` and push-to-main:
- Boot postgres + ollama services
- `pytest tests/unit lib services -m "not integration and not ollama and not real_llm" --timeout=30 --maxfail=10`
- Separate job for `-m integration` against the booted services
- `cd ui && npm ci && npm run build && npm test -- --run`
- Branch protection: require both jobs to pass before merge

**Acceptance Criteria**

Opening a PR with a deliberately broken test fails CI within 5 min.

**Test Plan**

Demonstrate via a throwaway PR.

---

### P1-6: Rendering / query HTTP adapter has no retry, no circuit-breaker

**Priority:** P1 · **Effort:** M

**Problem**

`services/greeting/rendering_adapter.py:342-351, 469-490`: `_post()` makes a single httpx call with a 30s timeout, no retries. On failure the caller silently falls back to `_synthesize_placeholder_reasoning`, hiding the failure.

`services/query/adapters.py:187`: `httpx.AsyncClient` is recreated per call (no pooling).

**Impact**

Reliability + Performance: brief RND blips → silent placeholder responses, no alarm; per-call client adds TLS overhead.

**Proposed Change**

- Pool a single `httpx.AsyncClient` across the adapter's lifetime
- Add tenacity-style retry: 3 attempts, exponential backoff (250ms, 500ms, 1s) on `ConnectError | ReadTimeout | RemoteProtocolError | 5xx`
- On final failure, surface a structured `RenderingUnavailableError` and log with `tenant_id`, `card_id`, `attempts` — do not silently placeholder

**Acceptance Criteria**

- Adapter completes in <250ms p50 under no-failure load (vs current per-call client setup)
- On 3 forced 503s, raises `RenderingUnavailableError` with telemetry; on transient 1×503 then 200, succeeds

**Test Plan**

Unit with respx: simulate transient + permanent failures.

---

### P1-7: `RequestContextMiddleware` re-raises but logging itself can fail

**Priority:** P1 · **Effort:** S

**Problem**

`services/gateway/main.py:134-142`: the middleware does `log.error(...)` then `raise`. If structlog or the renderer throws (e.g., circular ref in payload), the original exception is lost and the request returns an opaque 500 with no trace.

**Impact**

Observability: lost root-cause for some 500s.

**Proposed Change**

Wrap the log call in `try/except Exception: print(...; sys.stderr)`; re-raise only the original exception.

**Acceptance Criteria**

Unit test injects a logger that raises; original exception still propagates with stderr fallback message.

**Test Plan**

`test_request_context_middleware_survives_logging_failure`

---

### P1-8: Frontend race condition in `useToday` polling — late responses overwrite state

**Priority:** P1 · **Effort:** M

**Problem**

`ui/src/hooks/useToday.ts:145-162`: `setInterval(() => getToday())` without an `AbortController` per tick. Tab visibility changes can leave multiple in-flight requests; later one wins on the wire but the earlier response calls `setToday` after, causing UI flicker / wrong-state.

Same pattern around the 600ms triage `setTimeout` chain (`ui/src/hooks/useToday.ts:240-256`) — no mounted-guard.

**Impact**

Correctness: visible flicker in CEO view; potential stale state showing dismissed cards.

**Proposed Change**

Use a single `AbortController` ref, abort on each new tick and on unmount; replace `setTimeout` with a mounted-ref guard.

**Acceptance Criteria**

Cypress/Playwright test: rapid focus toggle does not produce mismatched state.

**Test Plan**

Vitest: simulate concurrent fetch resolutions out of order; assert latest-only state.

---

### P1-9: WebSocket auth token transmitted as URL query parameter

**Priority:** P1 · **Effort:** M

**Problem**

`ui/src/api/stream.ts:36`: `?token=${encodeURIComponent(token)}` — tokens land in nginx access logs, browser history, referer headers, third-party tracing.

**Impact**

Security: token leakage; replay risk.

**Proposed Change**

Have client open WS without token; on open, send a JSON auth frame; gateway validates and binds session before forwarding.

**Acceptance Criteria**

tcpdump/log inspection of WS upgrade shows no token in URL; auth still required to receive frames.

**Test Plan**

Integration: connect with no token → connection rejected after auth timeout. Connect with valid frame → events flow.

---

### P1-10: TS `skipLibCheck: true` masks type errors in shared API contracts

**Priority:** P1 · **Effort:** M

**Problem**

`ui/tsconfig.json` has `"skipLibCheck": true`, meaning generated/imported `.d.ts` mismatches don't fail the build. Combined with widespread `any` (`ui/src/hooks/useToday.ts:101`, `ui/src/api/recommendation-stream.ts:111`), API-shape regressions ship silently.

**Impact**

Correctness: silent runtime errors when backend contract drifts.

**Proposed Change**

Set `skipLibCheck: false`; replace `any` in stream parsers with explicit typed unions; CI enforces `tsc --noEmit`.

**Acceptance Criteria**

`npm run typecheck` passes with `skipLibCheck: false`.

**Test Plan**

Add `npm run typecheck` to CI.

---

## P2 — Medium

---

### P2-1: `think_run_artifacts` unbounded growth, no partitioning, no retention

**Priority:** P2 · **Effort:** M

`db/migrations/0020_think_run_artifacts.sql` — append-only, ~9 rows per think run, JSONB payloads, no TTL, no partition. Will reach tens of GB per month per tenant.

**Proposed Change:** Convert to monthly partitions on `captured_at`; add a daily `pg_cron` job dropping partitions > 30 days; document retention.

**Acceptance Criteria:** `\d+ think_run_artifacts` shows partitioned; cron job exists; metric tracks rows/day.

---

### P2-2: Dead-letter tables written but not consumed or alerted

**Priority:** P2 · **Effort:** M

`services/think/worker.py:556-601` writes `model_reeval_dead_letter`; same for post-commit dead-letter. No admin endpoint, no metric, no alert.

**Proposed Change:** (a) `/api/admin/dead_letter?table=...` admin endpoint (auth-gated); (b) emit `dead_letter_depth` metric per `(tenant, table)`; (c) document operator runbook.

---

### P2-3: Worker crash recovery leaves orphan rows in `think_trigger_queue`

**Priority:** P2 · **Effort:** S _(combined with P1-2)_

Same root cause as P1-2. Ensure the `worker.run()` startup sweep is idempotent and logs how many orphans it recovered.

---

### P2-4: `post_commit_pending_idx` lacks `tenant_id`; per-tenant dispatch is O(all_pending)

**Priority:** P2 · **Effort:** S

`db/migrations/0015_post_commit_durability.sql:71-73`: partial index on `(scheduled_at)` only.

**Proposed Change:** Drop and recreate as `(tenant_id, scheduled_at) WHERE processed_at IS NULL AND dead_lettered_at IS NULL`. Verify with `EXPLAIN` on the worker poll query.

---

### P2-5: `post_commit_actions` dedup race between INSERT and `processed_at` update

**Priority:** P2 · **Effort:** S

`post_commit.py:417-428`: unique constraint uses `NULLS NOT DISTINCT`, but a fresh INSERT between dispatch and `mark_processed` doesn't collide with the now-non-NULL `processed_at` row → duplicate dispatch.

**Proposed Change:** Switch to `INSERT ... ON CONFLICT ... DO UPDATE SET processed_at = now()` to atomically dedupe.

---

### P2-6: Embedding network call inside `ModelsRepo.insert` transaction

**Priority:** P2 · **Effort:** S _(subset of P0-5)_

Covered by P0-5; pulled out as separate task because the fix in `lib/shared` repo is independent.

---

### P2-7: f-string SQL building in `simulation/reset.py` thrashes the statement cache

**Priority:** P2 · **Effort:** S

`simulation/reset.py:55-176` — query text varies per call; asyncpg's 100-statement cache evicts under load.

**Proposed Change:** Use a single parameterised query with conditional WHERE clauses appended via positional args; never build column/path names from variables.

---

### P2-8: Debug router (`/debug/*`) exposed when `COMPANY_OS_ENV=staging`

**Priority:** P2 · **Effort:** S

`services/gateway/main.py:1933` — gating by env name leaks raw prompts and substrate in any environment a developer labels "staging".

**Proposed Change:** Gate on a separate `DEBUG_ENDPOINTS_ENABLED` flag, defaulting off; dev compose can opt in.

---

### P2-9: Bootstrap-secret check skipped silently in non-prod when secret unset

**Priority:** P2 · **Effort:** S

`services/gateway/main.py:521-536` — if `COMPANY_OS_ENV != prod` and `AUTH_BOOTSTRAP_SECRET` blank, anyone can mint a session.

**Proposed Change:** Log a startup `WARN` if non-prod and unset; refuse boot if `COMPANY_OS_ENV in {"staging", "test"}` and unset.

---

### P2-10: f-string interpolation patterns in gateway query construction

**Priority:** P2 · **Effort:** S

`services/gateway/debug_router.py:350` and `services/gateway/main.py:1660` — table/column names interpolated into SQL strings. Currently safe (allowlists exist) but fragile.

**Proposed Change:** Replace with explicit dict-of-allowed-tables + asyncpg positional params for values.

---

### P2-11: `_record_cost` background task swallows exceptions silently

**Priority:** P2 · **Effort:** S

`services/rendering/core.py:705-714` — `loop.create_task(_do_insert())` with no `add_done_callback`. Cost rows lost on transient DB failure; billing/metrics blind.

**Proposed Change:** `task.add_done_callback(lambda t: t.exception() and log.error(...))`.

---

### P2-12: Silent exception swallowing in query/retrieval handlers

**Priority:** P2 · **Effort:** M

- `services/query/core.py:227-229`: card resolver bare `except Exception` → returns wrong-context answers
- `services/query/core.py:360-364`: prefetch deserialization bare `except` → cache poisoning invisible
- `services/retrieval/assembler.py:478-481`: bridge revenue lookup → silent `None`, rendering proceeds

**Proposed Change:** Distinguish transient (`asyncio.TimeoutError`, `ConnectionError`) vs permanent (`KeyError`, `ValidationError`); log structured payload with `tenant_id` and triggering id; surface a `bridge_context_incomplete=True` flag where partial degradation is acceptable.

---

### P2-13: Worker liveness has no heartbeat or `/health`

**Priority:** P2 · **Effort:** M

`scripts/run_think_worker.py` and `scripts/run_post_commit_worker.py` expose no health surface; orchestrators can't detect a hang.

**Proposed Change:** Either expose a tiny aiohttp `/health` on each worker, or write `worker_heartbeats(worker_id, last_seen, queue_depth)` every 10s; alert if `now - last_seen > 60s`.

---

### P2-14: Token/cost tracking lacks per-call/per-tenant ceilings

**Priority:** P2 · **Effort:** M

`lib/llm/provider.py` — callers can request arbitrary `max_tokens`; no global cap; no tenant budget.

**Proposed Change:** Env-driven `LLM_MAX_TOKENS_PER_CALL` and per-tenant cap in `think_run_costs` aggregation with a circuit-breaker that returns `429` once exceeded.

---

### P2-15: Token-counting fields fallback to 0 on missing usage

**Priority:** P2 · **Effort:** S

`lib/llm/provider.py:200-217` — silently returns `(0,0)` if SDK rename happens; cost tracking quietly breaks.

**Proposed Change:** Raise a typed error or at minimum `log.warning("usage_missing", provider=..., model=...)`.

---

### P2-16: `archive_reason` is free `TEXT` — no DB `CHECK` constraint

**Priority:** P2 · **Effort:** S

`db/migrations/0001_foundation.sql:157-158` — Pydantic `Literal` enforced in app only; raw SQL inserts can corrupt.

**Proposed Change:** Add `CHECK` constraint via new migration with the documented allowed set.

---

### P2-17: asyncpg `statement_cache_size` left at default (100)

**Priority:** P2 · **Effort:** S

`lib/shared/db.py:66-71` and `services/gateway/db_bootstrap.py:94-100` — many distinct query shapes; cache thrashes under load.

**Proposed Change:** Set `statement_cache_size=500`; document in the constructor; verify Postgres `pg_stat_statements` shows reduced planning time.

---

### P2-18: `MAX_PAYLOAD_BYTES` checked after reading whole body

**Priority:** P2 · **Effort:** S

`services/gateway/main.py:591` — full request body is read before size check, allowing memory DoS.

**Proposed Change:** Inspect `Content-Length` header first and reject early; if absent, stream-read with running counter.

---

### P2-19: Cascade depth check uses `>= MAX_CASCADE_DEPTH` (off-by-one)

**Priority:** P2 · **Effort:** S

`services/think/worker.py:366-390` — the documented bound is "≤ 5 generations" but actual reject fires at 5 (so 0..4 succeed).

**Proposed Change:** Either change to `>` (allowing 5) or update docs to match the strict bound. Add a unit test pinning the chosen behavior.

---

### P2-20: TRUNCATE-based test isolation hides flaky cleanup

**Priority:** P2 · **Effort:** M

`conftest.py:102-115` uses `TRUNCATE` per test; if a test crashes mid-teardown, next test inherits dirty state.

**Proposed Change:** Switch hot tests to per-test transactions with rollback (asyncpg savepoint pattern); reserve `TRUNCATE` for tests that genuinely need committed state.

---

### P2-21: Real-LLM session-scoped response cache pollutes across tests

**Priority:** P2 · **Effort:** S

`tests/real_llm/conftest.py:88-96` — cache is `scope="session"`; one bad fixture poisons every subsequent run.

**Proposed Change:** Function-scope or expose a `cache.clear_for(test_id)` and call it in autouse fixture.

---

### P2-22: `services/synthetic/` has zero tests despite being a substrate-injection path

**Priority:** P2 · **Effort:** M

~288 LoC, no `services/synthetic/tests/`. Bypasses dedup and T1 enqueue.

**Proposed Change:** Add unit tests for `ingest_signal()` covering each `skip_*` flag combination; assert that prod-mode rejects sim ingestion regardless.

---

### P2-23: Strategy mocks in `services/query/tests/test_core.py` skip real ranking

**Priority:** P2 · **Effort:** M

`services/query/tests/test_core.py:39-48` — replaces every retrieval strategy with `FakeStrategy`. The dispatcher is tested; the actual scoring (RRF, relevance) is not.

**Proposed Change:** Replace with deterministic seed data + real strategies; mock only the embedder.

---

### P2-24: `RenderingService` test layer never exercises real provider serialization

**Priority:** P2 · **Effort:** M

`services/rendering/tests/test_api.py:62-66` — `ScriptedProvider` returns prebaked strings. We never assert that real Anthropic/OpenAI/DeepSeek responses parse correctly.

**Proposed Change:** Mock at httpx layer with respx, using realistic provider response shapes captured from `real_llm` runs.

---

### P2-25: Backoff formulas inconsistent between think and post-commit workers

**Priority:** P2 · **Effort:** S

- think: `worker.py:520` → `min(300, 10 * 2**min(attempts,4))` → `[20, 40, 80, 160, 300]`
- post_commit: `post_commit.py:346` → `2 * 2**(attempts-1)` → `[2, 4, 8, 16, 32]`

**Proposed Change:** Centralize a `compute_backoff(attempt: int) -> float` helper; pin to one formula; add unit tests.

---

### P2-26: `view_ceo_cache` refresh under stampede uses no advisory lock

**Priority:** P2 · **Effort:** S

Combined with P0-4: even after leader election, two leaders briefly during failover can both refresh. Add `pg_try_advisory_xact_lock(hashtext('view_ceo_refresh:' || tenant_id))` inside the refresh tx.

---

### P2-27: Frontend `dangerouslySetInnerHTML` on server-rendered HTML lacks client-side sanitization

**Priority:** P2 · **Effort:** M

Many sites: `Conversation.tsx:25`, `RecCard.tsx:502,517,676`, `JustUpdated.tsx:28`, `SignalStrip.tsx:35`, `ArtifactDrawer.tsx:130`. If any backend path ever interpolates user input into HTML, XSS opens.

**Proposed Change:** Run all such HTML through `DOMPurify` (`isomorphic-dompurify`) in a single helper. Audit backend rendering paths in the same pass.

---

### P2-28: Demo-picker localStorage holds auth tokens

**Priority:** P2 · **Effort:** M

`ui/src/api/demo-picker-client.ts:102-106` — token + session id + tenant id in `localStorage`; XSS-stealable.

**Proposed Change:** Move to `httpOnly + Secure + SameSite=Lax` cookie set by gateway; UI keeps only display state.

---

### P2-29: `ResizeObserver` per `RecCard`, no debounce

**Priority:** P2 · **Effort:** S

`ui/src/components/RecCard.tsx:235-240` — observer per card; on viewport resize, all cards run their callback simultaneously.

**Proposed Change:** Single shared `ResizeObserver` in a context provider; or rAF-debounce per card.

---

### P2-30: 12-card hardcoded slice in CEO view feed

**Priority:** P2 · **Effort:** M

`ui/src/App.tsx:482` — `visibleCards.slice(0, 12)`. Filter matches beyond 12 are invisible; no "load more" affordance.

**Proposed Change:** Replace with infinite-scroll via `IntersectionObserver`, or expose pagination explicitly.

---

### P2-31: `OllamaClient.embed_batch` fans out single requests; no global backoff under 503

**Priority:** P2 · **Effort:** S

`lib/embeddings/ollama.py:115-149` — concurrent fan-out, each request retries independently → thundering herd to Ollama.

**Proposed Change:** Per-client semaphore + shared backoff state when any request 503s.

---

### P2-32: `GATEWAY_OWNS_POOL` and `GATEWAY_START_GRT_SCHEDULER` undocumented

**Priority:** P2 · **Effort:** S

Both flags drive critical lifecycle behavior; neither in `.env.example`.

**Proposed Change:** Document both; add startup validation that warns if `OWNS_POOL=1` but the lifespan didn't actually create the pool.

---

### P2-33: `rendering_adapter` async tasks don't inherit request context

**Priority:** P2 · **Effort:** S

`services/gateway/main.py:386` — dispatcher tasks started from lifespan don't carry `request_id`/`tenant_id` contextvars.

**Proposed Change:** `contextvars.copy_context()` at task creation; or pass an explicit `LogContext` argument.

---

### P2-34: `OLLAMA_URL` defaults to `http://localhost:11434` in client constructor

**Priority:** P2 · **Effort:** S

`lib/embeddings/ollama.py:43` — default reaches localhost from inside a container if env not set; silent failure.

**Proposed Change:** Default to empty; raise on instantiation if both env and arg unset.

---

### P2-35: `think_run_artifacts` + debug_capture writes inside the apply transaction

**Priority:** P2 · **Effort:** S

`services/think/reason.py:410-633` — debug rows extend critical-section duration even when capture is for observability only.

**Proposed Change:** Buffer captures in memory; flush after commit. Also default `DEBUG_ARTIFACT_CAPTURE=0` in prod.

---

### P2-36: Custom `<span role="button">` in `JustUpdated` — not keyboard accessible

**Priority:** P2 · **Effort:** S

`ui/src/components/JustUpdated.tsx:35-39` — no key handler.

**Proposed Change:** Replace with `<button type="button">`. Same audit pass over `RecCard`'s `<article tabIndex={0}>` for `aria-label`.

---

## P3 — Low

| # | Finding | Effort |
|---|---------|--------|
| P3-1 | Token-bucket rate limit lacks key-collision and concurrency tests (`rate_limit.py`) | S |
| P3-2 | `acts/retry.py` only tested as side-effect; extract dedicated unit suite | S |
| P3-3 | Slack signature verification has no dedicated unit test (only via integration) | S |
| P3-4 | `models.archive_reason` Literal not pinned by DB CHECK (covered by P2-16) | S |
| P3-5 | `LLMResponseCache` keys on `schema.__name__` only — collision risk if two Result Pydantic models exist (`provider.py:687`) | S |
| P3-6 | Repeated `_jsonify`/`_model_dump`/`_serializable_response` helpers in `query/core.py` could be unified | S |
| P3-7 | `cascade_depth` semantic is off-by-one; doc fix or boundary fix (duplicate of P2-19 — choose one) | S |
| P3-8 | Sidebar `localStorage.setItem` on every toggle, not batched (`ui/src/components/Sidebar.tsx:26-36`) | S |
| P3-9 | `LLM_PROVIDER` env-validation: provider chosen but matching key absent → unhelpful error path (`provider.py:494-519`) | S |
| P3-10 | `repair_note` concatenation into user prompt — fragile if `PydanticValidationError` text becomes user-controlled later (`provider.py:737-742`) | S |
| P3-11 | Stream JSON parse errors silently dropped in `ui/src/api/stream.ts:56-62` and `recommendation-stream.ts:99-129`. Add `console.error` and an error callback. | S |
| P3-12 | `useAsk` doesn't clear stale error on success (`ui/src/hooks/useAsk.ts:42`) | S |
| P3-13 | Lazy import of `revenue_at_risk_for_customer` masks circular dep (`assembler.py:471-474`). Add a static import-graph test. | S |
| P3-14 | `ref_type` → table lookup in `recommendations/repo.py:173-192` safe today; add an `assert ref_type in _REF_TYPE_TO_TABLE` guard | S |
| P3-15 | `structural_sibling_expansion_enabled` flag never read (`retrieval/config.py:91-92`). Either wire it or annotate as deferred. | S |
| P3-16 | `card_resolver` and `access_context_builder` in `query/core.py` are dead parameters until Agent-GRT lands; document or remove | S |
| P3-17 | Worker loop error logs lack `tenant_id`/`run_id`/`trigger_id` structured fields | S |
| P3-18 | `Turn.created_at` is not enforced as tz-aware; risk on serialization round-trip (`query/core.py:84,372-381`) | S |
| P3-19 | `_compute_bridge_context` partial degradation should set `bridge_context_incomplete=True` flag | S |
| P3-20 | `services/query/core.py` and `services/recommendations/repo.py` lack type hints on private helpers | S |
| P3-21 | `ArtifactDrawer` link affordance — no underline/cursor styling (`ui/src/components/ArtifactDrawer.tsx:77`) | S |

---

## Cross-Cutting Reports

### Areas with NO Tests

- `services/synthetic/` (~288 LoC, substrate injection path)
- `services/workers/maintenance/scheduler.py` (orchestration; only indirect coverage)
- `services/ingestion/handlers/slack.py` signature verification (covered only as integration side-effect)
- Standalone `acts/retry.py` retry/backoff logic

### Flows That Cannot Currently Be Tested

- Real LLM correctness — gated by `RUN_REAL_LLM=1` + `DEEPSEEK_API_KEY`; nightly-only
- `ThinkWorker` in unit context — requires live Postgres + asyncio loop. No fakeable harness.
- Migration idempotency / rollback — current conftest runs them once at session start, no per-test reset
- Multi-replica scheduler / worker behavior (no harness to spin two instances)

### Fragile Logic / Hidden Coupling

- LLM/embedding network calls held inside DB transactions (P0-5)
- Lazy import in `retrieval/assembler.py` to avoid circular dep with `resources/bridge`
- `applied_triggers` idempotency relies on a SELECT-then-INSERT pattern racing against concurrent workers
- Scheduler relies on in-process `_started` flag for coordination
- Module-level mutable state in test fixtures (`ScriptedProvider.responses`)
- Real-LLM session-scoped cache (`scope="session"`) implicitly couples tests
- `DEFAULT_TENANT_ID = '00000000-...-0001'` hardcoded across env files; test-vs-prod tenant confusion

### Undefined Behavior

- Two replicas where both have `GATEWAY_OWNS_POOL=1` — pool can be closed while in use
- Migration ordering of duplicate `0014_*` files
- `applied_triggers` race outcome on PK collision (currently raises uncaught)
- Numeric coercion of `Decimal('123.45')` into a `NUMERIC` column with default precision (silent truncate)
- WS retry timer firing after factory unmount (module-scoped state)

---

## Quick Wins

> High impact / low effort — do these first.

| Task | Why | Effort |
|------|-----|--------|
| P0-1 Slack signature bypass fix | Closes auth bypass, ~20 LoC | S |
| P0-2 Rename duplicate 0014 migration | Removes a class of latent bugs | S |
| P0-3 Strip Authorization from logs | Direct compliance + security gain | S |
| P0-6 `ON CONFLICT DO NOTHING RETURNING` | Single-query fix, regression-test exists | S |
| P1-1 Flip `GATEWAY_MOUNT_SIM=0` | One-line docker-compose change | S |
| P1-3 `NUMERIC(14,2)` migration | Single ALTER, prevents silent loss | S |
| P2-4 Add `tenant_id` to post-commit pending index | One `CREATE INDEX` | S |
| P2-15 Loud-fail on missing LLM usage fields | Prevents silent cost-tracking outage | S |
| P2-17 Bump `statement_cache_size` to 500 | One-line config, big planner win | S |
| P2-25 Centralize backoff formula | Removes confusing divergence | S |

---

## High-Risk Areas

> Likely production incidents if left unaddressed.

1. **Think pipeline transaction scope (P0-5)** — single biggest stability risk. Any LLM/Ollama latency spike → cascading pool exhaustion.
2. **Scheduler stampede (P0-4)** — first horizontal scale-out will multiply cost and contend on `view_ceo_cache`.
3. **Trigger queue lock leakage (P1-2)** — first worker crash will silently drop throughput on affected partitions.
4. **Slack ingest bypass (P0-1)** — externally exploitable; compromises substrate integrity.
5. **No PR test gate (P1-5)** — every other fix can regress silently.
6. **`applied_triggers` PK race (P0-6)** — under any concurrency, idempotency contract breaks.

---

## Test Harness Recommendations

| Harness | Description |
|---------|-------------|
| `FakeOllama` | In `lib/embeddings/tests/fakes.py`, deterministic embedder; promote to package-level fixture |
| `InMemoryThinkQueue` | Adapter that the worker can run against, for unit-testing trigger lifecycle without Postgres |
| `respx` mocking | At the LLM-provider HTTP boundary, replacing today's `ScriptedProvider` so wire-format regressions are caught |
| Per-test transaction fixture | Wrap each integration test in `BEGIN; … ROLLBACK` (asyncpg savepoint) instead of `TRUNCATE` |
| Static check | `tests/static/test_no_io_in_tx.py` walks the AST looking for `await llm/embed/httpx` inside `transaction()` context managers; fails the build |
| `HarnessGateway` | `TestClient` factory that lets tests assert env-mode mounting decisions |

---

## Summary & Sprint Plan

### Summary by Priority

| Priority | Count | Total Effort |
|----------|-------|--------------|
| P0 | 6 | 1×L, 1×M, 4×S |
| P1 | 10 | 4×M, 6×S |
| P2 | 36 | 7×M, 29×S |
| P3 | 21 | 21×S |
| **Total** | **73** | **≈ 10 engineer-weeks** |

> P0+P1 ≈ 4 engineer-weeks · P2 ≈ 6 weeks · P3 ad-hoc

### Recommended Sprint Sequence

**Sprint 1** — Compliance + CI Gate
> P0-1, P0-2, P0-3, P0-6, P1-1, P1-3, P1-5

**Sprint 2** — Stability-Defining Changes
> P0-4, P0-5, P1-2

**Sprint 3** — Data Integrity + Remaining P1
> P1-6 through P1-10, P2-1, P2-4, P2-5, P2-16, P2-17

**Ongoing** — Backlog hardening sprints
> Remaining P2/P3 items
