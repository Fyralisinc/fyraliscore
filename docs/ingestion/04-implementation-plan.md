# Fyralis Ingestion — Implementation Plan

**Canonical reference:** `00-system-design.md` is the source of truth for architectural intent and non-negotiables N1–N5. Every milestone below names which non-negotiables it discharges. **A milestone cannot be cut in scope below the point where one of its named non-negotiables is no longer satisfied.** This is the test for "is this trim acceptable?"

**Scope:** Sequenced migration from the current forward-only webhook/poller code (Phase 1 state) to the Temporal + Kafka + S3 + Redis backfill architecture specified in `02-high-level-design.md` v2.1 and `03-low-level-design.md` v3.1.

**Read first:** this plan assumes familiarity with the canonical doc (00), HLD (02), and LLD (03). It does not re-explain architecture; it sequences the work, names what blocks what, and identifies the tests that gate each milestone.

**Status of this plan:** the milestones are ordered by hard dependency (substrate before workflows; workflows before cutover; cutover before backfill). The effort estimates (S/M/L) are eyeball numbers, not story points — they communicate relative cost, not absolute schedule. Phase 4 implementation will reveal sequencing decisions that need revision; this plan should be re-validated at the start of each milestone.

*Coherence audit (v4.1 amendment): each milestone now declares which N1–N5 non-negotiables it discharges. This makes scope-cuts auditable against `00-system-design.md` §2.*

---

## Table of contents

1. [Gap analysis](#1-gap-analysis)
2. [Sequenced milestones](#2-sequenced-milestones)
3. [Critical path (must do first)](#3-critical-path-must-do-first)
4. [Deliberate deferrals](#4-deliberate-deferrals)
5. [Test strategy](#5-test-strategy)
6. [Open questions / decisions needed](#6-open-questions--decisions-needed)

---

## 1. Gap analysis

Effort key: **S** = ≤2 days, **M** = ≤2 weeks, **L** = >2 weeks (or coordinated cross-team).
Risk-if-deferred key: **Low** = nothing else blocks on it; **Med** = limits one milestone; **High** = blocks the cutover.

### 1.1 Schemas (LLD §1)

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| `onboarding_runs` table | missing | S | High — workflow can't write progress |
| `onboarding_shards` table | missing | S | High — planner output has nowhere to go |
| `ingestion_failures` table | missing | S | Med — DLQ has no queryable mirror |
| `onboarding_triggers` (OAuth outbox) | missing | S | High — workflow trigger mechanism |
| `gateway_session_state` (Discord) | missing | S | Med — Discord crash recovery |
| `tenant_flags` (cutover flag) | missing | S | High — cutover ungated without it |
| `entity_aliases_normalized_idx` functional index | missing | S | High — batched alias lookup degrades |
| `pgbouncer` infra | missing | M | High — writer connection count blows past `max_connections` |
| `observations.UNIQUE(source_channel, external_id, occurred_at)` | correct (intentional, per Phase 2.1 Q A) | — | — |
| All existing OAuth substrate (`provider_installations`, `encrypted_secrets`, etc.) | correct | — | — |

### 1.2 Workflows (LLD §2)

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| `OnboardingTriggerPollerWorkflow` + Schedule | missing | M | High — no way to start tenant workflows |
| `TenantOnboardingWorkflow` | missing | S | High |
| `SourceOnboardingWorkflow` | missing | M | High |
| `ShardFetchWorkflow` | missing | M | High |
| `FeelsOnboardedMonitorWorkflow` + Schedule | missing | S | Med — Bridge has no feels_onboarded signal |
| `IngestionCircuitBreakerWorkflow` + Schedule | missing | M | High for cutover (gates rollback) |
| Temporal cluster (Cloud or self-hosted) | missing | M | High |
| Workflow test harness (time-skipping framework) | missing | M | Med — replay tests can't run |

### 1.3 Activities (LLD §2 + §4 + §7)

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| Trigger-claim / mark-consumed activities | missing | S | High |
| `create_or_get_onboarding_run`, `mark_shard_*`, `advance_shard_cursor` | missing | S | High |
| `publish_progress_event`, `publish_completion_events` | missing | S | Med |
| `measure_recency_gap`, `emit_feels_onboarded_and_stamp_run` | missing | S | Med |
| `fetch_page_slack` | missing | M | High (per-source critical path) |
| `fetch_page_github` | missing | M | High |
| `fetch_page_discord` | missing | M | Med (gateway is fallback) |
| `fetch_page_gmail` | missing | M | High |
| `reconcile_slack`, `reconcile_github`, `reconcile_gmail` | missing | M | Med |
| `reconcile_discord` (sparse sampling) | missing | M | Med |
| `measure_kafka_lag`, `update_breach_window`, `set_tenant_flag` | missing | M | High for cutover |
| `sample_breached_tenants_from_signal_topic` | missing | S | High for cutover |

### 1.4 Per-source planners (LLD §3)

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| `plan_shards_slack` | missing | M | High |
| `plan_shards_github` | missing | M | High |
| `plan_shards_discord` | missing | M | High |
| `plan_shards_gmail` | missing | M | High |
| `persist_shard_rows` shared helper | missing | S | High |

### 1.5 Data plane (LLD §5)

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| Kafka cluster (3 brokers, idempotent producer config) | missing | M | High |
| S3 bucket + lifecycle rules + IAM | missing | S | High |
| Redis cluster + Lua script loader | missing | M | High |
| Normalizer pool (multiprocessing supervisor + worker) | missing | M | High |
| Observation writer pool (aiokafka + batched INSERT) | missing | M | High |
| DLQ writer (consumer + UPSERT to `ingestion_failures`) | missing | S | Med |
| Embedding worker (Kafka consumer + Ollama + UPDATE) | missing | S | Med (Mode A) / High (replaces orphan accumulation) |
| Dual-mode writer config (Mode A + Mode B) | partial design / not implemented | M | Med (Mode B optional pending product call) |
| Redis SETNX dedup layer | missing | S | Low (defense in depth) |
| `canonicalize_gmail_batch_in_txn` (§5.6, post-cutover) | missing | M | Low — interim shape works |

### 1.6 Code changes to existing modules

| Component | Status | Effort | Risk if deferred |
|---|---|---|---|
| `services/webhooks/router.py` — flag-branched dual path | wrong-shape (inline only) | M | High |
| 4 × OAuth callbacks — outbox row in install transaction | wrong-shape (no outbox; per Phase 2.1 Q E1) | M | High |
| 4 × OAuth audit helpers — accept `TenantContext` | partial | S | Med |
| `services/integrations/discord/gateway/` — Redis leader lock + Postgres session UPSERT | wrong-shape (in-memory only) | M | High (data loss without it) |
| `lib/shared/db.py` — `statement_cache_size=0` on `create_pool` | missing | S | High (pgbouncer txn-mode incompatible without it) |
| 4 × outbound clients — remove ad-hoc 429 retry, call central limiter | wrong-shape (per-integration ad hoc) | M | Med |
| `services/entity_aliases/repo.py` — `find_by_aliases(aliases: list[str])` method | missing | S | High (writer batch perf) |
| Gmail `fetcher.py` / `history_poller.py` / `watch_scheduler.py` — convert to Temporal Schedules | wrong-shape (asyncio loops) | L | High for Gmail backfill |
| Gmail `_provision_install` docstring fix | wrong (claims idempotency it doesn't deliver) | S | Low (docstring only) |

### 1.7 Existing assets that don't change

| Component | Status |
|---|---|
| `services/ingestion/handlers/` — all 6 channel handlers + registry | correct |
| `services/ingestion/core.py::ingest()` | correct (called from writer post-cutover; signature unchanged) |
| `services/webhooks/signatures/` — all 5 signature verifiers | correct |
| `services/webhooks/tenant_resolver.py` | correct |
| `lib/shared/secrets/` — Fernet envelope encryption | correct |
| `observations` table schema + `think_trigger_queue` | correct |
| `services/integrations/{slack,github,discord}/uninstall.py` chokepoints | correct |

### 1.8 Test infrastructure

| Component | Status | Effort |
|---|---|---|
| Temporal test cluster (in-process via testsuite) | missing | M |
| Kafka test cluster (in-process or testcontainers) | missing | M |
| S3 test backend (moto or minio) | missing | S |
| Redis test instance | missing | S |
| Pgbouncer test instance (for connection-count assertions) | missing | S |
| Workflow replay test framework integration | missing | M |
| End-to-end small-tenant fixture | missing | M |

---

## 2. Sequenced milestones

Six numbered milestones (M1–M6) plus M7 (deferred refinements). M-Load is an M5-surfaced infrastructure work-unit inserted between M5 and M6 (production Kafka readers + synthetic-traffic dry run). M-Temporal is documented but **deferred indefinitely** ([05-lld-amendments.md](05-lld-amendments.md) A11) — M6 ships as asyncio services with pattern-alignment so a later Temporal port is mechanical rather than a rewrite. Each milestone has a gate; no milestone starts until the previous one's gate passes. Within a milestone, parallelisation is encouraged; the gate is the integration point.

### M1 — Foundational substrate

**Discharges non-negotiables:** none directly (foundational substrate). Enables all subsequent N1–N5 work; M1 is the gate without which no later milestone can deliver an N.

**Outcome:** all infra is provisioned, all new tables exist, the pgbouncer + statement_cache_size change ships, and a no-op normalizer/writer process pair runs against an empty Kafka topic without errors. Zero user-visible behavior change.

**Changes:**
- DDL migrations 0045 (onboarding_runs + onboarding_shards), 0046 (ingestion_failures), 0047 (onboarding_triggers), 0048 (gateway_session_state), 0049 (entity_aliases_normalized_idx CONCURRENTLY), 0050 (tenant_flags).
- Infra: Temporal Cloud namespace OR self-hosted Temporal cluster (decision per §6 Q2); Kafka cluster (3 brokers, topics created with config from LLD §10 and §11.3); S3 bucket with lifecycle rules; Redis (single instance for v1); pgbouncer (sidecar vs centralised per §6 Q1).
- `lib/shared/db.py` — `statement_cache_size=0` added to `create_pool`, DSN switched to pgbouncer endpoint.
- `pyproject.toml` — add `temporalio`, `confluent-kafka`, `aiokafka`, `aioboto3`, `redis`, `orjson`, `opentelemetry-*`.
- `services/ingestion/{normalizer,writers,raw_tier,rate_limit,progress,idempotency,feature_flags,workflows,activities,planners,reconciler}/__init__.py` — package skeletons.
- `services/ingestion/rate_limit/scripts/acquire.lua` + `report_retry_after.lua` + Python client.

**Tests that must pass:**
- `test_pool_pgbouncer_compatibility`: assert asyncpg pool works against pgbouncer with `statement_cache_size=0`; assert prepared-statement-required queries fail loudly (no silent regression).
- `test_redis_lua_acquire_and_refill`: full sequence of acquire/sleep/acquire; assert token refill math; assert lockout from `report_retry_after`.
- `test_s3_put_if_absent_idempotent`: PUT same content hash twice; second is no-op (412 PreconditionFailed handled).
- `test_kafka_producer_idempotent`: produce same message twice from same producer session; assert single broker copy via offset count.
- `test_migration_0045_to_0050_apply_and_rollback`: forward + backward migration on a clean DB.
- `test_functional_index_used_in_explain`: assert `EXPLAIN (FORMAT JSON)` for the batched alias query shows `Index Scan using entity_aliases_normalized_idx`.

**Risk if deferred:** everything downstream blocks. M1 is the hardest milestone to compress; underestimate at peril.

**Risk of running out of order:** none; M1 is the first.

---

### M2 — Raw tier shadow path

**Discharges non-negotiables:** N2 (Replayable from raw — every webhook body lands in S3 before transformation), N5 (Webhook and backfill converge — webhooks write to the same `ingestion.raw` topic that backfill will use). Tests gated by 48-hour zero-divergence comparison establish the shadow-path correctness foundation N1 depends on at M5.

**Outcome:** the webhook router writes every received payload to S3 AND publishes to `ingestion.raw`, **in addition to** calling the existing inline `ingest()`. A no-op normalizer + writer pair consumes the topic but does NOT write observations (write path is feature-flagged off). Operationally invisible to users; ops sees S3 fills and Kafka consumer-group lag stays at zero.

**Changes:**
- `services/ingestion/raw_tier/s3.py` — `PutIfAbsent`, content-hash key builder, zstd compression.
- `services/ingestion/raw_tier/envelope.py` — Pydantic envelope model.
- `services/webhooks/router.py` — after signature verify + tenant resolve, AND BEFORE returning the inline response, write to S3 and publish to `ingestion.raw` with `ingress_kind="webhook"`. Wrap in try/except — shadow path failure must NOT break the inline response (which is the user-visible behavior during M2).
- `services/ingestion/normalizer/worker.py` — consume `ingestion.raw`, transform via handler registry, produce to `ingestion.normalized`. Writes only metrics, no observations.
- `services/ingestion/writers/observation_writer.py` (no-op mode) — consume `ingestion.normalized`, log a shadow-write event, do NOT INSERT.
- Discord Gateway worker — add same shadow write to S3 + Kafka after every dispatched `MESSAGE_CREATE` (still calls existing inline path).
- Gmail Pub/Sub endpoint — same shadow write (still triggers existing fetcher).

**Tests that must pass:**
- `test_webhook_shadow_path_writes_to_s3_and_kafka`: send a Slack webhook, assert observation written via inline path AND S3 object exists AND Kafka message produced.
- `test_normalizer_consumes_shadow_without_writing`: produce a synthetic envelope, assert normalizer produces to `ingestion.normalized`, assert no row in observations table.
- `test_envelope_schema_version_invariant`: round-trip envelope through Pydantic; assert field set stable.
- `test_shadow_path_failure_does_not_break_inline`: inject S3 timeout; assert inline observation still written; assert error logged + DLQ candidate created.

**Risk if deferred:** can't validate the data-plane shape under production traffic before flipping the writer on. This is the test bed for M3-M5.

**Risk of running out of order:** if M1 isn't done, no infra to write to.

---

### M3 — Embedding worker (decoupled)

**Discharges non-negotiables:** N1 (Never lose data — fixes the `embedding_pending=TRUE` orphan accumulation identified in Phase 1 Risk #6; without a worker, observations land in DB but stay invisible to retrieval). N3 partial (separate Kafka topic isolates embedding work from observation-write work; one cannot starve the other).

**Outcome:** the new Kafka-based embedding worker is live; new observations from the existing inline path get embedded via the new worker (the existing inline-embedding code is left in place, but the worker is the primary). The pre-existing `embedding_pending=TRUE` backlog gets backfilled by a one-shot script.

**Changes:**
- `services/ingestion/writers/embedding_worker.py` — full implementation per LLD §5.4.
- `services/ingestion/core.py` — modify to publish to `ingestion.embedding` after successful INSERT (parallel to the existing inline embedding attempt; both write `embedding_pending=FALSE` under guard).
- `services/ingestion/recovery/embedding_backlog.py` — full implementation per LLD §12.1.
- Diagnostic query (Block 2) run on staging → determines backlog size → determines whether script runs as one-shot or as a multi-day rate-limited job.

**Tests that must pass:**
- `test_embedding_worker_consumes_and_updates`: write observation with `embedding_pending=TRUE`, publish to topic, assert worker UPDATEs row.
- `test_embedding_worker_concurrent_with_inline_safe`: race the worker against the existing inline embedder on the same row; assert single UPDATE wins (guard clause).
- `test_embedding_backlog_script_idempotent`: run script twice on same DB; second pass UPDATEs nothing.
- `test_embedding_backlog_script_rate_limited`: assert script does not exceed configured QPS.

**Risk if deferred:** the orphan accumulation (Phase 1 risk #6) continues. Every observation in the meantime adds to the backlog.

**Risk of running out of order:** independent of M2 result; can ship after M1.

---

### M4 — Discord Gateway leader election + session persistence

**Discharges non-negotiables:** N1 (Never lose data — fixes the in-memory `session_id`/`seq` data-loss window identified in Phase 1 Risk #3). N3 partial (Redis lease prevents multi-pod IDENTIFY collisions; one pod's crash does not affect others).

**Outcome:** the Discord Gateway worker holds a Redis lease before establishing a WS session; on every dispatched frame, it UPSERTs `gateway_session_state`. On worker crash, the new leader reads the persisted `session_id`/`seq` and RESUMEs. Pod scale-up no longer doubles IDENTIFY traffic.

**Changes:**
- `services/integrations/discord/gateway/leader_lock.py` — new module: Redis-based lease with 30s TTL refreshed every 10s.
- `services/integrations/discord/gateway/client.py` — wrap the `run()` loop in `acquire_leader_lease`; on every dispatch, fire-and-forget UPSERT to `gateway_session_state`.
- `services/integrations/discord/gateway/state.py` — new helper: load/save `GatewaySessionState` against Postgres.
- `scripts/start.sh` — ensure exactly one Discord Gateway worker container per region (existing convention; documented).
- Test: deploy two pods, assert one acquires the lease and the other waits; kill the leader, assert the waiter takes over and RESUMEs from persisted `seq`.

**Tests that must pass:**
- `test_leader_lock_single_holder`: two competing workers; assert only one holds the lock at a time.
- `test_leader_lock_release_on_crash`: holder process killed; new holder acquires within ~lease TTL.
- `test_gateway_session_persist_and_resume`: simulate dispatch frames; assert `session_id` and `last_seq` UPSERTed; restart worker; assert next IDENTIFY uses RESUME with the persisted values.
- `test_gateway_no_data_loss_on_planned_restart`: write N frames, kill worker, restart, assert N frames are in observations (no gap).

**Risk if deferred:** Phase 1 risk #3 persists — worker crashes silently drop messages in the recovery window.

**Risk of running out of order:** independent of M2/M3; can ship in parallel with M3.

---

### M5 — Steady-state cutover (the riskiest milestone)

**Status (2026-05-18): code complete; execution deferred.** All four sub-blocks (M5.1 circuit breaker, M5.2 writer full mode, M5.3 webhook router cutover, M5.4 runbook + deferrals) merged to `feat/ingestion-m5-cutover-mechanism`. The mechanism is tested at unit + integration levels (9 + 7 + 6 = 22 tests; load-bearing tests `test_writer_observations_match_inline_for_same_input` and `test_double_ingestion_safe_during_cutover` are green). Real-traffic cutover happens when customers exist + the M-Load dry run completes + M-Temporal wires the breaker's deferred Kafka readers. Operator runbook: [m5-cutover-runbook.md](m5-cutover-runbook.md).

**Discharges non-negotiables:** N1 (cutover with observation UNIQUE protecting against double-ingest; circuit breaker auto-reverts under sustained lag → no data loss during regression), N3 (per-tenant cutover flag + circuit breaker means one tenant's lag cannot affect another's flag state), N5 (webhook path becomes the Kafka path; convergence at `ingestion.raw` becomes live). **This is the milestone where N1 transitions from "design property" to "tested property of the running system."** Pre-cutover gates listed below are the proof of N1; do not weaken them.

**Outcome:** for tenants with `ingestion.kafka_path_enabled=TRUE`, the webhook router writes to Kafka and returns 202; the inline `ingest()` is NOT called. The writer pool becomes the sole observation writer. The circuit breaker monitors lag and auto-flips the flag back on sustained breach. **Cutover scope is slack + github only**; discord webhooks remain on inline regardless of flag (see [m5-cutover-runbook.md §2](m5-cutover-runbook.md) and [05-lld-amendments.md](05-lld-amendments.md) A7 for the deferral rationale); gmail enters via Pub/Sub and lands under the flag in M6.

**Pre-cutover gate (all must be true):**
1. M1-M4 complete and stable for ≥1 week in production.
2. Shadow-path observation counts (M2) match inline observation counts within 0.01% for ≥48 hours of sustained traffic.
3. Circuit breaker tested in staging: synthetic lag injected, flag flips, traffic reverts inline within 5 min.
4. Runbook `ops/runbooks/ingestion-cutover.md` reviewed and signed off.
5. Diagnostic queries (Block 2 + new ones) results in hand or explicit acknowledgment that proceeding without them is acceptable.
6. Product call on WS-latency tolerance answered → Mode A vs Mode A+B decision made (see §6 Q4).
7. **`services/ingestion/tests/test_ingest_core.py` is green in CI** — **Resolved** on branch `fix/test-ingest-core-ci`. The 15 FK-violation failures + 1 fixture-setup error were fixed by seeding the `tenants` row in the `tenant_id` fixture at [services/ingestion/tests/conftest.py:185-217](../../services/ingestion/tests/conftest.py#L185-L217) (commit `5ea5dc9`). A new CI workflow at [`.github/workflows/ingestion-tests.yml`](../../.github/workflows/ingestion-tests.yml) (commit `913572e` + scope narrowing in `bbf3031`) runs the suite on every push and PR to `integration/ingestion-hardening` and `main` under a non-superuser `fyralis_test` role (LOGIN, no SUPERUSER, no BYPASSRLS), so the project's RLS policies fire under test. First verification CI run: [26021692539](https://github.com/Fyralisinc/fyraliscore/actions/runs/26021692539) — 33 passed + 1 skipped (`test_real_ollama_embedding_stored` skips when `OLLAMA_URL` is unset, by design). Free win surfaced: `test_rls_policy_isolates_by_tenant` now PASSES in CI under `fyralis_test` (was previously only runnable manually). Original gate framing preserved: the shadow comparison measures count parity; count parity is not correctness parity; the legacy baseline now has the verified behavioural coverage the gate requires.
8. **Discord Gateway save-state ordering is durable against the broker-ack window** — **Resolved** on branch `fix/a6-broker-ack-ordering` (commit `269ce65` Phase 2 + `08c3b1f` Phase 3 + `4ddaf7f` Phase 3 follow-up). Option 1 chosen — per-frame `pre_save_flush(producer, timeout_seconds=2.0)` between the dispatch handler and the save-task creation. On flush failure (broad-scope: any Exception), the metric `discord_gateway_pre_save_flush_failures_total` increments, a warning is logged, and the save is skipped — the next worker re-processes the frame on RESUME under M2 dedup. The gateway worker's save-state is now durable against broker-not-yet-acked frames; verified by `test_no_frames_lost_across_sigkill` running against the extracted production function (no test-level workaround — the subprocess simulation imports the same `pre_save_flush` from [`services/integrations/discord/gateway/_durability.py`](../../services/integrations/discord/gateway/_durability.py) that production uses). See [`05-lld-amendments.md` §A6](05-lld-amendments.md), [`docs/decisions/a6-resolution.md`](../decisions/a6-resolution.md), and the operator runbook at [`docs/ingestion/m4-gateway-runbook.md`](m4-gateway-runbook.md). Original finding context preserved: M4 inherited the pre-M4 produce-return-on-local-enqueue gap; M5 made it operationally relevant by removing the inline fallback; this condition's resolution closes the gap before that cutover.

**Changes (delivered):**
- [services/ingestion/feature_flags/client.py](../../services/ingestion/feature_flags/client.py) + [circuit_breaker.py](../../services/ingestion/feature_flags/circuit_breaker.py) — full implementation per LLD §11.1 + §11.2. Per the M5.1 Phase 0 finding the breaker ships as an asyncio service (not a Temporal Schedule); M-Temporal will port it. Production Kafka readers raise `NotImplementedError` until M-Temporal injects real implementations (intentional fail-loud; see [05-lld-amendments.md](05-lld-amendments.md) A9).
- [services/ingestion/feature_flags/traffic_signal.py](../../services/ingestion/feature_flags/traffic_signal.py) — 1% deterministic-hash producer; wired into the webhook router at M5.3. FetchPage activity wiring deferred to M6 per LLD §11.3.
- [services/webhooks/router.py](../../services/webhooks/router.py) — flag-branched: if `ingestion.kafka_path_enabled=TRUE` AND provider ∈ `_CUTOVER_ENABLED_PROVIDERS` (slack, github), skip inline `ingest()`, return 202 after Kafka publish. Graceful Kafka-failure fallback to inline + bumps `webhook_router_kafka_path_total{outcome="fallback"}`.
- [services/ingestion/writers/observation_writer.py](../../services/ingestion/writers/observation_writer.py) (full mode) — flip from no-op to writing observations via `ingest_from_draft` (per-envelope; Finding 4 — see [05-lld-amendments.md](05-lld-amendments.md) A10 for Mode B collapse rationale).
- Cutover plan: tier 1 (internal Fyralis test tenant) → tier 2 (volunteer customer) → tier 3 (10% of customers) → tier 4 (50%) → tier 5 (100%). Each tier flip is per-tenant via the `tenant_flags` table. Operator procedure: [m5-cutover-runbook.md §3](m5-cutover-runbook.md).

**Tests that pass:**
- `test_writer_observations_match_inline_for_same_input` ([test_observation_writer_m5.py](../../services/ingestion/writers/tests/test_observation_writer_m5.py)) — load-bearing N1 cutover-safety parity. Inline and writer paths produce structurally-equivalent observations (kind, source_channel, source_actor_ref, trust_tier, embedding, content fields).
- `test_writer_full_mode_dedupes_on_redelivery` — Kafka redelivery → `ingest_from_draft` returns deduped=True; no duplicate row.
- `test_breaker_trips_on_sustained_lag` + `test_breaker_state_survives_restart` ([test_circuit_breaker.py](../../services/ingestion/feature_flags/tests/test_circuit_breaker.py)) — 5-consecutive-tick threshold; state survives SIGTERM via subprocess test.
- `test_breaker_does_not_auto_recover` + `test_breaker_resets_bookkeeping_on_operator_reenable` — no-auto-recovery + single-step operator re-enable.
- `test_double_ingestion_safe_during_cutover` ([test_router_m5_cutover.py](../../services/webhooks/tests/test_router_m5_cutover.py)) — load-bearing N1-during-cutover: same logical webhook via inline (flag=FALSE) AND Kafka path (flag=TRUE flipped between requests); `count(*) == 1` after both paths + writer simulation.
- `test_cutover_kafka_failure_falls_back_to_inline` — graceful degradation: customer 200/201, fallback metric incremented.
- `test_flag_cache_ttl_governs_cutover_window` — explicit `time.monotonic` control (no `asyncio.sleep`); 30s TTL bounds propagation latency.

**Tests deferred (staging / real-traffic dependent):**
- Synthetic 5-min-lag staging trip test — M-Load.
- `test_runbook_rollback_scenario_a_clean` (global rollback at scale) — M-Load.
- `test_runbook_rollback_scenario_b_per_tenant` (per-tenant rollback at scale) — covered structurally by `test_breaker_per_tenant_isolation`; production-volume version pending real customers.

**Risk if deferred:** the entire backfill story (M6) depends on the cutover. Cannot ship backfill without the steady-state path being trustworthy. **Risk of code-without-execution (the current state):** until M-Load runs, the mechanism's behaviour under realistic traffic is unverified; M-Load is the gate condition for the first real cutover.

**Risk of running out of order:** running M5 before M2's shadow-comparison has burned in is the single most dangerous sequencing mistake in this plan. The shadow comparison is the only mechanism that catches subtle handler-pipeline divergences before the writer becomes the sole source of truth.

---

### M-Temporal — Temporal infrastructure (DEFERRED INDEFINITELY)

**Status (2026-05-18): DEFERRED INDEFINITELY.** M6 ships as asyncio services following the M3.3 cursor-persistence pattern; the Temporal port becomes a future migration rather than a prerequisite. The pattern-alignment requirements documented in M6.0 below make a later Temporal port mechanical rather than a rewrite.

**Trigger conditions for revisiting** (per [05-lld-amendments.md](05-lld-amendments.md) A11) — any ONE of these is grounds for re-opening this work-unit:

1. **First crash-recovery failure** — an asyncio service crashes and the state-in-Postgres reconstruction fails to restore correctly, OR a SIGTERM-restart loses work that Temporal's history-as-source-of-truth would have preserved.
2. **First significant operator-tooling friction** — an incident where debugging the orchestration takes substantially longer than it would have with Temporal's workflow history + replay tooling. "Substantially" = >2× the time of a comparable Temporal investigation.
3. **First multi-day debugging session** — an investigation that consumes >2 working days where the bisected root cause is "asyncio service had no introspectable history of its decisions." Single instance — not a pattern of three.

Any of these flips the cost-benefit calculation: today the asyncio shape is cheaper to operate (one less infra component, one less SDK to learn); the trigger conditions are when that flips.

**Why deferred (rationale):** M5.1's Phase 0 audit found Temporal infrastructure absent. Standing it up adds operational surface (cluster ops, SDK learning curve, deployment story) for benefits that are currently theoretical (no production traffic exists; no incident has surfaced where Temporal's replay would have shortened recovery). M3.3's cursor-style asyncio pattern demonstrated that orchestration without Temporal is viable when state lives in Postgres and the service is single-purpose. M6 will use the same pattern across more orchestration surfaces.

**What does NOT block on this deferral:**
- Production execution of M5 cutover (gated on M-Load, not Temporal).
- M6 backfill rollout (gated on M-Load, the asyncio orchestration framework in M6.0, and the M6 per-source sub-blocks).
- Circuit breaker functionality (already ships as an asyncio service that meets the pattern-alignment requirements).

**What this work-unit would deliver if re-opened** (preserved for reference): Temporal Cloud account or self-hosted cluster; `services/ingestion/temporal/` package with worker registration; port of the asyncio orchestration services (circuit breaker, OAuth poller, TenantOnboarding, SourceOnboarding, ShardFetch, FeelsOnboardedMonitor) to Temporal workflows + activities; the production Kafka readers and partition fix (now part of M-Load — see below).

---

### M-Load — Synthetic-traffic cutover validation + breaker readers

**Status (2026-05-18): planned; the gate condition for the first real-customer cutover.**

With M-Temporal deferred indefinitely, M-Load absorbs the production Kafka readers and partition-correlation fix that were originally in M-Temporal's scope. Those changes don't require Temporal — they're independent infrastructure that the breaker's asyncio service can consume directly.

**Discharges non-negotiables:** N1 (verifying that the cutover's correctness properties hold at production-equivalent volume — the M5 code passes the unit/integration gates but production-volume regression is unknown).

**Outcome:** (a) the breaker's deferred Kafka readers and partition stand-in are replaced with production implementations; (b) a staging dry run with a synthetic-traffic generator drives the cutover-enabled providers (slack, github) at production-equivalent rate for ≥1 hour and validates the four cutover properties.

**Changes — breaker production readers (from former M-Temporal scope):**
- `services/ingestion/feature_flags/circuit_breaker.py::_measure_kafka_lag_default` — implement via `confluent_kafka.AdminClient.list_consumer_group_offsets` + broker-timestamp correlation, OR Burrow integration if operationally cheaper. Today raises `NotImplementedError` ([05-lld-amendments.md](05-lld-amendments.md) A9).
- `services/ingestion/feature_flags/circuit_breaker.py::_sample_active_tenants_default` — implement via a consumer-group reading the last `signal_lookback_sec` of `ingestion.tenant_traffic_signal`, returning `{tenant_id: partition}`. Today raises `NotImplementedError`.
- `services/webhooks/router.py::_kafka_partition_for_tenant` — replace blake2b stand-in with either (a) `IdempotentProducer.produce` augmented to accept an on-delivery callback that records partition into a tenant→partition table the signal hook reads from, or (b) explicit partition selection using `mmh3` murmur2 matching librdkafka's algorithm. See [05-lld-amendments.md](05-lld-amendments.md) A8.

**Changes — synthetic traffic generator + dry run:**
- `services/synthetic/cutover_load.py` — synthetic-traffic generator that signs and posts to `/webhooks/{slack,github}/*` at configurable QPS, with a controllable mix of duplicate / unique payloads.
- `tests/load/test_cutover_dryrun.py` — orchestrates the run + asserts the four cutover properties.
- `docs/ingestion/m-load-runbook.md` — operator guide for running and interpreting the dry run.

**Tests that must pass:**
- `test_breaker_real_lag_reader_detects_synthetic_breach`: inject 60s+ lag on a real partition; assert `_measure_kafka_lag_default` returns the lag within ±5s of the true value.
- `test_kafka_partition_lookup_matches_actual_landing_partition`: produce N keyed messages; assert the predicted partition equals the on-delivery partition for ≥99% of records.
- `test_signal_topic_round_trip`: publish 1000 signal records; consume via `_sample_active_tenants_default`; assert tenant→partition map round-trips correctly.
- Cutover dry-run properties: (a) `webhook_router_kafka_path_total{outcome="success"}` >> `outcome="fallback"`; (b) writer observation count equals the synthetic generator's send count (modulo intentional dedup); (c) p95 webhook-to-observation latency under target; (d) circuit breaker fires only when synthetic lag is injected, never spuriously.

**Risk if deferred:** the first real-customer cutover happens without production-volume verification AND without functional breaker lag-measurement. Code-level tests prove the mechanism is correct on toy inputs; M-Load is the gate that proves it under realistic load with real Kafka readers.

**Risk of running out of order:** M-Load needs to run AFTER M5 (the cutover code) and BEFORE M6 (which depends on the breaker actually functioning in production to enforce N3). It does NOT need Temporal — that decoupling is the rationale for M-Temporal's deferral.

---

### M6 — Backfill rollout per source (asyncio services; Temporal-aligned)

**Status (2026-05-18): seven sub-blocks (M6.0 through M6.6) implementing the LLD §2 workflows as long-running asyncio services rather than Temporal workflows.** This is a deliberate consequence of M-Temporal's indefinite deferral; the pattern-alignment requirements below ensure that a later Temporal port (under the trigger conditions in [05-lld-amendments.md](05-lld-amendments.md) A11) is mechanical rather than a rewrite.

**Discharges non-negotiables:** N1 (cursor-data ordering invariant becomes a tested property; `test_advance_cursor_atomic_with_kafka_publish` is the gate), N4 (`feels_onboarded` content-based event becomes a user-facing reality; recency-first planning materializes), N3 (per-source planner + per-tenant rate buckets enforce isolation under backfill load).

**Outcome:** new installs trigger `TenantOnboarding`; backfill runs to completion; reconciliation closes coverage gaps; `feels_onboarded` events fire; existing tenants get an opt-in "backfill now" admin action. Rollout per source in order: Gmail → GitHub → Slack → Discord.

**Pre-M6 gate:** M-Load complete (synthetic dry run passes; breaker production readers green); M5 cutover stable for ≥2 weeks for the slack+github tier ramp; circuit breaker has NOT auto-fired for any production tenant in that window.

#### Pattern-alignment requirements (load-bearing for the seven sub-blocks)

Every M6 asyncio service MUST honour these five requirements. They are derived from the trigger conditions in [05-lld-amendments.md](05-lld-amendments.md) A11 — when one of those trigger conditions fires and the Temporal port is reopened, alignment with these requirements makes the port mechanical (the asyncio main loop is replaced with a Temporal workflow body; the named functions become activities; the state schema is unchanged).

1. **Orchestration separated from side effects.** The main loop reads state from Postgres, decides what to do next, and calls a named side-effect function. Decisions live in pure functions of the state row; side effects (API calls, Kafka publishes, DB writes outside the orchestration's own state table) live in separately-named functions that take their inputs explicitly. The boundary makes the eventual Temporal split (workflow code vs activity code) one refactor pass, not a structural rewrite.

2. **State in Postgres, not memory.** Every progress-bearing variable lives in a Postgres table (`onboarding_runs`, `onboarding_shards`, `circuit_breaker_state`, `embedding_backlog_state`, etc.). The asyncio process holds NO state that wouldn't survive a SIGTERM-restart. Recovery on startup is "load the latest state row from Postgres" — no in-memory caches that need warming, no per-process counters that diverge from the durable record.

3. **Retry logic in named functions.** When a side-effect call fails and needs retrying, the retry policy lives in a function with a name (e.g. `retry_with_backoff_on_429`), NOT inline `try/except` blocks scattered through the orchestrator. Temporal's retry policies are declarative; named retry helpers map to those declarations 1:1 when porting.

4. **Signals via Postgres polling.** Cross-service communication uses Postgres rows as the signal channel, not in-process events or shared queues. The OAuth outbox poller polls a table; the `TenantOnboarding` service polls another table; the `FeelsOnboardedMonitor` polls observation counts. Polling intervals are short enough for operator UX (≤30s) but long enough not to thrash the DB. Temporal's `signal_workflow` API maps to "insert a row in the signal table" — same shape, different transport.

5. **No cross-workflow shared in-process state.** Each asyncio service is one process per logical workflow (or a small fleet, partitioned by tenant). Services do NOT share Python globals, in-process queues, or singleton objects. The circuit breaker doesn't reach into the writer's metrics dict; the OAuth poller doesn't share state with the TenantOnboarding service. All cross-service handoffs go through Postgres or Kafka. This is what makes per-workflow Temporal porting independent — one service at a time, in any order.

**Gate test for the pattern-alignment requirements:** `test_asyncio_orchestration_matches_temporal_shape` runs a static analyzer over `services/ingestion/workflows/` and asserts: (a) no module-level mutable state, (b) every `time.sleep` / `asyncio.sleep` longer than the polling interval is preceded by a Postgres state-persist call, (c) every external-API call is wrapped by a named retry helper from `services/ingestion/workflows/retry/`. Specifics are calibrated as M6.0 lands.

#### M6.0 — Asyncio orchestration substrate

Lays the framework all subsequent M6 sub-blocks build on. No business logic; just the pattern.

- `services/ingestion/workflows/__init__.py` — package root with the substrate.
- `services/ingestion/workflows/state.py` — base helpers for "load state row by id," "persist updated state row," "advance cursor atomically with Kafka publish" (the LLD §3.1 cursor-data ordering invariant).
- `services/ingestion/workflows/retry.py` — named retry helpers: `retry_with_backoff_on_429`, `retry_with_jitter_on_5xx`, etc.
- `services/ingestion/workflows/signals.py` — Postgres-table-based signal polling (`poll_signal_table(table, predicate, interval_sec=5)` returns an async iterator).
- `services/ingestion/workflows/runtime.py` — the long-running-service skeleton mirroring M3.3's embedding backlog drainer: `signal.SIGTERM` handler, max_ticks for tests, `pgbouncer_compatible=True` pool helper (sixth `statement_cache_size=0` activation).
- `services/ingestion/progress/publisher.py` + `events.py` — Pydantic event models per LLD §6.
- `services/ingestion/workflows/feels_onboarded_monitor.py` — asyncio service that polls observation counts per tenant per source; emits `feels_onboarded` events when the LLD §6 thresholds are met.
- Tests: `test_asyncio_orchestration_matches_temporal_shape` (the pattern-alignment gate), `test_signal_polling_resumes_after_restart`, `test_named_retry_logs_per_attempt`, `test_feels_onboarded_monitor_fires_at_threshold`.

#### M6.1 — OAuth outbox poller + TenantOnboarding

- `services/ingestion/workflows/oauth_poller.py` — long-running asyncio service that polls `onboarding_triggers` (the OAuth outbox table from M1) under `FOR UPDATE SKIP LOCKED`; consumes claimed rows; spawns per-tenant onboarding work by writing to `onboarding_runs`.
- `services/ingestion/workflows/tenant_onboarding.py` — long-running asyncio service that picks up `onboarding_runs` rows where `status='pending'`, fans out per-source onboarding (writes per-source rows to `source_onboarding_runs` or similar), and waits via Postgres polling for completion signals from the source workflows.
- Tests: `test_outbox_poller_consumes_under_for_update_skip_locked`, `test_tenant_onboarding_resumes_after_restart`, `test_tenant_onboarding_fans_out_to_all_sources`.

#### M6.2 — SourceOnboarding + ShardFetch + reconciliation framework

- `services/ingestion/workflows/source_onboarding.py` — per-source planner orchestration: calls `plan_shards_<source>`, persists shard rows, polls shard completion.
- `services/ingestion/workflows/shard_fetch.py` — per-shard fetcher loop: cursor advance atomic with Kafka publish (the N1 invariant — `test_advance_cursor_atomic_with_kafka_publish` is the gate); uses the named retry helpers from M6.0; rate-limits via the M1.3 Lua bucket.
- `services/ingestion/workflows/reconciler.py` — reconciliation orchestration: detects coverage gaps via per-source count APIs (the per-source modules implement those in M6.3–M6.6); decides whether to re-share a shard; persists decision in `onboarding_shards`.
- Tests: `test_shard_fetch_resumes_after_restart_from_persisted_cursor`, `test_advance_cursor_atomic_with_kafka_publish`, `test_reconciler_detects_gap_and_reshares_shard`.

#### M6.3 — Gmail backfill (planner + fetcher + reconciler)

- `services/ingestion/planners/gmail.py` — `plan_shards_gmail`.
- `services/ingestion/fetchers/gmail.py` — `fetch_page_gmail`. Subsumes the existing `gmail/fetcher.py` + `history_poller.py` + `watch_scheduler.py` — the LLD names this as the most disruptive single change; allocate L effort.
- `services/ingestion/reconciler/gmail.py` — `reconcile_gmail`.
- Tests: `test_planner_gmail_produces_expected_shards`, `test_fetch_page_gmail_advances_cursor_atomically`, `test_reconciler_gmail_detects_below_threshold_no_reshare`, `test_e2e_gmail_install_to_first_observation`.

#### M6.4 — GitHub backfill

- `services/ingestion/planners/github.py`, `fetchers/github.py`, `reconciler/github.py`. Reuses existing `GithubClient` token-cache and chokepoint logic.
- Tests: `test_planner_github_produces_expected_shards`, `test_fetch_page_github_advances_cursor_atomically`, `test_reconciler_github_above_threshold_reshares`, `test_e2e_github_full_backfill_5_repos`.

#### M6.5 — Slack backfill

- `services/ingestion/planners/slack.py`, `fetchers/slack.py`, `reconciler/slack.py`.
- Tests: `test_planner_slack_produces_expected_shards`, `test_fetch_page_slack_advances_cursor_atomically`, `test_e2e_slack_install_feels_onboarded_within_target`.

#### M6.6 — Discord backfill (sparse sampling)

- `services/ingestion/planners/discord.py`, `fetchers/discord.py`, `reconciler/discord.py` (5% sparse sampling per LLD §3.4).
- Tests: `test_planner_discord_produces_expected_shards`, `test_reconciler_discord_sparse_sampling_correctness`, `test_e2e_discord_gateway_message_to_observation`.

#### M6 — common artifacts

- `test_oauth_outbox_to_workflow_end_to_end`: simulate OAuth callback; assert outbox row written, poller consumes within 5s, TenantOnboarding starts, per-source services pick up the work, observations land.
- `test_end_to_end_small_tenant_backfill`: fixture tenant with 5 channels/repos/mailboxes; full backfill; assert coverage 100%.
- `test_oauth_outbox_to_workflow_end_to_end` is the integration gate across M6.1–M6.6.

**Risk if deferred:** the headline product gap (no backfill) persists.

**Risk of running out of order:** M6 before M5 means backfill writes go to a path that hasn't been validated against the steady-state path; divergences become bugs at install time. M6.3–M6.6 (per-source sub-blocks) MUST land after M6.0–M6.2 (the substrate) — the pattern-alignment requirements are enforced at the framework level.

---

### M7 (post-cutover refinements; not blocking the v1 cutover)

These are deferred but tracked here so they don't get lost.

- **Gmail unified canonicalize-in-writer-txn** (LLD §5.6) — ships behind `gmail.unified_canonicalization_enabled` flag; enable per-tenant after observed correctness matches.
- **Rate-limit-without-blocking-activity-slot** (LLD §3.1 future refinement) — change FetchPage step 1 to raise `RateLimited(retry_after_ms)` and let Temporal's retry policy reschedule.
- **`embedding_pending=TRUE` column deprecation** — replace with NULL check on `embedding` (LLD open Q4); deferred until embedding worker has been the sole writer for ≥1 month.
- **Per-tenant task queues opt-in** (LLD §2.4 / HLD edit 7) — add `tenants.task_queue_isolation_enabled BOOLEAN`; activate per-tenant for premium tier.
- **Mode B writer code deletion** — if the product call says "1-5s is fine for everyone," remove Mode B after one release cycle.

---

## 3. Critical path (must do first)

Five changes that must land before *anything* in §2's milestones can start. These are not milestones themselves; they are prerequisites.

1. **PgBouncer + `statement_cache_size=0`.** The writer pool's connection math is unsurvivable without pgbouncer. Asyncpg's default prepared statement cache is incompatible with transaction-mode pgbouncer; the codebase change is one line in `lib/shared/db.py` plus a sidecar/managed-service decision. Without this, M1 cannot ship: every other database-touching component assumes it.

2. **`entity_aliases_normalized_idx` functional index.** The batched alias lookup in the writer (LLD §5.2) is the source of the "~54 → ~7 queries per observation" claim. Without the functional index, batching makes large-tenant write latency worse than the current per-phrase pattern. Ship as migration 0049 inside M1; verify via EXPLAIN test before M5 cutover.

3. **New schemas (0045-0050).** Migrations are cheap to write but have a hidden ordering constraint: `onboarding_shards` references `onboarding_runs` which references `tenants`; `gateway_session_state` is standalone; `tenant_flags` references `tenants`. Apply in numerical order; the migration runner does this by convention but verify on staging.

4. **OAuth outbox + poller.** No workflow runs without a trigger. The OAuth callback changes (LLD §1.4.1) are the only mechanism that exists in this design for starting workflows from a user action. Without this, every other workflow piece is unreachable. The OAuth-callback transactional refactor is the work; the schema is trivial.

5. **Temporal cluster (Cloud or self-hosted decision).** Without Temporal, no workflows run. The decision (§6 Q2) is more about ops cost than functionality; resolve early so M1's worker registration code can be tested end-to-end against the real cluster.

**Order of critical-path delivery:** (3) and (5) are independent and can be done in parallel; (1) and (2) are also independent. (4) depends on (3) and (5). Critical-path duration ≈ max(M, M) ≈ ~2 weeks if no surprises.

---

## 4. Deliberate deferrals

Things that look like they belong in this plan but explicitly do NOT. Reasons attached so a future reader doesn't re-introduce them.

- **Multi-region active-active.** Single-region for v1; cross-region requires Temporal namespace federation, Kafka MirrorMaker, S3 cross-region replication. Each is a multi-quarter effort that buys nothing until a customer has a data-residency contract. Phase 5+.
- **Confluent Schema Registry / Avro / Protobuf.** Pydantic v2 + JSON in Kafka is sufficient. A registry adds a service to operate without solving a problem we have. Revisit if topics are ever consumed by code outside this monorepo.
- **Per-tenant Kafka clusters.** Partition affinity provides per-tenant isolation. Per-tenant clusters add operational complexity proportional to customer count for no isolation benefit beyond what we have.
- **Per-tenant Temporal namespaces.** Workflow IDs include `tenant_id`; Temporal serializes per-workflow-id and isolates per-workflow-history. Namespaces are for cluster-level tenancy (e.g., white-label Temporal access); we don't sell that.
- **Custom backpressure protocol.** Kafka consumer-group lag is the signal; the circuit breaker (LLD §11.2) is the response. No application-layer flow control.
- **Multi-shard Discord Gateway.** Single shard suffices below ~2,500 guilds per Discord's sharding rules. Implementing sharding now is YAGNI; defer until the per-shard guild count crosses the threshold.
- **Slack edits/deletes/reactions ingestion.** The current handler accepts `message` events with `text` only; backfill design depends on the handler shape, which is preserved. Adding event types is an orthogonal workstream (new `_EVENT_SHAPERS` entries, no substrate change).
- **GitHub event types beyond the existing six.** Same reasoning as Slack: orthogonal.
- **Gmail unified-canonicalization refactor.** Deferred to post-cutover M7. The interim three-transaction shape (current code) works; bundling the refactor with the cutover would obscure root-cause attribution if either breaks.
- **Embedding worker via `embedding_pending=TRUE` polling instead of Kafka.** Considered and rejected: Kafka topic is the steady-state signal for new work; the polling script (LLD §12.1) handles only the pre-cutover backlog. Two mechanisms for two distinct populations.
- **Rate limiter migration to a service mesh-level component** (e.g., Envoy filters). The Redis Lua bucket is fast, observable, and Python-side. Service-mesh integration is a deployment-architecture conversation, not a v1 design call.
- **Replacing the existing `ingest()` core function.** It's correct; preserve it. The writer wraps it (batched) rather than rewriting it.

---

## 5. Test strategy

Test categories with example names. The full test list is large; this is the architecture, not the catalog.

### 5.1 Unit tests

Per-module, fast (<100ms each), no external deps.

- `test_normalize_phrase_idempotent`
- `test_envelope_pydantic_validation`
- `test_rate_limiter_lua_acquire_token_math` (via in-process Lua interpreter or testcontainers Redis)
- `test_idempotency_constructor_<source>_matches_handler`
- `test_observation_writer_group_by_tenant`
- `test_shard_recency_score_decay`
- `test_feature_flag_cache_ttl_invalidation`

### 5.2 Integration tests

Per-component, with real dependencies (Postgres, Redis, Kafka, S3). Marked `@pytest.mark.integration`.

- `test_pool_pgbouncer_compatibility` (M1 gate)
- `test_kafka_idempotent_producer_dedup` (M1 gate)
- `test_s3_put_if_absent_returns_412_on_duplicate` (M1 gate)
- `test_outbox_poller_consumes_under_for_update_skip_locked` (M1 gate)
- `test_observation_writer_batched_insert_preserves_dedup` (M5 gate)
- `test_circuit_breaker_flips_flag_under_sustained_lag` (M5 gate)
- `test_planner_<source>_against_mocked_api` (M6 per-source gate)

### 5.3 Idempotency replay tests

Run same input through the pipeline twice; assert zero duplicate observations.

- `test_replay_same_webhook_produces_one_observation` (M2 + M5)
- `test_replay_same_backfill_shard_produces_no_duplicates` (M6 per-source)
- `test_replay_dlq_recovery_idempotent` (M5)
- `test_replay_gmail_thread_canonicalization_idempotent` (M7 unified-txn shape)

### 5.4 Cursor recovery tests

Kill a worker mid-fetch; assert resume from correct cursor.

- `test_fetch_page_<source>_resumes_after_worker_kill`
- `test_advance_cursor_atomic_with_kafka_publish` — assert publish before advance is the actual order
- `test_workflow_heartbeat_timeout_triggers_retry`

### 5.5 Rate-limit honoring tests

Mock 429 responses; assert backoff.

- `test_<source>_429_with_retry_after_sleeps_then_retries`
- `test_lua_lockout_overrides_token_math_during_window`
- `test_rate_limiter_under_concurrent_acquires_serializes`

### 5.6 Reconciliation tests

Inject a gap; assert detection and re-shard.

- `test_reconcile_<source>_detects_below_threshold_gap_no_reshare`
- `test_reconcile_<source>_detects_above_threshold_gap_reshares`
- `test_reconcile_discord_sparse_sampling_correctness` — assert sampling distribution
- `test_reconcile_two_passes_then_status_partial` — assert workflow completes with `status='partial'` and `coverage_confidence` reflects it

### 5.7 End-to-end small-tenant tests

Fixture data; full pipeline; assert coverage = 100%.

- `test_e2e_gmail_install_to_first_observation`
- `test_e2e_github_full_backfill_5_repos`
- `test_e2e_slack_install_feels_onboarded_within_target`
- `test_e2e_discord_gateway_message_to_observation`
- `test_e2e_oauth_outbox_to_workflow_to_writer`

### 5.8 Cutover-specific tests (M5)

- `test_shadow_path_observation_count_matches_inline` — the M5 gating test; run for 48h before cutover.
- `test_runbook_rollback_scenario_<a/b/c/d>`
- `test_double_ingestion_safe_during_cutover_window`

### 5.9 Workflow replay tests (Temporal time-skipping framework)

Per LLD §2.4 Bug 4 fix — these are required to assert determinism.

- `test_source_workflow_replays_deterministically_with_asyncio_primitives`
- `test_shard_workflow_replay_after_seven_day_pause`
- `test_monitor_workflow_no_history_bloat_over_long_runs`

### 5.10 Recovery script tests

- `test_embedding_backlog_idempotent_safe_with_concurrent_worker`
- `test_gmail_case_a_recovery_no_op_on_already_provisioned`
- `test_gmail_case_b_recovery_does_NOT_reset_active_watches` — the latent-bug catch from Phase 2.1 Q5; must explicitly assert active watches retain their history_id
- `test_thread_canonical_id_scanner_idempotent`

### 5.11 Performance / load tests

Not blocking individual milestones; gates the M5 → M6 transition.

- `test_steady_state_p95_latency_at_1k_webhooks_per_minute`
- `test_writer_throughput_at_default_batch_size`
- `test_normalizer_pool_lag_under_burst`

---

## 6. Open questions / decisions needed

The final list before M1 starts. Each requires a named owner and a target date.

### Q1 — PgBouncer deployment mode

**Decision needed:** sidecar per pod vs centralised service.
**Owner:** Infra / SRE.
**Default if undecided:** sidecar (lower latency, less SPOF risk).
**Blocks:** M1.

### Q2 — Temporal Cloud vs self-hosted

**Decision needed:** Cloud (faster start, per-action billing) vs self-hosted (~$1.5k/mo infra + sustained SRE burden).
**Owner:** Engineering leadership.
**Recommended:** Cloud for v1 (LLD §11.2 already prefers it).
**Blocks:** M1.

### Q3 — Diagnostic queries (carried forward from Block 2 + Phase 3)

Four queries blocked on staging DB access:
1. Embedding backlog count (Block 2 corrected query, LLD §12.1).
2. Gmail Case A orphans (Block 2 corrected query, LLD §12.2).
3. Gmail Case B partial-provisioning detection (new from Phase 3 Q5, LLD §12.3).
4. Gmail NULL-`thread_canonical_id` rows (new from Phase 3 Q2, LLD §12.4).

**Owner:** whoever has staging psql access.
**Blocks:** sizing M3 (embedding backlog scope) and the M5 prereq (M5 gate condition #5 says results in hand OR explicit acknowledgment that proceeding without them is acceptable). Does not block M1 or M2.

### Q4 — WS dashboard latency tolerance (Phase 2.1 Q4)

**Decision needed:** is the cutover's regression from ~100ms inline to ~1-5s end-to-end acceptable for the WS-pushed dashboard at [services/realtime/dispatcher.py](services/realtime/dispatcher.py)?
- If **YES**: ship Mode A writer only; delete Mode B code after one release cycle (M7).
- If **NO**: ship Mode A + Mode B dual-mode writer; flip Mode B for WS-sensitive tenants via the per-tenant flag.

**Owner:** Product.
**Blocks:** M5 cutover decision; LLD §5.3 already specifies the dual-mode default; no architecture change either way.

### Q5 — SDK sandbox URL verification (LLD Bug 4, Phase 3 carry-forward)

**Decision needed:** before LLD §2.4's `asyncio.Semaphore`/`create_task`/`gather` pattern ships, hit the live Temporal Python SDK docs and confirm the cited section names are current.
**Owner:** the engineer implementing M1's workflow skeleton.
**Blocks:** nothing structurally; if the docs say otherwise, swap to Temporal-native primitives (`workflow.wait_condition`, etc.). Spike effort: <1 hour.

### Q6 — Kafka topic partition counts

**Decision needed:** the LLD picks 64 for `ingestion.raw` and `ingestion.normalized`, 16 for `ingestion.tenant_traffic_signal`, 16 for `onboarding.progress`. These are reasonable defaults; tuning requires measured per-source message rates.
**Owner:** SRE during M1.
**Default if undecided:** ship LLD numbers; revisit at first burst event.

### Q7 — Normalizer pool auto-scaler signal

**Decision needed:** scale out on lag > 60s; scale-in policy is "stay at peak for 1h after lag drops below 10s, then -1 pod every 15 min." Confirm or adjust.
**Owner:** SRE.
**Blocks:** M2 (no-op normalizer is one pod; M5 needs N).

### Q8 — Frontend update for Gmail response shape change

**Decision needed:** Gmail's `connect_finalize` response changes from `"provisioning": "started"` to `"provisioning": "queued"` (LLD §1.4.1 worked example). Frontend code consuming this string needs to update or accept both.
**Owner:** Frontend team.
**Blocks:** M5 (Gmail OAuth callback outbox shape ships here, since outbox is a critical-path piece).

### Q9 — Per-tenant task queue opt-in schema

**Decision needed:** add `tenants.task_queue_isolation_enabled BOOLEAN` now (anticipating premium tier) or defer until a premium tenant exists.
**Owner:** Engineering / product.
**Default if undecided:** defer (M7); current per-source default is fine for all v1 tenants.

### Q10 — Reconciliation interval for ongoing tenants

**Decision needed:** after a tenant's backfill completes, do we run reconciliation periodically (e.g., weekly) to catch silent drops? Or only on-demand?
**Owner:** Engineering.
**Default if undecided:** on-demand (operator action), not periodic. Periodic reconciliation is a Phase-5 maintenance loop, not v1.

---

**End of Phase 4.** The four-document set (`01-current-state.md`, `02-high-level-design.md`, `03-low-level-design.md`, `04-implementation-plan.md`) is now complete. M1 is the next concrete action — kick it off when critical-path Q1, Q2, and Q5 have named owners and target dates.
