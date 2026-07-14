# Checkpoint — BYOC Fleet Instrumentation (G1–G7)

**Branch:** `feat/byoc-instrumentation` (worktree `/home/prajwal-adhikari/Desktop/v2/fyralis-byoc`)
**Status:** COMPLETE and verified — merge-ready
**Date:** 2026-06-24
**Design source:** `docs/plans/byoc-control-plane.md` §12 ("Gaps to fill before building")

## Scope

This track closes the seven pre-build instrumentation gaps (G1–G7) identified in
the BYOC control-plane design §12. These gaps were flagged as blocking
*trustworthy fleet health* — i.e. they had to be closed before the vendor-owned
control plane could remotely answer "is this customer-VPC deployment healthy?"
without grepping logs or guessing.

The work is purely *fleet instrumentation on the existing Fyralis data plane*:
new Prometheus metric families on the shared default registry (so every worker
that renders `/metrics` exposes them), wired at their exact emission sites, plus
one additive schema-ledger migration and the postgres-exporter custom-query
verification. No behavioral change to ingestion, reasoning, or the writers.

## The seven gaps and what each added

### G1 — schema-version ledger + fleet metrics
*Design: §9.2 Database & schema integrity, tagged 🔴 "no `schema_migrations` table".*

- **Migration `db/migrations/0155_schema_migrations.sql`** — formal, checked-in
  definition of the ledger both runners already lazily created. Additively
  widens it with a `checksum` column (digest of the file bytes at apply time,
  for drift detection) alongside the existing filename PK (= `0NNN_` schema
  version) and `applied_at`. Idempotent (`CREATE TABLE IF NOT EXISTS` +
  `ALTER TABLE ADD COLUMN IF NOT EXISTS`) — safe on a fresh DB and on one
  already bootstrapped by the older two-column runner. Infra bookkeeping; no
  `tenant_id` / RLS (same pattern as `writer_poison_attempts` 0137 /
  `workflow_states` 0065).
- **Metrics (`lib/observability/metrics.py`):** `fyralis_schema_version` (max
  applied prefix), `fyralis_schema_applied_count` (ledger row count), and
  `fyralis_schema_last_failed_migration{filename}` (1 while a migration apply is
  wedged, cleared to 0 on a clean apply).
- **Wiring (`lib/shared/migrations.py`):** captures + records the file checksum,
  sets `fyralis_schema_last_failed` on `MigrationError` (before re-raising, so
  the production `stop` path also records the wedged file), clears it on a clean
  apply, and publishes version/applied-count from the ledger after the run.
  Extension ledgers (non-default table) skip the schema-version gauge so a
  private numbering can't clobber it.
- **`scripts/docker-migrate.sh`** — the production shell runner records the same
  checksum (`sha256sum`) and bootstraps the checksum column, keeping shell and
  Python runners in lockstep.

### G2 — OAuth source-token health
*Design: "the most common 'source silently dies' failure is invisible to fleet monitoring".*

- **Metrics:** `fyralis_oauth_refresh_failures_total{provider,reason}` +
  `fyralis_oauth_token_expires_in_seconds{provider}` (token-expiry-soon gauge).
- **Wiring (`services/ingest/integrations/oauth_refresh.py`):** mirrors every
  non-success exchange outcome onto the failures counter via
  `_record_refresh_failure` at all six failure sites (`bad_request_config` ×2,
  `transport`, `http_4xx`/`5xx`, `invalid_response` ×2). Sets the expiry gauge
  on every successful mint (= `expires_in`) and — via a new backward-compatible
  optional `provider=` kwarg on `needs_refresh()` — to the LIVE remaining
  seconds during the proactive poll sweep, so token-expiry-soon fires even for
  tokens not refreshed this tick. The local `oauth_refresh_outcomes_total` stays
  for per-deployment debugging.

### G3 — LLM circuit-breaker state + provider errors
*Design: "deepseek down → all reasoning fast-fails" was log-only.*

- **Metrics:** `fyralis_llm_circuit_breaker_state{provider,state}` (per-provider
  open/half_open/closed) + `fyralis_llm_provider_errors_total{provider,error_class}`.
- **Wiring (`services/reasoning/think/circuit_breaker.py`):** a `_publish_state()`
  helper sets the breaker gauge on EVERY transition and at breaker
  creation/register/reset, so `state=open` is directly alertable and a
  never-tripped provider still shows `closed` (distinguishing "healthy" from
  "unscraped"). Verified through the full closed→open→half_open→closed cycle.
- **Wiring (`services/reasoning/think/llm_reason.py`):** increments the provider
  errors counter for every provider error caught in the reasoning retry loop,
  labeled via the existing `classify_error()` — separating
  `rate_limit`/`permanent` (fix billing/quota) from `transient` (wait it out).

### G4 — durable think validation/cost metrics
*Design: "validation_dropped_ops, cost-by-kind lost on restart".*

- **Metrics:** `fyralis_think_validation_dropped_ops_total{reason,op_type}` +
  `fyralis_think_llm_cost_usd_total{trigger_kind}`, mirrored onto the default
  registry under distinct `fyralis_`-prefixed names (distinct names avoid a
  duplicate-series collision on the think worker, which emits both
  `render_prometheus_text()` and `render_default()`).
- **Wiring (`services/reasoning/think/observability.py`):** `log_dropped_op` and
  `record_think_run_cost` now mirror their in-process singletons onto the
  default registry at the emission site. Continuous fleet scrape retains the
  series across a restart in the central TSDB; cost is additionally durable in
  `think_run_costs` (the §12 G4 DB-backed option).

### G5 — expected-vs-running worker set
*Design: "anomaly_processor / deadline_resolver not in compose → deployment looks healthy while T2/T3 reasoning never runs".*

- **Metrics:** `fyralis_worker_expected{worker_class}` +
  `fyralis_worker_compose_present{worker_class}`, driven by
  `EXPECTED_WORKER_CLASSES` encoded IN CODE. `anomaly_processor` /
  `deadline_resolver` are flagged `present=False` (coded-but-undeployed), so a
  healthy-looking deployment exports a `0` directly. Eager-published at import,
  so the static set is on the first scrape.

### G6 — silent-data-loss signals promoted to counters
*Design: "Producer flush-undelivered & shadow-drop are log-only — these are data-loss signals".*

- **Metrics:** `fyralis_kafka_producer_shutdown_undelivered_total` +
  `fyralis_writer_shadow_drop_total{ingress_kind}`.
- **Wiring (`services/ingest/ingestion/kafka/producer.py`):**
  `IdempotentProducer.stop()` now increments the undelivered counter by
  `remaining` (the loss magnitude, not just "a stop timed out") on flush
  timeout. `stop()` is the single shutdown flush site all callers funnel
  through (tenant_onboarding / reconciler / shard_fetch / source_onboarding /
  feels_onboarded_monitor / `__main__`).
- **Wiring (`services/ingest/ingestion/writers/observation_writer.py`):** the
  shadow-path drop increments `fyralis_writer_shadow_drop_total{ingress_kind}`,
  so the control plane can alert hard on `ingress_kind=backfill` (the
  silent-data-loss invariant violation) while tolerating `live` (the inline path
  persists those when `kafka_path_enabled` is FALSE).

### G7 — postgres-exporter custom queries verified in the BYOC bundle
*Design: "DLQ/think/embedding gauges may be unpopulated in some checkouts".*

- **Finding:** the bundle config lives at
  `observability/postgres-exporter/queries.yaml`, NOT under `ops/` (which holds
  only pgadmin) — the original task hint was stale. All five design-named
  metrics ship and are correct: `fyralis_dlq_unresolved`,
  `fyralis_think_queue_pending`, `fyralis_embedding_backlog_pending`,
  `fyralis_dead_letter_rows`, and `fyralis_onboarding_shards`. Nothing was
  missing.
- **Addition:** one query (`fyralis_schema` → `_version` / `_applied`) so the G1
  schema ledger is also visible to the fleet via the DB on the production
  shell-runner path, which cannot set in-process Prometheus gauges.

## Commits (`git log origin/main..HEAD`, oldest first)

| Commit | Subject |
| --- | --- |
| `1080940` | feat(byoc): G1 schema-version ledger + fleet metrics; G7 verify exporter queries |
| `05a5f05` | feat(byoc): G6 promote producer-flush-undelivered and writer shadow-drop to counters |
| `489af12` | feat(byoc): G2/G3/G5 fleet metric singletons + encoded expected-worker set |
| `1f453e3` | feat(byoc): G2 wire OAuth refresh-failure counter + token-expiry-soon gauge |
| `db9d927` | feat(byoc): G3 export LLM breaker state + per-provider error-class counters |
| `cdee4b9` | feat(byoc): G4 mirror in-memory think validation/cost onto the default registry |
| `23d24ab` | test(byoc): add regression coverage for fleet instrumentation (G1–G6) |

(G7 was folded into the G1 commit; G2/G3/G5 singletons land in `489af12`, then
G2/G3/G4 are wired at their emission sites in the three follow-ups.)

## Verify verdicts

The track was checked on three dimensions:

- **Correctness — PASS.** Each metric family is emitted at its exact emission
  site; the G3 breaker gauge was verified through the full
  closed→open→half_open→closed cycle; G1 publishes version/applied-count from
  the ledger and flips `_last_failed_migration` on a broken file (and back to 0
  on a clean apply). G5 reads `anomaly_processor`/`deadline_resolver` as `0`
  (coded-but-undeployed) and deployed classes as `1`.
- **Conflict-safety — PASS.** Migration `0155` is additive and idempotent
  (`IF NOT EXISTS` everywhere), safe on fresh and already-bootstrapped DBs; no
  RLS/tenant change. G4 uses distinct `fyralis_`-prefixed series names to avoid
  a duplicate-series collision on the think worker (which renders two
  registries). The OAuth `needs_refresh(provider=…)` signature change is
  backward-compatible (new kwarg defaults to `None`).
- **Tests — PASS.** 22 BYOC instrumentation tests; **47 passed / 1 skipped**
  (the single skip is the DB/Docker-gated 100-webhook shadow soak, gated by
  per-test marks). Tests drive metrics at their real emission sites with no DB
  required for the pure paths — e.g. the real `apply_migrations_dir` over a
  no-DB fake connection for G1, breaker transitions for G3, the eager-published
  expected-worker lines for G5, producer `stop()` loss accounting for G6, and
  OAuth failure/expiry for G2. (The broader extended suite, including the
  pre-existing tests these extend, reports 73 passed / 1 skipped under the
  project venv.)

## Merge-readiness

**Ready to merge.** All seven §12 gaps are closed, verified on correctness,
conflict-safety, and tests, with the implementation frozen and regression
coverage added at the emission sites. The only previously-untested commits (the
first six) are now covered by `23d24ab`. The branch is 7 commits ahead of
`origin/main` with no behavioral change to the data plane.

## Out of scope — control-plane services intentionally UNBUILT

This track is *only* the fleet-instrumentation prerequisite. The rest of the
BYOC control plane described in `docs/plans/byoc-control-plane.md` is
intentionally NOT built here:

- **Auth proxy** (the #1 build — Mimir/Loki have no native auth)
- **Mimir** (central metrics TSDB / remote-write target)
- **Loki** (central log aggregation)
- **OTel collector / agent** (the customer-VPC outbound-only data-plane agent)
- **Fleet console** (the operator-facing health UI)

These remain greenfield. What this checkpoint delivers is the instrumented data
plane those services will eventually scrape and remote-write — i.e. the metrics
exist and are trustworthy before any control-plane component is wired up.
