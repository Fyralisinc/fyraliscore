# Implementation Plan: Concurrent Backfill + Live-via-Kafka Validation (50 tenants × 4 sources)

**Branch base**: `integration/ingestion-hardening` (anchor) · **Date**: 2026-05-21
**Predecessors**: M-Validate (spine, Run 1) · M-Validate-Live (composition, A30) · M5.3/M5.4 (Slack/GitHub cutover)

## Goal

A single operator-invokable validation run (**Run 4**) in which, for **all four sources**
(slack, discord, github, gmail) at **50 tenants**, the **backfill** producer chain and
**live** ingestion run **concurrently** (not sequential phases), and **live ingestion is
routed through Kafka** — i.e. live webhooks/events publish to `ingestion.raw` and are
drained by the *same* normalizer → observation_writer consumer chain as backfill, rather
than writing inline via `core.ingest()`.

This closes synthetic-validation fidelity gaps #1 (live bypasses Kafka), #2 (backfill/live
sequential, no concurrent race), and #4 (live only tested at 16 tenants), plus the A30.6
fixed-drain-window follow-up. Synthetic inputs (mock clients + fixtures) are retained — no
real-API work in this unit.

**Scope decision (confirmed):** build the production live-via-Kafka cutover for **Discord
and Gmail first**, so all four sources test uniformly via the Kafka path. Slack and GitHub
already have it (`_CUTOVER_ENABLED_PROVIDERS`, [router.py:91-94](../../services/webhooks/router.py#L91-L94)).

## Current state (grounded)

- **Slack/GitHub live-via-Kafka EXISTS.** [router.py:850-894](../../services/webhooks/router.py#L850-L894): when
  `ingestion.kafka_path_enabled=TRUE` for the resolved tenant and the provider is in
  `_CUTOVER_ENABLED_PROVIDERS`, `_attempt_kafka_path` does S3 PutIfAbsent → publish to
  `ingestion.raw` → return 202, **skipping inline `ingest()`**. Requires
  `request.app.state.kafka_producer` and `s3_raw_client` to be wired; otherwise it logs
  `cutover_deps_missing` and falls back to inline.
- **Discord Gateway already publishes to Kafka as a SHADOW** ([dispatch.py:169-234](../../services/integrations/discord/gateway/dispatch.py#L169-L234)):
  post-inline `shadow_write_raw(..., ingress_kind="gateway")`. `DispatchDeps` already
  carries `kafka_producer`/`s3_raw_client`/`tenant_flags`. The M5.4 deferral was about
  Discord *webhook interactions* needing a synchronous `CHANNEL_MESSAGE_WITH_SOURCE`
  response — the **Gateway MESSAGE_CREATE path has no such constraint**, so cutover is
  feasible here.
- **Gmail Pub/Sub is shadow-only.** The push-handler ([push_handler.py](../../services/integrations/gmail/push_handler.py))
  fetches messages then ingests inline; the Pub/Sub notification is intentionally omitted
  from channel_mapping ([channel_mapping.py:40-44](../../services/ingestion/normalizer/channel_mapping.py#L40-L44))
  because a notification is not a message resource. The *fetched message* IS a resource and
  can be published to `ingestion.raw`.
- **Backfill harness owns the consumer subprocesses + drain.** [BackfillHarness.run()](../../services/synthetic/backfill_harness/harness.py)
  spawns 7 subprocesses (incl. normalizer + observation_writer), runs the backfill, then
  drains via a **hardcoded 30s window** (A30.6; [run3_concurrency_stress.py:53-62](../../services/synthetic/validation_runs/run3_concurrency_stress.py#L53-L62)).
- **Live phase is inline + sequential.** [composition.py:23-26](../../services/synthetic/validation_runs/composition.py#L23-L26):
  "Live ingestion is INLINE … no Kafka consumer is needed." The runner does backfill →
  drain → live ([runner.py:162](../../services/synthetic/validation_runs/runner.py#L162)).
  `build_live_drivers` builds the shared app via `build_app` **without** wiring
  `kafka_producer`/`s3_raw_client` ([composition.py:188-196](../../services/synthetic/validation_runs/composition.py#L188-L196)).
- **Run 3** is 50-tenant 15/15/10/10, **backfill-only**, the structural starting point for Run 4.

## Migrations

**None.** `kafka_path_enabled` and `shadow_write_enabled` flags exist; `tenant_flags` table
exists. New `channel_mapping` rows for live ingress are **code constants**
([channel_mapping.py](../../services/ingestion/normalizer/channel_mapping.py)), not DB rows.

---

## Workstreams

### WS1 — Discord Gateway live-via-Kafka cutover (production code)

Convert the post-inline shadow-write into a **cutover** when `kafka_path_enabled=TRUE`.

- **T1.1** In [dispatch.py](../../services/integrations/discord/gateway/dispatch.py), before the
  inline `ingest()` call (line 146), read `kafka_path_enabled` (default False) via
  `deps.tenant_flags`. When TRUE and shadow deps are wired:
  - publish to `ingestion.raw` via `shadow_write_raw(..., ingress_kind="gateway")` (the
    existing helper, lifted out of `_maybe_shadow_write_gateway`),
  - emit the 1% traffic signal (mirror `_attempt_kafka_path`),
  - **skip inline `ingest()`** and return,
  - on publish failure: fall back to inline `ingest()` (graceful degradation, parity with
    [router.py:875-894](../../services/webhooks/router.py#L875-L894)).
- **T1.2** Add channel_mapping `("discord", "gateway") → "discord:message"` so the
  normalizer routes the live-via-Kafka envelope to the same handler the backfill mapping
  (`("discord","backfill")→"discord:message"`) uses. Verify external_id parity (the
  fetched/gateway message must shape an identical `discord:message` payload — the dispatch
  handler conformance already does this for backfill in M6.7).
- **T1.3** Leave the Discord **webhook interactions** path (`services/webhooks/router.py`
  discord branch) untouched — it stays inline (M5.4 still applies; this WS is Gateway-only).
- **Tests** (`services/integrations/discord/gateway/tests/`): flag TRUE → inline `ingest`
  NOT called, one `ingestion.raw` envelope published with `ingress_kind="gateway"`; flag
  FALSE → inline path unchanged; publish failure → inline fallback fires once.

### WS2 — Gmail push-handler live-via-Kafka cutover (production code)

- **T2.1** In [push_handler.py](../../services/integrations/gmail/push_handler.py), at the
  per-message dispatch point, read `kafka_path_enabled` (default False). When TRUE: for each
  fetched message, `shadow_write_raw(..., source="gmail", ingress_kind="poll")` to
  `ingestion.raw` instead of inline ingest; on failure fall back to inline. Pub/Sub still
  returns 200 immediately (fetch is post-ack), so no response-shape constraint.
- **T2.2** Add channel_mapping `("gmail", "poll") → "gmail:"` (the live-fetched-message
  ingress) — distinct from the deliberately-omitted Pub/Sub *notification* ingress. Confirm
  the published envelope produces the same `external_id` (`gmail:{install}:{message_id}`) as
  backfill so the cross-path twin still dedups.
- **Tests** (`services/integrations/gmail/tests/`): flag TRUE → fetched message published to
  raw, inline NOT called; external_id parity vs backfill fetcher; flag FALSE unchanged.

### WS3 — Wire cutover deps into the validation FastAPI app (harness code)

`_attempt_kafka_path` and the Discord/Gmail cutovers need a real producer + S3 client on
`app.state` / in deps, or they silently fall back to inline.

- **T3.1** In [composition.build_live_drivers](../../services/synthetic/validation_runs/composition.py#L159):
  construct one `IdempotentProducer` (real, → `ingestion.raw`) and the moto-backed
  `s3_raw_client`; set `shared_app.state.kafka_producer` / `.s3_raw_client` /
  `.tenant_flags` for slack+github; pass the same into `DispatchDeps` (discord) and the
  gmail `_GmailDeps`.
- **T3.2** Set `kafka_path_enabled=TRUE` for **every** tenant at setup (all 4 sources), so
  live takes the Kafka path. (Backfill already sets it for full-mode writes —
  [harness.py:372](../../services/synthetic/backfill_harness/harness.py#L372).)

### WS4 — Concurrent orchestrator + configurable drain (harness code, closes A30.6)

Replace the sequential backfill→drain→live with a concurrent driver.

- **T4.1** Refactor `BackfillHarness` to separate **(a) start consumers**, **(b) run
  producer(s)**, **(c) drain** into independently callable phases (today `run()` fuses
  them). Keep `run()` as the backward-compatible composition for Runs 1–3.
- **T4.2** Add a `ConcurrentHarness` (or `run_concurrent()` mode) that: starts the consumer
  subprocesses once → `asyncio.gather(backfill_producer_drive, live_generator_drive)` so
  shard_fetch and live webhooks publish to `ingestion.raw` **simultaneously** → drains the
  shared chain **once** after both producers finish.
- **T4.3** Make the drain window a parameter (`drain_timeout_s`, `drain_stable_for_s`)
  threaded from Run 4 — closes the A30.6 hardcoded-30s limitation and unblocks higher-volume
  soak. Drain = "observation count for the run's tenants holds steady for `stable_for_s`",
  reusing the stability-poll shape from [composition.wait_for_live_consumer_drain](../../services/synthetic/validation_runs/composition.py#L676).

### WS5 — Live generators drive through the cutover (harness code)

- **T5.1** The slack/github generators already POST to the shared app; with WS3 deps wired +
  flag TRUE they now get 202 (Kafka) instead of 200 (inline) — assert the status flip in a
  generator-level test.
- **T5.2** The discord generator dispatches via `DispatchDeps`; ensure WS3 passes the
  producer/S3/flags so WS1's cutover branch activates. The gmail generator's `simulate_push`
  must reach WS2's cutover (deps on `_GmailDeps`).

### WS6 — Run 4: concurrent 50-tenant validation (new `run4_concurrent.py`)

- **T6.1** `services/synthetic/validation_runs/run4_concurrent.py`: 50 tenants (15/15/10/10),
  HAPPY_PATH, all 4 sources, backfill + live concurrent, live-via-Kafka. Per-tenant volumes
  sized with the now-configurable drain window (no longer pinned to 30s).
- **T6.2** Register `--run=4` and include it in `--run=all` ([runner.py:317-328](../../services/synthetic/validation_runs/runner.py#L317-L328)).
- **Assertions** (extend [assertions.py](../../services/synthetic/validation_runs/assertions.py)):
  1. `assert_per_tenant_isolation` — each tenant's count = backfill + live, independently.
  2. `assert_concurrency_overlap_observed` — the monitor saw live observations land **while**
     `source_onboarding_runs.status='in_progress' > 0` (proves true overlap, not sequencing).
  3. `assert_live_routed_through_kafka` — all four sources' live observations carried the
     Kafka ingress (verify via writer full-mode metric / `ingestion.raw` published counts ==
     live events, and inline-ingest counter == 0 for the run).
  4. `assert_cross_path_twins_dedup_under_concurrency` — the twin is now dispatched live
     **during** backfill drain (a real race); the `(source_channel, external_id,
     occurred_at)` index must still collapse to one row, for all sources where a twin exists
     (extend to Discord now that its live ingress maps to `discord:message` — close the
     A30.3 disjoint-namespace gap by aligning the live twin id with backfill).
  5. `assert_no_signal_leak` + `assert_completion_once_per_tenant` (carry from Run 3).
  6. `assert_dlq_empty` — happy path routes nothing to `ingestion.dlq`.

### WS7 — Reporting, docs, ticket hygiene

- **T7.1** `RunReport` for Run 4 → `docs/validation/path_i/run4_report.md`; update
  `summary.md` coverage matrix (live column becomes "via-Kafka" for all 4; mark concurrency
  tested at 50 tenants live).
- **T7.2** Append a scope note (mirror f5f7a7f) stating what Run 4 now covers and what's
  still synthetic-only (no real API — gap #6 remains open).
- **T7.3** File follow-ups / link existing: **#45** (consumer graceful-shutdown — the
  rc=-9/-15 still applies to the now-busier consumers); Discord webhook-interaction cutover
  (still deferred, M5.4) as explicitly out-of-scope here.

---

## Phase ordering

1. **WS1 + WS2** (prod cutover, independent — parallelizable) with unit tests. *Gate: both
   sources publish to raw under flag, fall back inline on failure, external_id parity holds.*
2. **WS3** (wire deps into validation app) — depends on nothing in WS1/WS2 code but is
   validated by them.
3. **WS4** (concurrent orchestrator + configurable drain) — the structural core.
4. **WS5** (generators drive cutover) — small, depends on WS3.
5. **WS6** (Run 4 + assertions) — depends on WS1–WS5.
6. **WS7** (reports/docs).

Slack/GitHub live-via-Kafka is exercised end-to-end as soon as WS3+WS4 land (no WS1/WS2
dependency), so a partial Run 4 (slack/github only) can validate the concurrent orchestrator
before Discord/Gmail cutover is finished — useful de-risking checkpoint.

## Risks

1. **Discord cutover ordering vs the interactions path.** The Gateway and webhook-interaction
   code share `dispatch`-adjacent helpers; the change must be Gateway-frame-only. *Mitigation:
   gate on the Gateway entry point; leave `_CUTOVER_ENABLED_PROVIDERS` (webhook router)
   unchanged.*
2. **Gmail fetched-message envelope shape must match backfill** or the cross-path twin won't
   dedup and the normalizer may reject. *Mitigation: reuse the M6.7 handler-conformance shape;
   T2.2 asserts external_id parity against the backfill fetcher.*
3. **Shared consumers under concurrent backfill+live load.** Live and backfill for the same
   tenant key to the same Kafka partition (`_kafka_partition_for_tenant`), so per-tenant
   ordering holds; cross-tenant interleaving is the point of the test. *Mitigation: drain is
   count-stability based, agnostic to interleave order.*
4. **Drain false-positive under concurrency** (count momentarily stable mid-flight).
   *Mitigation: require both producers reported done AND stable-for window elapsed before
   asserting; generous `drain_timeout_s`.*
5. **#45 consumer rc=-9/-15** will still show on the busier consumers; Run 4's rc policy must
   accept it exactly as Runs 1–3 do until #45 ships.
6. **Cutover dep wiring drift** — if `kafka_producer`/`s3_raw_client` aren't on `app.state`,
   cutover silently falls back to inline and the run would "pass" while testing the wrong path.
   *Mitigation: WS6 assertion #3 explicitly fails if any live observation took the inline path.*

## Out of scope (explicit)

- Real provider APIs / real OAuth / real signatures (gap #6) — remains synthetic.
- Discord **webhook-interaction** cutover (M5.4 response-shape question) — Gateway only here.
- Fault-injection under concurrency (Run 2's FLAKY) — a natural Run 5 follow-up.
