# Ingestion LLD — Amendments Tracker

This file is the running log of implementation findings that contradict,
extend, or invalidate text in [03-low-level-design.md](03-low-level-design.md).
Every entry MUST cite (a) the LLD section that needs editing and (b) the
implementation file + line range that surfaced the finding. M3.4's closeout
folds these back into the LLD itself; until then, the tracker is the
canonical record.

**Rule for adding entries:** one entry per finding, written when the
finding surfaces. Do NOT batch — accumulating uncaptured findings is the
exact failure mode this tracker exists to prevent.

---

## Coherence pass status

**M3.4 (this milestone):** A1, A2, A3, A4, A5 folded into the LLD prose.
Edits live in [03-low-level-design.md](03-low-level-design.md) §1.3, §5.2,
§5.4, §5.5, §8, §12.1, and §13. The entries below stay in this file as
audit history; future readers should treat the LLD prose as authoritative.

**M1+M2 amendments tracker:** there is a separate
[../decisions/lld-amendments-pending.md](../decisions/lld-amendments-pending.md)
with six M1+M2 findings. M3.4 folded the three LLD-resident items
(§1.6 BEGIN/COMMIT, §5.2 Path B handler discipline + cooperative-sticky
note, §13 zero-refill sentinel). The remaining three (shadow-write
ordering — HLD, parsed-dict surfaces — new LLD subsection, infrastructure
deps — non-amendment) need a separate coherence pass and have not been
removed from that file.

---

## Open amendments

### A1 — `ingestion_failures` UPSERT key needs DB enforcement, not app-level

- **Status:** Resolved (migration 0051, M3.1).
- **LLD section:** §1.3 (`ingestion_failures` schema) and §5.5 (DLQ
  writer UPSERT).
- **Implementation surface:** [db/migrations/0046_ingestion_failures.sql](../../db/migrations/0046_ingestion_failures.sql)
  (the migration that originally deferred this to app code) and
  [services/ingestion/writers/dlq_writer/dlq_writer.py](../../services/ingestion/writers/dlq_writer/dlq_writer.py)
  (the writer that needed it).
- **What the LLD says today:** §1.3 column-justification text claims
  "the UPSERT key is enforced by application code (UNIQUE constraint
  would be too restrictive for the genuinely-distinct-occurrence cases
  like `reconciliation_gap_unresolved` which has no `raw_s3_key`)."
- **What's actually true:** Postgres treats NULLs as DISTINCT in unique
  indexes by default (`NULLS DISTINCT`), so a UNIQUE on
  `(tenant_id, source, raw_s3_key, failure_kind)` does NOT restrict
  raw_s3_key-NULL rows — multiple rows with NULL raw_s3_key are
  permitted, which is exactly the carve-out the LLD wanted. The
  app-level dedup pattern is also race-vulnerable under READ COMMITTED:
  two concurrent producers can both SELECT-miss and both INSERT,
  producing duplicate rows for the same logical failure (the recovery
  tool's hot path is exactly this race).
- **Resolution:** Migration 0051 adds `CREATE UNIQUE INDEX
  ingestion_failures_upsert_key_idx ON ingestion_failures
  (tenant_id, source, raw_s3_key, failure_kind)`. The DLQ writer
  switched from SELECT-then-INSERT/UPDATE to
  `INSERT ... ON CONFLICT (...) DO UPDATE`. The test
  `test_dlq_writer_handles_concurrent_inserts_via_unique_constraint`
  in [services/ingestion/writers/tests/test_dlq_writer.py](../../services/ingestion/writers/tests/test_dlq_writer.py)
  fires 10 concurrent UPSERTs from separate connections and asserts
  one row with `attempt_count == 10`.
- **LLD edit pending in M3.4:** rewrite the §1.3 column-justification
  paragraph and the §5.5 UPSERT paragraph; the rewrite must explain
  why NULL raw_s3_key still allows the genuinely-distinct rows
  (Postgres NULLS DISTINCT semantics), not the old "UNIQUE too
  restrictive" framing.

### A2 — `ingestion_failures.failure_kind` enum needs `embedding_ollama_failure`

- **Status:** Resolved for DB (migration 0051, M3.1). Wire side lands in M3.2.
- **LLD section:** §1.3 (CHECK enum) and §8 row 18 (failure mode
  catalog naming).
- **Implementation surface:**
  [db/migrations/0046_ingestion_failures.sql](../../db/migrations/0046_ingestion_failures.sql)
  (CHECK enum), [services/ingestion/dlq/models.py:40-44](../../services/ingestion/dlq/models.py#L40-L44)
  (wire `WireFailureKind`), and the future M3.2
  [services/ingestion/writers/embedding_worker.py] (when added).
- **What the LLD says today:** §1.3 lists 8 failure kinds, none of
  which fit Ollama embedding terminal-after-retry. §8 row 18 names the
  failure mode but uses `failure_kind='ollama_unavailable'`
  — a third spelling that matches neither the wire nor the existing
  DB enum convention.
- **What's actually true:** M3.1 ships the DLQ writer with a
  wire→DB failure_kind map; M3.2 will publish a new wire kind
  `embedding.ollama_failure` from the embedding worker which needs a
  matching DB enum value `embedding_ollama_failure`. Naming
  convention: wire is dot-separated producer-namespaced
  (`embedding.ollama_failure`), DB is underscore-separated bucket
  (`embedding_ollama_failure`). §8's `ollama_unavailable` was a
  pre-implementation guess.
- **Resolution:** Migration 0051 extends the CHECK enum to include
  `embedding_ollama_failure`. M3.2 will add the wire side in
  [services/ingestion/dlq/models.py](../../services/ingestion/dlq/models.py)
  (`WireFailureKind`) and the writer-side
  [services/ingestion/writers/dlq_writer/dlq_writer.py:66-74](../../services/ingestion/writers/dlq_writer/dlq_writer.py#L66-L74)
  map entry. No additional migration needed.
- **LLD edit pending in M3.4:** sync §1.3 CHECK list with the 9
  current enum values; rewrite §8 row 18 to use `embedding_ollama_failure`
  (DB) and `embedding.ollama_failure` (wire); add a note in §1.3 or §5.5
  that wire and DB kinds use different naming conventions and the
  bridge is `_WIRE_TO_DB_FAILURE_KIND`.

### A3 — Embedding worker UPDATE guard wording

- **Status:** Resolved (prompt superseded by LLD wording).
- **LLD section:** §5.4 (Embedding worker pool — `embed_and_update`).
- **Implementation surface:**
  [docs/ingestion/03-low-level-design.md:1737-1743](03-low-level-design.md#L1737-L1743)
  (current LLD pseudocode) vs. the actual
  `observations` schema's `embedding_pending BOOLEAN` column.
- **What the LLD says:** §5.4's `embed_and_update` pseudocode uses
  `WHERE id = $2 AND embedding_pending = TRUE` — the correct guard.
- **What the M3 prompt said (incorrect):** `WHERE id = $2 AND
  embedding IS NULL`.
- **Why the two are NOT equivalent:** the LLD wording supports
  re-embedding (operator sets `embedding_pending = TRUE` on a row
  with an existing embedding to force a re-compute — the LLD form
  succeeds because the guard only checks the flag; the prompt form
  silently fails because `embedding IS NULL` is false). The prompt
  wording also races with the inline ingestion path during the
  coexistence window (inline sets `embedding_pending = FALSE` and
  `embedding != NULL` atomically; the worker checking `embedding IS
  NULL` would still see the row as claimable until inline's commit
  is visible).
- **Resolution:** M3.2 implementation follows LLD wording; M3 prompt
  wording is incorrect and superseded. The LLD §5.4 form is
  load-bearing for both race-safety AND re-embed support. M3.2 ships
  two tests against this property:
  - `test_embedding_worker_concurrent_with_inline_safe` — race-safety
    under concurrent inline + worker writes.
  - `test_embedding_worker_supports_reembed_with_existing_embedding`
    — operator-driven re-embed: insert with `embedding=<old_vector>`
    and `embedding_pending=TRUE`, run worker, assert
    `embedding=<new_vector>` and `embedding_pending=FALSE`.
- **LLD edit pending in M3.4:** none in the LLD itself (it's already
  correct). The M3 prompt will be updated separately before M3.2's
  next iteration so the discrepancy is closed at the source.

### A6 — Discord Gateway shadow-write Kafka flush window (M4.3 finding)

- **Status:** **Resolved** (branch `fix/a6-broker-ack-ordering`,
  commits `269ce65` + `08c3b1f` + `4ddaf7f`). Option 1 chosen
  (per-frame flush). Verified by
  `test_no_frames_lost_across_sigkill` running against the
  extracted production function (no test-level workaround).
- **LLD section:** §5.4 (the Discord Gateway worker's frame-by-frame
  shadow path) + §1.5 (gateway_session_state save-after-handle
  contract).
- **Implementation surface:**
  [services/integrations/discord/gateway/dispatch.py:226-234](../../services/integrations/discord/gateway/dispatch.py#L226-L234)
  (the `shadow_write_raw` call in `_maybe_shadow_write_gateway`)
  and [services/ingestion/kafka/producer.py:116-149](../../services/ingestion/kafka/producer.py#L116-L149)
  (`IdempotentProducer.produce` returns on local-enqueue, not
  broker-ack).
- **What we found:** M4.3's load-bearing
  `test_no_frames_lost_across_sigkill` initially failed: only 1 of
  3 expected frames appeared on `ingestion.raw`. Root cause —
  `IdempotentProducer.produce()` returns when the message is in
  librdkafka's local queue, NOT when the broker has acked. The
  configured `linger_ms=5` + `acks=all` mean a 5ms window exists
  where SIGKILL drops in-flight messages. The save-after-handle
  ordering then persists `last_seq=N` to Postgres while the
  Kafka message for seq N was never delivered. Next worker
  RESUMEs past N — Discord never re-delivers — silent N1 breach.
- **What we did for the test:** the M4.3 subprocess entrypoint
  inserts `await kafka_producer.flush(timeout_seconds=5.0)` between
  `shadow_write_raw` and `save_session_state`. This makes the
  shadow-write boundary durable and the test passes.
- **What production looks like today:** the M2 production webhook
  router + M2.2 gateway dispatch call `shadow_write_raw` WITHOUT a
  flush. The design assumption is "the producer is idempotent +
  acks=all, so a producer-side restart re-publishes from in-memory
  queue." Under SIGKILL the queue is lost; under SIGTERM the
  worker has time to call `producer.stop()` which flushes.
- **Trade-off:** per-frame flush adds ~5-50ms latency (broker round
  trip). For the M5 cutover scenario where the inline path is the
  source of truth this is fine. For M6+ when the shadow path
  becomes the only path AND Discord Gateway is the surface, a
  per-frame flush would cap throughput at ~20 frames/sec (single
  shard, sequential dispatch).
- **Three options for resolution (the design discussion that
  must happen before M5):**
    1. **Per-frame flush.** Insert
       `await kafka_producer.flush(timeout=2)` between
       `shadow_write_raw()` and `save_session_state()` in the gateway
       dispatch path. The save then only persists `last_seq=N` once
       the broker has acked frame N. Strongest N1 guarantee;
       per-frame latency bounded by broker RTT (~5-50ms depending on
       broker latency + linger). Throughput ceiling per shard is
       1/RTT — adequate for Discord MESSAGE_CREATE volumes on a
       typical tenant but a hard cap if the gateway becomes the
       sole high-volume source.
    2. **Batched flush every N frames or T milliseconds.** Save
       state every frame, flush every 10 frames or 100ms. Bounds
       the loss window (at most N frames or T ms of frames lost
       under SIGKILL) without paying broker RTT per frame.
       **Violates N1 by design**: "lost up to N frames" is not
       "never lose data" under any reading. Listed here for
       completeness; should be rejected unless N1 is explicitly
       softened to "lose at most ε frames per crash."
    3. **Save inside the producer's delivery-report callback.**
       confluent-kafka's idempotent producer delivers a callback
       when the broker has acked a message. Move
       `save_session_state(last_seq=N)` into that callback so the
       save fires only after frame N is durable on Kafka.
       Decouples the WS receive loop from broker RTT (the next
       frame's `shadow_write` runs in parallel with the previous
       frame's save). Strict N1 preserved. Highest implementation
       complexity: out-of-order callback completion needs ordering
       discipline (a callback for seq=5 must not race ahead of a
       callback for seq=4 when persisting `last_seq`); the save
       races with the next frame's produce; bookkeeping for the
       in-flight set is nontrivial.
- **Read (not a decision; a starting point for the design call):**
  Option 3 is structurally correct and is what the M4.2
  "save-after-handle" contract was written to express. Option 1 is
  the conservative fallback if Option 3's complexity is judged
  too high for the throughput regime. Option 2 should be rejected
  unless N1 is renegotiated.
- **LLD edit pending:** §5.4 needs a paragraph on Kafka publish
  durability semantics + the gap between produce-return and
  broker-ack. The choice between options (1)/(2)/(3) above is a
  pre-M5 design decision (M5 makes the gateway worker the sole
  Discord ingestion path; before that flip happens, the production
  code path must be durable against broker-not-yet-acked frames).
  Tracked as M5 pre-cutover gate condition (8).
- **Resolution (2026-05-18):**
    - **Decision:** Option 1 — per-frame flush. See
      [docs/decisions/a6-resolution.md](../decisions/a6-resolution.md).
    - **Production code:** the durability barrier lives at
      [services/integrations/discord/gateway/_durability.py](../../services/integrations/discord/gateway/_durability.py)
      (free function `pre_save_flush`). Called from
      [services/integrations/discord/gateway/client.py](../../services/integrations/discord/gateway/client.py)
      `DiscordGatewayClient._dispatch_loop` between the dispatch
      handler returning and the `on_dispatched` save task being
      scheduled. On flush failure (broad-scope: any Exception),
      the metric
      `discord_gateway_pre_save_flush_failures_total` increments,
      a warning is logged, and the save is skipped — the next
      worker re-processes the frame on RESUME (safe under M2
      dedup).
    - **Latency cost:** mean 1.65 ms / p95 2.71 ms per frame on
      the dev cluster (n=100). Production 3-broker clusters run
      in the same order of magnitude.
    - **Verification:** four unit tests at
      [services/integrations/discord/gateway/tests/test_pre_save_flush.py](../../services/integrations/discord/gateway/tests/test_pre_save_flush.py)
      cover ordering, timeout, failure-skip, and broad-scope
      metric. The cross-process load-bearing
      [`test_no_frames_lost_across_sigkill`](../../services/integrations/discord/gateway/tests/test_gateway_lifecycle.py)
      now exercises the production function (subprocess
      simulation imports the same `pre_save_flush`).
    - **Operator runbook:**
      [docs/ingestion/m4-gateway-runbook.md](m4-gateway-runbook.md)
      documents the latency expectation and the failure-metric
      response procedure.

### A5 — Failure-kind-specific replay anchors

- **Status:** Open (M3.4 documents the LLD edit).
- **LLD section:** §1.3 (`ingestion_failures` schema) and §8 (failure
  mode catalog).
- **Implementation surface:**
  [docs/ingestion/03-low-level-design.md:239](03-low-level-design.md#L239)
  ("Some failures have no upstream S3 reference" — the existing
  nullability rationale) and the four wire failure kinds shipped
  through M3.2.
- **What the LLD says today:** §1.3 says `raw_s3_key` is nullable
  "because some failures (rate-limit-exhausted-pre-fetch, fetcher-
  terminal-before-any-page) have no raw body. The replay tool checks
  for NULL before attempting to re-publish." It explains the
  *nullability* but not the *alternative anchor pattern* — i.e. what
  the replay tool reads from `error_context` instead.
- **What's actually true:** Each failure kind needs its own replay
  anchor, and that anchor lives in `error_context` when it isn't
  `raw_s3_key`. The current set:
    - `normalizer.parse_failure`, `normalizer.invariant_failure`,
      `writer.invariant_failure` → `raw_s3_key` is the anchor;
      replay re-publishes the raw envelope referenced by the S3
      object.
    - `embedding.ollama_failure` → `error_context.observation_id`
      is the anchor; replay re-attempts Ollama on the observation
      row (the raw bytes are not the relevant input — the
      already-normalized `content_text` column is). `raw_s3_key` is
      NULL on these DLQ rows by design.
  Future failure kinds (reconciliation gaps, fetcher terminal
  errors, the §8 catalog rows 12–17) MUST declare their own anchor
  by populating either `raw_s3_key` or a documented key inside
  `error_context`. Replay tooling needs the convention to be
  enumerable; otherwise every new failure kind silently breaks
  replay.
- **LLD edit pending in M3.4:** rewrite §1.3 column-justification
  for `raw_s3_key` to introduce the anchor-pattern explicitly;
  extend §8's failure mode catalog with a "replay anchor" column
  listing the relevant key per row; cross-reference from §5.5 (DLQ
  writer) so future implementers see the contract.

### A7 — Discord webhook cutover deferred (synchronous-response constraint)

- **Status:** Open. Surfaced during M5.3. Resolution requires a Discord-response-shape decision (see "What's required" below).
- **LLD section:** §11 (cutover feature flag) and §11.3 (traffic signal — currently named "webhook router + FetchPage activity" producer wiring; the prose implies uniform per-provider behaviour that the M5.3 implementation does not deliver).
- **Implementation surface:** [services/webhooks/router.py:79-83](../../services/webhooks/router.py#L79-L83) (`_CUTOVER_ENABLED_PROVIDERS` = {"slack": "slack", "github": "github"}; discord intentionally excluded) and [m5-cutover-runbook.md §2](m5-cutover-runbook.md) (per-source cutover semantics table).
- **What the LLD says today:** §11 enumerates `ingestion.kafka_path_enabled` as a per-tenant flag whose TRUE value routes "the webhook" to the Kafka path. The prose is silent on per-provider semantics — implicit claim is uniform behaviour across slack/github/discord.
- **What's actually true:** Discord interactions (slash commands) require a synchronous response with shape `{"type": 4, "data": {"content": "..."}}` (CHANNEL_MESSAGE_WITH_SOURCE) within Discord's ~3-second deadline, or the Discord client UI displays "The application didn't respond in time." The M5.3 cutover contract returns 202 with `{"status": "accepted"}` — incompatible. M5.3 enforces this by excluding discord from `_CUTOVER_ENABLED_PROVIDERS`; discord webhooks remain on the inline path regardless of the flag value.
- **What's required to resolve:** decide between (a) keeping discord interactions on inline indefinitely (operationally fine; documented today), (b) synthesizing the CHANNEL_MESSAGE_WITH_SOURCE response inside the cutover branch BEFORE returning, with the observation arriving asynchronously via the writer pool (operationally feasible; requires the bot to acknowledge "Got it" and post the real follow-up message later via Discord's webhook-token follow-up API), or (c) deferring discord cutover until a Discord-specific Temporal workflow lands in M6/M7. Decision: defer to post-M5.
- **LLD edit pending:** §11 prose must explicitly enumerate per-provider cutover semantics (matches the runbook §2 table). The implicit "uniform" framing should not survive.

### A8 — Kafka partition stand-in is approximate (`_kafka_partition_for_tenant`)

- **Status:** Open. M-Temporal will resolve.
- **LLD section:** §11.3 (traffic signal — "raw_partition: the partition the just-published envelope landed on").
- **Implementation surface:** [services/webhooks/router.py::_kafka_partition_for_tenant](../../services/webhooks/router.py) (blake2b-based deterministic hash, num_partitions=32 default) and [services/ingestion/feature_flags/traffic_signal.py::maybe_emit_traffic_signal](../../services/ingestion/feature_flags/traffic_signal.py).
- **What the LLD says today:** §11.3 specifies that the signal record carries `raw_partition` so the breaker can correlate tenant → partition. Implicit assumption: the producer knows the partition synchronously at publish time.
- **What's actually true:** `IdempotentProducer.produce` enqueues to librdkafka's local queue; the REAL partition is determined asynchronously by librdkafka's murmur2_random partitioner. The producer's `produce()` return value is `None`; partition is only available via an `on_delivery` callback. M5.3 ships a blake2b deterministic stand-in (same key → same partition, NOT bit-equivalent to murmur2_random) that gives a stable per-tenant value but mis-attributes lag if the cluster has a different partition count or the partitioner differs from the default.
- **Operational impact today:** zero. The breaker's lag readers raise `NotImplementedError` until M-Temporal wires real implementations (see A9), so no production code consumes `raw_partition` for actual lag attribution yet.
- **What's required to resolve:** M-Temporal must either (a) augment `IdempotentProducer.produce` to accept an `on_delivery` callback that records partition into a tenant→partition table the breaker reads from, or (b) compute the partition deterministically using `mmh3` (or an equivalent murmur2 implementation) that matches librdkafka's algorithm bit-for-bit. Option (a) is more correct under partition-count changes; option (b) is simpler if num_partitions is fixed.
- **LLD edit pending:** §11.3 must acknowledge the partition-prediction-vs-on-delivery question explicitly; the current text presumes synchronous partition knowledge that the librdkafka contract doesn't provide.

### A9 — Default circuit-breaker Kafka readers raise `NotImplementedError` (fail-loud, intentional)

- **Status:** Open. M-Temporal will inject real implementations.
- **LLD section:** §11.2 (Cutover circuit breaker) — the LLD describes lag measurement + active-tenants sampling as concrete capabilities of the breaker.
- **Implementation surface:** [services/ingestion/feature_flags/circuit_breaker.py::_measure_kafka_lag_default](../../services/ingestion/feature_flags/circuit_breaker.py) and `_sample_active_tenants_default` — both raise `NotImplementedError` with a message naming M-Temporal as the resolution path. Tests inject mocks via the same function-pointer kwargs.
- **What the LLD says today:** §11.2 enumerates the breaker's responsibilities including "measure consumer-group lag on `ingestion.raw` per partition" and "sample active tenants from the signal topic," implying production-ready implementations.
- **What's actually true:** M5.1 ships the breaker as an asyncio service per the Phase 0 finding (Option B; Temporal infrastructure absent at M5.1 time). The state-machine logic, persistence, alert path, and operator re-enable handling are all production-ready. The Kafka readers are NOT — they raise loudly if called in production-without-injection so a misconfigured deployment fails at startup rather than silently no-op'ing.
- **Design rationale:** fail-loud is intentional. A silent no-op default would let an operator deploy the breaker, observe no trips, and (incorrectly) conclude the cutover is healthy. Raising `NotImplementedError` makes the missing infrastructure obvious. Test injection via kwargs preserves unit-test ergonomics.
- **What's required to resolve:** M-Temporal must implement `_measure_kafka_lag_default` (via `confluent_kafka.AdminClient.list_consumer_group_offsets` + broker-timestamp correlation, OR Burrow integration if operationally cheaper) and `_sample_active_tenants_default` (consumer-group reading the last `signal_lookback_sec` of `ingestion.tenant_traffic_signal`, keyed by tenant_id, returning `{tenant_id: partition}`).
- **LLD edit pending:** §11.2 must acknowledge the two-stage delivery — state machine in M5.1, Kafka readers in M-Temporal — or the LLD should be edited to forward-reference the M-Temporal section once that section is written.

### A11 — Temporal deferred indefinitely; M6 ships as asyncio with pattern-alignment

- **Status:** Open. Re-evaluated under the trigger conditions below.
- **LLD section:** §2 (Workflow orchestration via Temporal) and §11.2 (Cutover circuit breaker via Temporal Schedule). Both prescribe Temporal as the runtime; the production reality through M5.4 + M6 is asyncio services following M3.3's cursor-persistence pattern.
- **Implementation surface:** [services/ingestion/feature_flags/circuit_breaker.py](../../services/ingestion/feature_flags/circuit_breaker.py) (M5.1; asyncio); the M6.0–M6.6 services that will land under [04-implementation-plan.md §M6](04-implementation-plan.md#m6--backfill-rollout-per-source-asyncio-services-temporal-aligned) (all asyncio per the M6 restructure).
- **What the LLD says today:** §2 enumerates `OnboardingTriggerPollerWorkflow`, `TenantOnboardingWorkflow`, `SourceOnboardingWorkflow`, `ShardFetchWorkflow`, `FeelsOnboardedMonitorWorkflow`, `IngestionCircuitBreakerWorkflow` as Temporal workflows. §11.2 specifies the breaker as a Temporal Schedule.
- **What's actually true:** M5.1 ships the breaker as an asyncio service per the Phase 0 finding (Temporal infra absent, Option B chosen). M6 ships every workflow as an asyncio service per [04-implementation-plan.md §M6 pattern-alignment requirements](04-implementation-plan.md#pattern-alignment-requirements-load-bearing-for-the-seven-sub-blocks). Pattern-alignment makes a later Temporal port mechanical (workflow body ↔ asyncio main loop; activity ↔ named side-effect function; signal_workflow ↔ Postgres signal table; retry policy ↔ named retry helper; workflow state ↔ Postgres state row), but the port itself does not happen until one of the trigger conditions fires.

- **Trigger conditions for revisiting (any ONE flips the cost-benefit calculation):**

  1. **First crash-recovery failure.** An asyncio service crashes and Postgres-state reconstruction either fails to restore correctly OR loses work that Temporal's history-as-source-of-truth would have preserved. Diagnostic: an incident postmortem identifies "Temporal would have retained the prior decision tree; the asyncio service had only the latest state row."
  2. **First significant operator-tooling friction.** An incident where debugging the orchestration takes substantially longer than it would have with Temporal's workflow-history + replay tooling. "Substantially" = >2× the time of a comparable Temporal investigation. Diagnostic: an operator's incident-review explicitly names "no introspectable history of decisions" as the slowdown.
  3. **First multi-day debugging session.** An investigation that consumes >2 working days where the bisected root cause is "asyncio service had no introspectable history of its decisions." Single instance — not a pattern of three; the threshold is one instance because multi-day debugging sessions are themselves uncommon enough to be a load-bearing signal.

  These are NOT thresholds for cosmetic preferences ("Temporal would be nicer to read"); they are thresholds where the asyncio shape has demonstrably cost more engineering or operations time than the Temporal infrastructure investment would have. Document the incident, name the trigger condition met, then reopen [04-implementation-plan.md §M-Temporal](04-implementation-plan.md#m-temporal--temporal-infrastructure-deferred-indefinitely).

- **Why deferred (rationale):** standing up Temporal adds operational surface (cluster ops, SDK learning curve, deployment story) for benefits that are currently theoretical (no production traffic exists; no incident has surfaced where Temporal's replay would have shortened recovery). M3.3's cursor-style asyncio pattern demonstrated viability when state lives in Postgres and the service is single-purpose. Pattern-alignment ensures the deferral is reversible at low cost; the trigger conditions make the reversal criterion measurable rather than aesthetic.

- **What does NOT block on this deferral:** production execution of M5 cutover (gated on M-Load); M6 backfill rollout (gated on M-Load + M6.0 substrate); circuit breaker functionality (already shipping as asyncio).

- **LLD edits pending:** §2 prose must be rewritten from "Temporal workflows" to "long-running asyncio services per the pattern-alignment requirements in [04-implementation-plan.md §M6](04-implementation-plan.md#m6--backfill-rollout-per-source-asyncio-services-temporal-aligned), portable to Temporal under [05-lld-amendments.md A11](05-lld-amendments.md) trigger conditions." §11.2 prose for the breaker similarly. The Temporal-workflow code shapes in §2 stay as REFERENCE for the future port (they describe the destination, not the current code).

### A10 — Mode B writer collapses under Finding 4 (single mode ships)

- **Status:** Open. Tied to §6 Q4 (WS-latency product decision).
- **LLD section:** §5.3 (Dual-mode writer config — Phase 2.1 Q4 WS latency).
- **Implementation surface:** [services/ingestion/writers/observation_writer.py](../../services/ingestion/writers/observation_writer.py) (single per-envelope path via `_full_mode_write` → `ingest_from_draft`; no `max_poll_records` knob, no per-tenant Mode A vs Mode B selection).
- **What the LLD says today:** §5.3 specifies two writer modes selectable per tenant via `ingestion.writer_mode_low_latency`:
    - Mode A — Batched (default; ~500 records/poll; ~1000 obs/sec/process; ~500ms batch-wait latency).
    - Mode B — Low-latency (max_poll_records=1; ~50 obs/sec/process; ~50ms per-row latency).
- **What's actually true:** M5 Phase 0 Finding 4 chose per-envelope `ingest_from_draft` calls (not batched-transaction) to avoid an `ingest()` refactor that would have introduced an optional `conn` parameter shared across multiple envelopes. The accepted floor of ~50 obs/sec/process matches Mode B's profile, not Mode A's. The shipping writer is effectively Mode B; "Mode A" is a separate code path that would require either (a) a batched-transaction `ingest()` refactor or (b) a parallel batched writer that bypasses `ingest()` and writes observations directly (the N1 cutover-safety divergence risk that drove Finding 4 in the first place).
- **Operational implication:** until §6 Q4 (WS-latency tolerance) is answered, the writer ships one mode that matches Mode B's latency profile (~50ms per row) at Mode B's throughput (~50 obs/sec/process). If Q4 answers "YES, 1-5s is fine for everyone": delete the LLD §5.3 dual-mode prose; the single mode is permanent. If "NO, low-latency required for WS-sensitive tenants": Mode A would require a new work-unit ("M-Throughput") that refactors `ingest()` to accept an optional shared connection per Kafka poll, which Finding 4 explicitly defers.
- **What's required to resolve:** §6 Q4 product call. The architecture branches: Mode A is the deferred work-unit; Mode B is the current ship.
- **LLD edit pending:** §5.3 must be rewritten to either (a) describe the single-mode ship + delete the dual-mode prose, or (b) restate Mode A's throughput requirement and reference the M-Throughput follow-up work-unit. Pick once Q4 answers.

### A4 — §12.1 "one-shot script" → long-running rate-limited service

- **Status:** Open (M3.3 will implement; M3.4 documents the LLD edit).
- **LLD section:** §12.1 (Embedding backlog backfill).
- **Implementation surface:**
  [docs/ingestion/03-low-level-design.md:2690-2746](03-low-level-design.md#L2690-L2746)
  (current pseudocode) and the future M3.3
  [services/ingestion/recovery/embedding_backlog.py] (when added).
- **What the LLD says today:** §12.1 describes a one-shot script:
  reads rows in batches, sleeps to maintain QPS, returns a
  `BackfillReport`. Suitable for a small known backlog; structurally
  bounded to "run once, finish, exit."
- **What's actually true:** Production backlog at design-time is
  unknown — sizing range is 10–10M rows (per the M3 prompt's Option
  A locked decision). A one-shot script that exits after the current
  set of `embedding_pending=TRUE` rows is drained will need a
  retrofit if rows continue to land faster than the script processes
  them (steady-state burst, ingestion catch-up, etc.). M3.3 ships
  this as a rate-limited service that keeps the queue drained, reuses
  the M1.3 Lua bucket
  `(tenant_id="*system", source="ollama", method="embed")`, and
  persists a cursor so a restart resumes where it left off.
- **LLD edit pending in M3.4:** rewrite §12.1 from "one-shot script"
  to "long-running rate-limited service"; reference the M1.3 Lua
  bucket as the rate-limiter; describe cursor persistence; move
  configuration from CLI args to env vars
  (`BACKFILL_OLLAMA_QPS`, etc.); update the project structure listing
  in §9 so `recovery/embedding_backlog.py` is described accordingly.

### A12 — Executor-typed substrate signatures for transactional participation

- **Status:** RESOLVED with merge of `feat/ingestion-m6-0-executor-surface`.
- **LLD section:** none directly — this amendment refines the M6.0 substrate's API surface documented in [04-implementation-plan.md §M6.0](04-implementation-plan.md#m60--asyncio-orchestration-substrate). The LLD itself does not enumerate substrate signatures.
- **Implementation surface:** [services/ingestion/workflows/signals.py](../../services/ingestion/workflows/signals.py) (`emit_signal`, `poll_signals`, `claim_signals` NEW, `signal_count`) and [services/ingestion/workflows/state.py](../../services/ingestion/workflows/state.py) (`load_state`, `persist_state`). `state.advance_cursor_atomic_with_kafka_publish` is the deliberate exception (see below).
- **Trigger:** M6.1 Phase 0 design check surfaced that the substrate's pool-only signatures prevented atomic multi-step transactions in the OAuth poller and the TenantOnboarding orchestrator. Specifically: the poller must atomically (claim trigger row) + (insert `onboarding_runs`) + (emit `onboarding_run_created` signal), but the third step was `emit_signal(pool, ...)` which opened its own connection and committed independently. M6.1's atomicity contract required substrate participation in a caller-supplied transaction.
- **Decision:** the five non-N1 substrate functions now accept `asyncpg.Pool | asyncpg.Connection` (union spelled at each parameter; no aliased `Executor` type or Protocol). Both classes share the `.fetchval` / `.fetchrow` / `.fetch` / `.execute` duck-typed surface, so the function body works identically with either. A new `signals.claim_signals(conn: asyncpg.Connection, ...)` is the in-transaction claim primitive returning `list[WorkflowSignal]`; the existing `signals.poll_signals(pool, ...)` is refactored to delegate to `claim_signals` under a substrate-opened transaction (external contract preserved).
- **Deliberate exception:** `state.advance_cursor_atomic_with_kafka_publish` stays `pool: asyncpg.Pool` only. The N1 invariant requires the broker-ack flush to complete BEFORE the state UPDATE; extending the function into a caller-supplied transaction would let the caller commit writes AFTER the publish, which is precisely the ordering N1 forbids. The substrate enforces the invariant by owning the connection, not by trusting the caller. Documented in the function's amended docstring.
- **Effect on M6.1 and beyond:** M6.1's OAuth poller can wrap (trigger claim + onboarding-run insert + signal emit) in one `async with conn.transaction()` block, with the emit using `emit_signal(conn, ...)` so signal-emit failure rolls back the whole atomic operation. The TenantOnboarding orchestrator (M6.1 Phase 2) does the same for (claim `onboarding_run_created` + insert `source_onboarding_runs` + emit per-source `source_onboarding_requested`). The same shape applies to M6.2+ orchestrators that need claim-and-extend atomicity.
- **Effect on N1 / N2 / N3 / N4 / N5 non-negotiables:** none. The N1 invariant is unaffected — its dedicated primitive (`advance_cursor_atomic_with_kafka_publish`) stays pool-only. N2-N5 are orthogonal.
- **Relationship to [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment):** the executor amendment does NOT change A11's Temporal-deferral. The asyncio shape still ports cleanly to Temporal under A11's trigger conditions; the executor-union signature becomes an additional argument-signature change during the eventual migration (executor → Temporal activity context), but the orchestration shape is unchanged. The pattern-alignment requirements documented in [04-implementation-plan.md §M6](04-implementation-plan.md#pattern-alignment-requirements-load-bearing-for-the-seven-sub-blocks) remain load-bearing; the static analyzer (`test_pattern_alignment_passes_for_workflows_dir`) passes against the amended surface without rule modification.
- **Backwards-compat:** existing callers (M6.0 Phase 2 `FeelsOnboardedMonitor`, all existing tests) pass a Pool. Their behaviour is byte-identical after the amendment — confirmed by `test_state.py`, `test_signals.py`, `test_feels_onboarded_monitor.py`, and the full M3.3 + M5.1 retroactive smoke check.
- **Tests:** 8 new tests in [services/ingestion/workflows/tests/test_executor_surface.py](../../services/ingestion/workflows/tests/test_executor_surface.py) covering (a) Pool-path backwards-compat, (b) Connection-path transactional participation with rollback observability (the LOAD-BEARING M6.1 property), (c) `claim_signals` shape + concurrency + autocommit-without-txn documentation lock-in.
- **LLD edits pending:** none. The LLD describes Temporal workflows (still the eventual destination under A11); this amendment refines the asyncio-substrate intermediate-state API surface only. The amendment is fully captured by this tracker entry plus the [pattern-alignment-rules.md](pattern-alignment-rules.md) doc and the source-code docstrings.

### A13 — Signal addressing is a routing partition key, not a workflow instance identifier

- **Status:** RESOLVED with the Phase 1 amendment on `feat/ingestion-m6-1-oauth-and-onboarding` (the same commit that ships the OAuth poller; the addressing fix and Phase 1 are one logical unit per the M6.1 design conversation).
- **LLD section:** none directly — the LLD describes Temporal workflows where signal addressing maps to per-workflow-run inboxes naturally. This amendment refines the addressing semantics for the asyncio-substrate intermediate-state shape that ships under [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment).
- **Implementation surface:** [services/ingestion/workflows/signals.py](../../services/ingestion/workflows/signals.py) (module docstring + `WorkflowSignal` model + `emit_signal` / `claim_signals` / `poll_signals` docstrings). [services/ingestion/workflows/oauth_poller.py](../../services/ingestion/workflows/oauth_poller.py) (the M6.1 OAuth poller's emit_signal call, line 313).
- **Trigger:** M6.1 Phase 2 design check surfaced that Phase 1's OAuth poller emitted `onboarding_run_created` signals with `workflow_id=str(run_id)` — a per-run instance identifier. The Phase 2 TenantOnboarding orchestrator is a single global asyncio service that needs to consume signals across ALL onboarding_runs, but the substrate's `claim_signals(conn, workflow_kind=..., workflow_id=...)` filters by exact `workflow_id` — so the orchestrator couldn't claim per-run-addressed signals without scanning every possible run_id.
- **Decision:** the substrate's `(workflow_kind, workflow_id)` pair is a **routing partition key** for the consumer's inbox, NOT a per-resource instance identifier. Per-resource identity goes in `idempotency_key` (uniqueness) and `signal_data` (payload). For queue-consumer-style asyncio services that handle work for many resources, `workflow_id` is a fixed sentinel — typically equal to `workflow_kind` (e.g. `workflow_id="tenant_onboarding"` for the TenantOnboarding orchestrator's inbox).
  - Phase 1 amendment: `oauth_poller.py` emits with `workflow_id="tenant_onboarding"` (not `str(run_id)`). Per-run uniqueness via `idempotency_key=str(run_id)`.
  - Substrate docstring updates: `emit_signal`, `claim_signals`, `poll_signals` docstrings call out the routing-partition-key semantic; the module docstring carries the rationale + concrete examples for M6.1 + M6.2.
  - Substrate API surface: NO change. `(workflow_kind, workflow_id)` already supports this semantic; the docstrings were misleading rather than the API being wrong.
- **Why not amend the substrate to support wildcard/`None` workflow_id:** considered and rejected. A wildcard would weaken the addressing tuple's semantic (every consumer would need to know whether to pass a specific workflow_id or `None`, and the "scan-all" behaviour invites accidental cross-inbox consumption). The inbox-sentinel pattern is cleaner: the consumer-family's name IS the inbox.
- **Effect on M6.1 Phase 2 and M6.2-M6.6:** the orchestrator claims with `(workflow_kind="tenant_onboarding", workflow_id="tenant_onboarding")`. The same pattern propagates to M6.2's SourceOnboarding consumer (`(kind="source_onboarding", id="source_onboarding")`), M6.2-M6.6's per-source workflows, etc. The convention is now codified in the substrate docstrings; M6.2-M6.6 inherit it.
- **Relationship to [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment):** when the A11 trigger conditions fire and Temporal arrives, the inbox-sentinel pattern collapses naturally — the poller would `start_workflow(TenantOnboarding, id=run_id)` per resource; the signal-to-inbox shape disappears (replaced by Temporal's per-workflow signal inboxes). The Temporal port already needs to swap signal-emit for start_workflow; this amendment doesn't add a new migration cost on top of what A11 already documents.
- **Relationship to [A12](#a12--executor-typed-substrate-signatures-for-transactional-participation):** A12 enabled `emit_signal(conn, ...)` to participate in caller transactions. A13 makes the address that emit lands at consumable by an asyncio queue-consumer service. Both amendments are needed together for M6.1+ to work; both are independent of the N1 / N2 / N3 / N4 / N5 non-negotiables.
- **Pattern-alignment status:** no change. The static analyzer's five rules don't touch signal addressing; the amendment is documentation + one production line + one test line.
- **Tests:** Phase 1's existing test `test_oauth_poller_signal_emit_uses_run_id_as_idempotency_key` is updated to assert the new addressing (`workflow_id="tenant_onboarding"`, `idempotency_key=str(run_id)`). No new test class is needed — the property is already exercised by `test_oauth_poller_creates_onboarding_run_and_emits_signal_atomically` (which doesn't filter by workflow_id) and will be exercised again by Phase 2's `test_orchestrator_handles_run_created_signal_atomically` (which claims from the inbox).
- **LLD edits pending:** none. Same posture as A12; the LLD describes Temporal-workflow signal semantics, which don't apply to the asyncio inbox pattern. The amendment is fully captured by this tracker entry + the substrate docstrings.

### A14 — Source applicability resolved at orchestrator-tick-time, not at trigger-fire-time

- **Status:** RESOLVED with M6.1 Phase 2 (commit `93d6b71` on `feat/ingestion-m6-1-oauth-and-onboarding`).
- **LLD section:** LLD §2 (TenantOnboardingWorkflow shape) implicitly assumes the workflow receives a fixed `sources_enabled` list at start time. This amendment refines that semantic for the asyncio shape (which ships under [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment) until Temporal returns).
- **Implementation surface:** [services/ingestion/workflows/tenant_onboarding.py](../../services/ingestion/workflows/tenant_onboarding.py) (`_LOAD_ACTIVE_SOURCES_SQL` and `_determine_applicable_sources`, plus the "SOURCE APPLICABILITY" section of the module docstring).
- **Trigger:** M6.1 Phase 2 design check faced the question "where does the orchestrator get the list of sources to fan out to?" Two candidates: (a) the `onboarding_runs.sources_enabled[]` array (a snapshot from the trigger row, written by the poller at trigger-claim time); (b) live `provider_installations` + `gmail_installations` rows at orchestrator-tick-time. The two diverge when installs are added or disabled between trigger-fire and orchestrator-pickup.
- **Decision:** **live installs at orchestrator-tick-time IS the source of truth.**
  - `provider_installations` filtered by `(tenant_id, enabled=TRUE, provider IN ('slack','github','discord'))` UNION `gmail_installations` filtered by `(tenant_id, disabled_at IS NULL)` is the fan-out list.
  - `onboarding_runs.sources_enabled[]` becomes a SNAPSHOT artifact for audit — what the trigger described — NOT a controlling input.
  - Edge case: zero active installs at tick-time → mark the parent run `'failed'` with `error_summary = 'No active installs for tenant at orchestrator tick-time.'` (the Phase 2 Decision 3 cited in the runbook's failure-mode catalog).
- **Rationale.** Trigger-fire-to-orchestrator-pickup is not zero-latency. A tenant who installs slack at t=0 then gmail at t=2s then the orchestrator picks up the slack trigger at t=5s should onboard BOTH sources — using the trigger's `sources_enabled=[slack]` snapshot would silently skip gmail. The cost: when two triggers fire close together for the same tenant, BOTH runs fan out to the full active-install set; duplicate per-source backfill is the price. M6.2's SourceOnboarding owns idempotent-backfill decisions per (tenant, source); deduplication of overlapping backfill attempts is M6.2's concern, not M6.1's.
- **Effect on M6.2-M6.6:** SourceOnboarding receives `source_onboarding_requested` signals for whatever sources the orchestrator fan-out emits. Whether to re-backfill an already-backfilled (tenant, source) pair is an idempotency design decision for M6.2; the M6.1 contract is "the orchestrator emits a `source_onboarding_requested` per active install per parent run." Documented in [04-implementation-plan.md §M6.1 decision 3](04-implementation-plan.md#m61--oauth-outbox-poller--tenantonboarding).
- **Relationship to [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment):** when Temporal returns, the same semantic applies — TenantOnboardingWorkflow's first activity should be `query_active_installs(tenant_id)`, not "use the workflow-input sources_enabled list." The amendment's logic survives the port; only the orchestration shape (asyncio service ↔ Temporal workflow) changes.
- **Relationship to A13:** A13 made the orchestrator a global queue consumer. A14 codifies WHAT it queries when handling each claimed signal. Both amendments are M6.1 Phase 2 artifacts; both inherit cleanly to M6.2+.
- **Pattern-alignment status:** no change. The five rules don't touch source-applicability semantics.
- **Tests:** `test_orchestrator_determines_applicable_sources_from_installs` (LOAD-BEARING for the rule) and `test_orchestrator_fails_run_when_no_installs_active` (zero-installs edge case) in [test_tenant_onboarding.py](../../services/ingestion/workflows/tests/test_tenant_onboarding.py).
- **LLD edits pending:** none. Same posture as A12 and A13 — the LLD describes Temporal workflows; this is an asyncio-shape refinement that survives the eventual port. The amendment is fully captured by this tracker entry + the source-code docstrings + the implementation plan's §M6.1 architectural-decisions list.

### A15 — M6.2a uses M1-shipped `onboarding_shards` schema; no new migration

- **Status:** RESOLVED with M6.2a Phase 1 on `feat/ingestion-m6-2a-source-onboarding-and-shard-fetch`.
- **LLD section:** §1.2 (`onboarding_shards` schema). The amendment codifies that the LLD §1.2 schema as shipped in [db/migrations/0045_onboarding_runs_and_shards.sql](../../db/migrations/0045_onboarding_runs_and_shards.sql) IS the authoritative spec; M6.2a uses it without modification.
- **Implementation surface:** [services/ingestion/workflows/source_onboarding.py](../../services/ingestion/workflows/source_onboarding.py) (Phase 1) and [services/ingestion/workflows/shard_fetch.py](../../services/ingestion/workflows/shard_fetch.py) (Phase 2). [services/ingestion/planners/__init__.py](../../services/ingestion/planners/__init__.py) for the `PLANNER_DISPATCH` table and `Shard` dataclass.
- **Trigger:** M6.2a Phase 0 design check surfaced that the M6.2a prompt directed a new `0056_onboarding_shards.sql` migration with a schema that conflicted with the existing M1-shipped `onboarding_shards` table (which is the LLD §1.2 schema verbatim). The prompt was written without verifying that 0045 had already shipped the schema three milestones back.
- **Decision:** **NO new migration 0056 ships with M6.2a.** The M1-shipped `onboarding_shards` schema is strictly more capable than the prompt-described schema — it already carries `parent_shard_id` and the `'reconciliation_resharded'` state value that M6.2b's Reconciler will need, plus `recency_score` / `window_start` / `window_end` / `pages_fetched` / `observations_seen` for M6.3-M6.6 per-source fetcher use. Replacing it would have lost those columns and broken the LLD §1.2 commitment.
- **Column-naming map (M6.2a-prompt-words → existing-schema-columns):**

  | M6.2a prompt term | Existing column (0045) | Notes |
  |---|---|---|
  | `shard_id` (PK) | `id UUID PRIMARY KEY` | Same role. Code uses `id` everywhere. |
  | `shard_descriptor JSONB` | `shard_identifier JSONB NOT NULL` + `shard_kind TEXT NOT NULL` | Existing splits descriptor into kind + identifier. M6.2a writes both. |
  | `cursor JSONB` | `cursor_token TEXT` | M6.2a does NOT write to this column. Cursor lives in `workflow_states.state_data` per the M6.0 N1 primitive's contract (see below). |
  | `status` | `state` | Different name; same role. |
  | `failure_reason` | `last_error` | Same role; different name. |

- **Status-value mapping (M6.2a-prompt-words → existing-schema-values):**

  | M6.2a prompt value | Existing schema value | Notes |
  |---|---|---|
  | `'pending'` | `'pending'` | Same. |
  | `'in_progress'` | `'in_progress'` | Same. |
  | `'completed'` | `'done'` | Different vocabulary. Codified in M6.2a. |
  | `'failed'` | `'failed'` | Same. |
  | (M6.2b territory) | `'reconciliation_resharded'` | Reserved for M6.2b's Reconciler. M6.2a does NOT write this value. |

  Cross-table vocabulary mismatch (documented rather than papered over): `source_onboarding_runs.status` (M6.1's [0055](../../db/migrations/0055_source_onboarding_runs.sql)) uses `'completed'`; `onboarding_shards.state` (M1's [0045](../../db/migrations/0045_onboarding_runs_and_shards.sql)) uses `'done'`. Each is internally consistent within its own schema; the mismatch is real but bounded.

- **N1 primitive cursor home overlaps with legacy `cursor_token`.** The M6.0 substrate introduced `state.advance_cursor_atomic_with_kafka_publish` which writes the cursor to `workflow_states.state_data` (JSONB), keyed by `(workflow_kind, workflow_id)`. M6.2a's ShardFetch uses `workflow_kind="shard_fetch"` + `workflow_id=str(shard_id)` — one `workflow_states` row per shard. The legacy `onboarding_shards.cursor_token TEXT` column (from M1's 0045, predating the N1 primitive) stays NULL under M6.2a. M6.3-M6.6 per-source fetchers may optionally mirror the cursor to the legacy column for ops visibility; that's a per-source choice and not load-bearing.

- **`shard_kind` value convention (codified for M6.3-M6.6):**

  | Source | `shard_kind` value | M6.x sub-block |
  |---|---|---|
  | `slack` | `"slack_channel_window"` | M6.5 |
  | `github` | `"github_repo_events"` | M6.4 |
  | `discord` | `"discord_channel_window"` | M6.6 |
  | `gmail` | `"gmail_mailbox_window"` | M6.3 |

  M6.2a's planner dispatch table writes the stubbed `NotImplementedError`; M6.3-M6.6 each ship their respective planner with the matching `shard_kind`. The corresponding fetcher in [services/ingestion/fetchers/__init__.py](../../services/ingestion/fetchers/__init__.py) keys off this value.

- **Effect on M6.2b (Reconciler):** the existing `parent_shard_id UUID REFERENCES onboarding_shards(id)` column + the `'reconciliation_resharded'` state value are the M6.2b Reconciler's inheritance anchors. M6.2b adds NO schema columns; it INSERTs new `onboarding_shards` rows with `parent_shard_id` set and `state='reconciliation_resharded'` per the existing M1 design.

- **Relationship to [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment), [A12](#a12--executor-typed-substrate-signatures-for-transactional-participation), [A13](#a13--signal-addressing-is-a-routing-partition-key-not-a-workflow-instance-identifier), [A14](#a14--source-applicability-resolved-at-orchestrator-tick-time-not-at-trigger-fire-time):** Same posture chain. A11 deferred Temporal; A12 amended substrate signatures; A13 codified signal addressing; A14 fixed source applicability; A15 codifies the cross-milestone schema-vocabulary reconciliation. Each amendment ships under the M6.x sub-block that surfaces it, written when surfaced, without batching. Future M6 sub-blocks should expect their own findings to land in this tracker as A16+.

- **Shared-inbox pattern is the M6 idiom (codified here for M6.2b + M6.3-M6.6).** M6.1's TenantOnboarding orchestrator consumes both `onboarding_run_created` and `source_onboarding_completed` from a single inbox `(tenant_onboarding, tenant_onboarding)` and Python-dispatches on `signal_kind`. M6.2a's SourceOnboarding consumes both `source_onboarding_requested` and `shard_fetch_completed` from `(source_onboarding, source_onboarding)` with the same shape. M6.2b's Reconciler is expected to consume `source_onboarding_completed` from M6.1 from a `(reconciler, reconciler)` inbox (with whatever additional kinds it needs); M6.3-M6.6 per-source workflows may have their own kind-overlap needs. **Default: one inbox per consumer-family, Python-dispatch on `signal_kind` after claim.** The alternative — per-signal-kind inboxes — is available if a sub-block has a measurement-correctness reason to want it.

- **Pattern-alignment status:** no change. M6.2a's services use the substrate per the five rules; no analyzer change required.

- **Tests:** Phase 1's tests of `source_onboarding.py` verify the column writes work against the existing schema (`id`, `shard_kind`, `shard_identifier`, `state='pending'` / `'done'`, etc.). No schema-coupling test changes needed.

- **LLD edits pending:** none. The LLD §1.2 IS authoritative; M6.2a's job was to use it, not modify it. The amendment is fully captured by this tracker entry + the `source_onboarding.py` module docstring + the Phase 1 gate output (three-place documentation per the M6.0/M6.1 precedent).

### A16 — Three transactional patterns codified (M6 service-design guide)

- **Status:** RESOLVED with M6.2a Phase 3.
- **LLD section:** none directly — this amendment codifies the cross-service design patterns that ship across M6.0 + M6.1 + M6.2a. The LLD describes Temporal workflows where these distinctions are subsumed by Temporal's primitives; this amendment refines the patterns for the asyncio shape that ships under [A11](#a11--temporal-deferred-indefinitely-m6-ships-as-asyncio-with-pattern-alignment).
- **Implementation surface:** [services/ingestion/workflows/feels_onboarded_monitor.py](../../services/ingestion/workflows/feels_onboarded_monitor.py) (CLAIM-VIA-UPDATE worked example); [services/ingestion/workflows/shard_fetch.py](../../services/ingestion/workflows/shard_fetch.py) (N1 + fetch-loop worked examples); [services/ingestion/workflows/state.py::advance_cursor_atomic_with_kafka_publish](../../services/ingestion/workflows/state.py) (the N1 primitive).
- **Trigger:** M6.2a Phase 3 acceptance Decision 2 — three transactional patterns are now first-class in the codebase, and M6.2b/M6.3-M6.6 engineers need to know which pattern to use for which use case. Without this codification, future contributors would re-litigate the same design decisions.

- **The three patterns + choice criterion:**

  | Pattern | When to use | Retry semantic | Worked example |
  |---|---|---|---|
  | **N1 publish-then-persist** | The work is a cursor advance with associated Kafka publish; re-publish on retry is safe (idempotent producer + downstream UNIQUE dedup). | If publish fails, NEXT TICK re-attempts from the unchanged cursor. At-least-once delivery + dedup makes this safe. | `state.advance_cursor_atomic_with_kafka_publish` + `shard_fetch.py:_run_fetch_loop`. |
  | **CLAIM-VIA-UPDATE single-fire** | The work is a single-fire event (e.g., "tenant feels onboarded"); concurrent racers MUST NOT double-publish. | If the UPDATE matches but the publish fails AFTER, the event is marked fired but never reached the consumer. Acceptable iff consumers can rediscover the milestone via their own queries. | `feels_onboarded_monitor.py:_claim_and_publish_feels_onboarded`. |
  | **Multi-tick fetch loop with durable state surfaces** | The work spans many cursor advances (long-running fetch with external API calls); per-page atomicity is enough; the LOOP's overall progress lives in Postgres. | Per-page atomicity owned by the N1 primitive (each advance is publish-then-persist). The loop itself is NOT one transaction; restart resumes via durable state (`onboarding_shards.state='in_progress'` + `workflow_states.state_data["cursor"]`). | `shard_fetch.py:_run_fetch_loop` (same file as pattern 1 — the loop composes per-page N1 advances). |

- **Choice criterion (the design-time question):**
  - Is the work a single atomic step that produces ONE event? → CLAIM-VIA-UPDATE if double-publish is unacceptable; N1 otherwise.
  - Is the work a multi-step cursor advance, each step publishing? → N1 per step.
  - Does the multi-step work span minutes/hours, requiring restart resumption? → Multi-tick fetch loop pattern (compose N1 per step; treat the durable state surfaces as the resume anchor).

- **Pattern composition is expected.** ShardFetch demonstrates this: the fetch loop pattern (compose multiple N1 advances + use durable state to survive restart) wraps per-page N1 advances. M6.2b's Reconciler will likely combine CLAIM-VIA-UPDATE (one-shot decision to re-shard) with N1 (the new shard rows + Kafka publishes after the re-shard).

- **Relationship to the pattern-alignment static analyzer.** The analyzer is ordering-agnostic by design (per [pattern-alignment-rules.md](pattern-alignment-rules.md) Rule 2's "What it does NOT check" + the M6.0 Phase 3 commentary). The analyzer enforces STRUCTURAL properties (state goes through Postgres at some point); choosing the right pattern is a DESIGN-review concern enforced by per-service tests + this codification. Future M6 sub-blocks adding a fourth pattern should add an entry to this table and call it out in their service-module docstring.

- **Relationship to A12 (executor surface).** The pattern-choice and the executor-surface choice are orthogonal. CLAIM-VIA-UPDATE and N1 BOTH have variants that accept Pool (auto-commit) or Connection (caller-managed transaction). The choice criterion is "do I need to extend with adjacent writes in one atomic operation?" The pool-only-N1-primitive exception is documented in A12 for the deliberate ordering reason.

- **Relationship to the pattern-alignment-rules.md doc.** A16 supplements; it does NOT replace. The five rules in pattern-alignment-rules.md are STRUCTURAL (orchestration separation, state in Postgres, named retry, signals via Postgres, no cross-workflow state). A16 is the DESIGN-INTENT guide for which transactional pattern to choose. Future readers: rules-doc for "is my code structurally correct?"; A16 for "am I using the right pattern for my use case?"

- **Tests:** the three patterns are verified across:
  - N1: `test_advance_cursor_atomic_publishes_before_persists` (M6.0 Phase 1) + `test_shard_fetch_N1_invariant_holds` (M6.2a Phase 2 — service-integration level).
  - CLAIM-VIA-UPDATE: M6.0 Phase 2's `test_feels_monitor_*` suite verifies single-fire under concurrent racers.
  - Multi-tick fetch loop: `test_shard_fetch_resumes_from_persisted_cursor_after_restart` (M6.2a Phase 2, real subprocess) — proves the LOOP's restart resumption via durable state surfaces.

- **LLD edits pending:** none. Same posture as A11/A12/A13/A14/A15 — the LLD describes Temporal-workflow semantics that subsume these distinctions (Temporal owns transactional + retry semantics natively); this amendment is the asyncio-shape design guide. Three-place documentation: this amendment + each pattern's worked-example file's module docstring + the Phase 3 gate output.

### A17 — Reconciler state machine, idempotency-key discipline, and re-share recency boost

- **Status:** RESOLVED with M6.2b Phase 2 on `feat/ingestion-m6-2b-reconciler`.
- **LLD section:** LLD §2 (Reconciler workflow shape) and §3 (re-share recency-score boost).
- **Implementation surface:** [services/ingestion/workflows/reconciler.py](../../services/ingestion/workflows/reconciler.py) (state machine + idempotency); [services/ingestion/workflows/source_onboarding.py](../../services/ingestion/workflows/source_onboarding.py) (chain-change emit + pass-count idempotency key); [db/migrations/0056_reconciler_columns.sql](../../db/migrations/0056_reconciler_columns.sql) (two additive columns); [services/ingestion/reconcilers/__init__.py](../../services/ingestion/reconcilers/__init__.py) (`ResharedShard.recency_score` default).
- **Trigger:** M6.2b Phase 1 implementation surfaced three architectural decisions that M6.3-M6.6 + M6.2-future-readers need to inherit cleanly: (1) the `source_onboarding_runs.status` state machine across re-share cycles; (2) the SPLIT idempotency-key discipline between `source_shards_completed` (pass-count-keyed) and `source_onboarding_completed` (run-keyed); (3) the re-share recency-score boost per LLD §3.

- **(1) Reconciler state machine** — `source_onboarding_runs.status` transitions across the M6.2b chain:

  ```
  'pending' ──────► 'in_progress' ──────► 'completed'
                      ↑                          │
                      │                          │ (Reconciler decides reshare)
                      └─────'in_progress'──◄─────┘
                                  │
                                  │ (new shards complete; SourceOnboarding rolls up)
                                  ▼
                              'completed'
                                  │
                                  │ (Reconciler clean → stamp reconciled_at)
                                  ▼
                              'completed' AND reconciled_at IS NOT NULL  ← terminal
  ```

  Each `completed → in_progress` transition is one re-share cycle. `reconciliation_pass_count` increments on each. The TERMINAL state — the consumer-side observable — is `status='completed' AND reconciled_at IS NOT NULL`. The TRANSIENT state of operator interest is `status='completed' AND reconciled_at IS NULL` (post-rollup, pre-Reconciler-pickup). Migration 0056's `source_onboarding_runs_awaiting_reconcile_idx` is the diagnostic index.

- **(2) Idempotency-key discipline (SPLIT keys; LOAD-BEARING):**

  | Emit | Producer | Consumer | Idempotency key | Rationale |
  |---|---|---|---|---|
  | `source_shards_completed` | SourceOnboarding (rollup) | Reconciler | `f"{run_id}:{source}:pass_{N}"` | One per re-share cycle. Pass-count makes each cycle's emit a fresh key so `emit_signal` doesn't silently dedup the second cycle. |
  | `source_onboarding_completed` | Reconciler (clean path) | TenantOnboarding | `f"{run_id}:{source}"` | EXACTLY ONE per (run, source) lifetime. Pass-count NOT included so a re-emit-after-replay dedups at the UNIQUE constraint — TenantOnboarding sees the signal exactly once regardless of re-share cycle count. |
  | `shard_fetch_requested` | Reconciler (reshare path) | ShardFetch | `str(new_shard_id)` | Per-new-shard uniqueness. Matches M6.2a's ShardFetch consumer expectation. |

  The SPLIT — pass-count-keyed cycle signal vs run-keyed terminal signal — is the load-bearing invariant. Without it, either the re-share cycle deadlocks (cycle key dedups), or TenantOnboarding double-counts completions (terminal key includes pass_count). Verified by `test_reconciler_replays_completion_for_already_reconciled_run` (in-process, exactly-one assertion) and `test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` (5-subprocess E2E across pass_0 + pass_1).

- **(3) Re-share recency-score boost (per LLD §3):** new shards created by the Reconciler get `recency_score=1.5` by default (vs. the planner default of 1.0). The boost lets reshared gap-fillers run ahead of any remaining low-recency backfill — per LLD §3 + HLD §6 specifications. The default lives in test reconcilers (per `test_reconciler.py` and the reshare-path E2E test); M6.3-M6.6 per-source reconcilers may override per source-specific concerns. The boost is INTENTIONAL, not arbitrary; future readers (and code reviewers spotting a magic `1.5`) should refer to this entry + LLD §3.

- **Re-share linkage semantic** (codifying the M6.2b contract for M6.3-M6.6):
  - **`parent_shard_id`** on a re-shared `onboarding_shards` row references the ORIGINAL shard whose gap is being filled. The original transitions `done → reconciliation_resharded` (terminal) in the same transaction that INSERTs the new row.
  - The original's `done` state is preserved as audit history via the state value change — the data it collected is still there; `state='reconciliation_resharded'` means "this shard's data is good, but a sibling is filling its gap region."
  - Multiple new shards may share the same `parent_shard_id` (one original split into N gap-fillers). Each original is marked `reconciliation_resharded` exactly once even if N child shards reference it (the Reconciler de-dups parent IDs via a set).

- **Relationship to A15 + A16:** A15 codified the M1-shipped schema reuse and the schema-vocabulary mapping. A16 codified the three transactional patterns. A17 codifies the cross-cycle state machine + idempotency discipline + recency boost — operational refinements that compose with A15's schema and A16's patterns. M6.3-M6.6 readers find all three amendments together for the full per-source contract.

- **Pattern-alignment status:** no change. The Reconciler service satisfies all five rules by construction.

- **Pre-M6.3-M6.6 expected steady state.** Every per-source dispatch entry in `RECONCILER_DISPATCH` defaults to `has_gaps=False` (clean). The re-share path is exercised ONLY by tests; production traffic always takes the clean path until M6.3-M6.6 ship real algorithms. Documented in [runbook §6.C.4](m5-cutover-runbook.md) for operator clarity.

- **Tests:** the load-bearing properties are verified across:
  - State machine: `test_reconciler_handles_source_shards_completed_reshare_path` (single-cycle, in-process); `test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` (full-cycle, 5-subprocess E2E).
  - Idempotency discipline: `test_reconciler_replays_completion_for_already_reconciled_run` (in-process, asserts exactly-one source_onboarding_completed); the reshare-path E2E also asserts exactly-one source_onboarding_completed across 2 cycles.
  - Recency boost: the reshare-path tests verify `recency_score=1.5` on the reshared shard.

- **LLD edits pending:** none. Same posture as A11-A16 — the LLD describes Temporal-workflow semantics where this kind of state-machine + idempotency-key discipline is owned by Temporal natively; the asyncio-shape needs explicit codification. Three-place documentation: this entry + the `reconciler.py` module docstring + the M6.2b Phase 2 gate output.

---

## A18 — Per-source backfill is net-new code; framework + existing steady-state coexist until M7

**Status:** Resolved with M6.3 merge.

**Trigger:** M6.3 pre-Phase-1 audit established that the existing per-source code is steady-state machinery (Pub/Sub push + 10-min poll via Gmail's `users.history.list` for Gmail; analogous shapes for the other sources) — NOT backfill. True backfill via per-source backfill APIs (`users.messages.list` for Gmail; equivalent endpoints for GitHub/Slack/Discord) is **not implemented anywhere in the codebase pre-M6.3**. The initial M6.3 prompt's "behavior-preserving refactor of existing fetcher.py" premise was wrong.

A second substrate finding (S1) emerged during the revised M6.3 audit: Gmail's `gmail_installations` schema is workspace-scoped, not per-mailbox; the planner contract (`Callable[[UUID, asyncpg.Record], Awaitable[list[Shard]]]`) doesn't provide DB access, but the planner needs to enumerate mailboxes from `gmail_mailbox_watches`.

A18 codifies how these are resolved for M6.3 and how M6.4-M6.6 inherit the pattern.

### A18.1 — Per-source backfill is net-new code

**Decision:** Each M6.x per-source sub-block (M6.3-M6.6) ships net-new backfill code via three dispatch entries (planner, fetcher, reconciler). Existing per-source code (steady-state push/poll, webhook ingestion, Gateway, etc.) is NOT retired in these sub-blocks. The M6 framework's backfill path coexists with the existing steady-state path. Coexistence is interim; resolution happens in M7-territory inline-ingestion retirement work (deferred tickets filed in M6.3 Phase 3).

**Effect on M6.4-M6.6:** Each follows the same pattern. Each starts with a **pre-Phase-1 substrate audit** verifying:
- (a) The per-source backfill API exists and is callable from the existing client.
- (b) Whether per-source code retirement is needed (default expectation: NO — existing code is steady-state).
- (c) Whether M6.2a's install-loading handles this source (Gmail: yes via `_LOAD_GMAIL_INSTALL_SQL`; GitHub/Slack/Discord: presumed via `_LOAD_PROVIDER_INSTALL_SQL`).
- (d) Whether the per-source planner needs any per-source data plumbing the framework doesn't yet provide (e.g., Gmail's `gmail_mailbox_watches` aggregation).

If an audit surfaces a substrate finding, STOP and surface (same discipline as A12/A13/A15/A17). New A-numbered amendments only land if substrate findings require them.

### A18.2 — Per-source loader enrichment pattern (the S1 finding's resolution)

**Decision:** Per-source loaders in `services/ingestion/workflows/source_onboarding.py` may aggregate per-source enrichment data via JSON aggregation. The planner reads the enriched install record and stays stateless (no DB I/O in the planner).

**Example (Gmail, M6.3 S1 amendment):**

```sql
-- _LOAD_GMAIL_INSTALL_SQL — aggregates active mailboxes as a JSON column
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'email_address', mw.email_address,
             'google_user_id', mw.google_user_id,
             'history_id', mw.history_id
           ) ORDER BY mw.email_address
         ) FILTER (WHERE mw.id IS NOT NULL),
         '[]'::json
       ) AS mailboxes
  FROM gmail_installations gi
  LEFT JOIN gmail_mailbox_watches mw
    ON mw.gmail_installation_id = gi.id AND mw.state = 'active'
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 GROUP BY gi.id LIMIT 1
```

The planner reads `install["mailboxes"]`, orjson-decodes it (the JSON aggregate arrives as a string), and emits one shard per active mailbox.

**Inheritance:** M6.4-M6.6 may use this pattern if their installs need per-source enrichment. Each pre-Phase-1 audit asks: "is the install record self-contained, or does the planner need per-source 1-to-N data (channels, repos, guilds)?" If the latter, extend the source-specific loader.

**ShardFetch's loader is NOT subject to this pattern**: the fetcher works on one shard at a time via `shard_identifier`; aggregated mailbox/channel/repo lists are irrelevant inside the fetcher. Per-source enrichment lives in the planner's loader path only.

### A18.3 — Reconciler pool-provider seam

**Decision:** Per-source reconciler modules (`services/ingestion/reconcilers/<source>.py`) may need pool access for auxiliary DB reads (e.g., Gmail reads `workflow_states` for each shard's `final_history_id`). The M6.2b `RECONCILER_DISPATCH` contract does NOT pass a pool to the dispatch function. Resolution: per-source modules expose a module-level `set_pool_provider(pool)` function, and the service-startup path (in `services/ingestion/workflows/reconciler.py::_run_service` and `services/ingestion/workflows/__main__.py`) calls it before the reconciler service starts.

**Why not change the dispatch contract:** changing the M6.2b contract would require modifying all four per-source stubs and `services/ingestion/workflows/reconciler.py`. The seam pattern is a smaller blast radius, observable failure mode (`RuntimeError: pool provider not registered`), and tests can rebind it per-test via `monkeypatch.setattr`.

**Inheritance:** M6.4-M6.6 reconcilers SHOULD use this pattern if they need pool access. The startup path registers each source's pool provider before the Reconciler service starts.

### A18.4 — `shard_kind` mirrored into `shard_identifier`

**Decision:** Per-source fetchers may dispatch on `shard_kind` to know which API to call. The `onboarding_shards.shard_kind` column is the canonical row-level value (used by indexes / operator queries), but the fetcher receives only `shard_identifier` (JSONB) from `ShardFetch`'s call site. To make the fetcher's dispatch unambiguous without a framework contract change, planners and reconcilers SHOULD mirror the `shard_kind` value into `shard_identifier["shard_kind"]`. The fetcher reads it from there.

**Example (Gmail, M6.3):**

```python
# Gmail planner:
Shard(
    shard_kind="gmail_mailbox_window",
    shard_identifier={
        "shard_kind": "gmail_mailbox_window",  # mirrored
        "mailbox_email": "...",
        "user_id": "...",
    },
)

# Gmail reconciler (gap-fill shard):
ResharedShard(
    shard=Shard(
        shard_kind="gmail_history_gap",
        shard_identifier={
            "shard_kind": "gmail_history_gap",  # mirrored
            "start_history_id": "...",
            "end_history_id": "...",
        },
    ),
    parent_shard_id=...,
)
```

The fetcher reads `shard_identifier.get("shard_kind")` and routes to the right per-source API path.

**Inheritance:** M6.4-M6.6 may use this pattern when a single per-source fetcher serves multiple shard kinds (backfill vs gap-fill, or any other per-source-API variant). If a source has only one shard kind, the mirror is optional but recommended for symmetry.

### A18.5 — Cross-references

A18 inherits from and extends:
- **A12** — Executor-typed substrate signatures.
- **A13** — Signal addressing as routing partition key.
- **A14** — Source applicability resolved at orchestrator-tick-time.
- **A15** — M6.2a uses M1-shipped `onboarding_shards` schema.
- **A16** — Three transactional patterns (N1, CLAIM-VIA-UPDATE, multi-tick fetch loop).
- **A17** — Reconciler state machine + idempotency-key discipline + recency boost.

**Three-place documentation:** this amendment + each per-source module's docstring (planner / fetcher / reconciler) + the M6.3 closeout in `docs/ingestion/04-implementation-plan.md` §M6.3.

**LLD edits pending:** none. Same posture as A11-A17 — the LLD describes a Temporal-workflow design where these patterns would map onto Temporal's native primitives; the asyncio-shape needs explicit codification. M6.4-M6.6 may extend A18 with additional sub-sections (A18.6, A18.7, etc.) for per-source patterns that emerge.

### A18.6 — PlannerContext (M6.4 substrate addition)

**Status:** Resolved with M6.4 merge.

**Trigger:** M6.4's GitHub planner needs API access at plan time (enumerate repos via Octokit's `/installation/repositories`). The M6.2a/M6.3 planner contract (`(tenant_id, install) -> list[Shard]`) doesn't provide a source-side client.

**Decision:** Supersede the planner contract with `PlannerContext(tenant_id, install, conn, source_client)`. The planner receives a single bundle; per-source planners use whichever surfaces they need. Gmail's planner uses only `ctx.install` (the S1-amended loader provides the per-source data). GitHub's planner uses `ctx.source_client` for API enumeration. The `conn` is available for sources that prefer direct DB access (none currently use it).

**Implementation:**
- `services/ingestion/planners/context.py` — defines `PlannerContext`.
- `services/ingestion/planners/__init__.py` — `Planner` type alias updated to `Callable[[PlannerContext], Awaitable[list[Shard]]]`.
- `services/ingestion/workflows/source_onboarding.py` — adds `_build_source_client(source, pool, install)` factory; constructs the PlannerContext at the dispatch call site.
- All existing M6.3 Gmail planner tests pass after the refactor (signature change is mechanical; Gmail uses only `ctx.install`).

**Inheritance:** M6.5 (Slack) and M6.6 (Discord) follow the same pattern. Each per-source `_build_source_client` branch in `source_onboarding.py` decides what client to construct (or `None` if the planner doesn't need one).

---

## A30 — M-Validate-Live: composed backfill + live validation runs with per-source-scoped assertions; closes ticket #47

**Status:** Resolved on `feat/ingestion-validation-runs-live-composition`. Closes ticket #47 and the synthetic-testability chain that began with Z1.

**Trigger.** Ticket #47 (the spine's deferred remainder). The spine (A29) established the runner infrastructure + Run 1 backfill; A30 closes the loop with live composition + cross-path dedup + Runs 2–3. A pre-implementation substrate audit surfaced five discrepancies between the original mega-prompt and codebase reality (all absorbed before coding) plus one mid-implementation finding (gmail install-reuse, A30.2).

**A30.1 — Live-phase orchestration.** `composition.py` composes the four in-process live generators into the runner's live phase, which runs after the backfill drain against the SAME tenants/installs backfill used. The two HTTP webhook generators (`SlackWebhookGenerator`, `GithubWebhookGenerator`) share one FastAPI app built via the canonical `services.gateway.main.build_app(pool=..., slack_signing_secret=...)` (audit finding 2: the original prompt's `services.webhooks.app` path was wrong). Gmail Pub/Sub gets its OWN minimal app (`FastAPI()` + `services.webhooks.gmail_pubsub.router`; the router is NOT mounted by `build_app`). Discord uses direct dispatch via `DispatchDeps` + `build_tenant_resolver` (no HTTP). Tenant addressing is derived from the X3 harness convention (`provider_installations.installation_id = x3-{slug}-{source}`; gmail keyed by mailbox email). Live ingestion is **inline** (`services.ingestion.core.ingest`), so the live phase needs no Kafka consumer — unlike backfill, which the spine drives through the normalizer + observation_writer subprocesses. Verified: all four generators compose in one process, produce observations attributed to the seeded installs, no leaked subprocesses.

**A30.2 — Additive identity-injection + gmail install-reuse.** To make the cross-path twin a *real* collision (not a vacuous global-uniqueness check), the runner must dispatch a live event carrying the identity of a backfilled one. Three additive, backward-compatible kwargs were added (None default ⇒ unchanged auto-mint; all existing generator tests pass): Slack `simulate_message(ts=)`, GitHub `simulate_issue_event/simulate_pull_request_event(node_id=, occurred_at_iso=)`, Gmail `simulate_push(message_id=, internal_date=)`. **Mid-implementation finding:** the dedup index is `(source_channel, external_id, occurred_at)` — so a twin must match `occurred_at` too, not just the id. Slack's `ts` drives both; GitHub/Gmail therefore needed the extra timestamp kwarg (the original prompt named only the id kwargs). A second finding: `GmailPubSubGenerator` *self-seeds* its tenant+install and gmail's `external_id` embeds the install id — so identity-injection alone could not produce a gmail twin. Resolved (within the additive spirit) by making `_seed_db()` **reuse** an existing `gmail_mailbox_watches` row when one exists for the email (backfill's), so the live push shares backfill's install. Discord generator NOT modified.

**A30.3 — Cross-path dedup (load-bearing, 3-source).** `assert_cross_path_twins_dedup` engineers a real twin for Gmail + GitHub + Slack: after backfill drain, the runner captures one backfilled observation's `(external_id, occurred_at)`, replays it live via the injection kwargs, and asserts exactly one `observations` row survives (the UNIQUE index collapsed the pair). Verified green end-to-end. **Discord is excluded** — its live ids (`msg-y2-*`) and backfill ids (fixture-derived) are disjoint namespaces, so a cross-path twin is impossible by construction; Discord's per-path dedup is covered by A27.5 parity (M6.7). The inline live path dedups on `(source_channel, external_id)` (a pre-check in `core.ingest`), which makes the twin robust even when occurred_at rounding differs.

**A30.4 — Per-source-scoped auxiliary assertions.** Signature-gate (`assert_signature_validation_gate_holds_for_hmac_sources`) is scoped to **Slack + GitHub** only — Gmail's OIDC is no-op'd by Y1's design and Discord uses direct dispatch (no signature surface) (audit finding 3). Replay idempotency (`assert_live_replay_idempotency_holds`) is scoped to **Gmail + Slack + GitHub** — Discord's `LiveGatewayScenario` has no replay surface per A24 (audit finding 4). Burst presets use the actual source-specific constant names from `scenarios/live_scenarios.py` (audit finding 5). Both assertions verified in Run 1.

**A30.5 — Run 2 fault injection + A28 under composition.** `run2_fault_injection.py`: the 16-tenant shape with `FLAKY` (10% 5xx) applied to every backfill mock (propagated to the X3 subprocesses through the fixture registry's `fault_profile` field) + a deliberate **partition-missing injection** — one out-of-range (2023) `occurred_at` envelope per source driven through the REAL `observation_writer._handle_message` with a real `IdempotentProducer` publishing to the live `ingestion.dlq`. Validates A19 (no orchestrator subprocess crash under flakiness) + A28 (out-of-range → `partition_missing` DLQ, NOT crash-loop). The inline live path can't exercise A28 (it doesn't classify CheckViolationError to DLQ — writer-only), so the probe drives the writer logic directly. PARTIAL verdict is acceptable when FLAKY drops backfill counts; NOT_READY is reserved for an orchestrator crash or a missed A28 routing.

**A30.6 — Run 3 concurrency stress + #39 observability.** `run3_concurrency_stress.py`: 50 tenants (15 gmail / 15 github / 10 slack / 10 discord) through the seven shared subprocesses at concurrency=10, HAPPY_PATH, backfill-only. A concurrent monitor samples peak simultaneous `source_onboarding_runs.status='in_progress'` and peak unconsumed `workflow_signals` backlog. Asserts per-tenant isolation (exact counts gmail/github/slack; discord all-equal+positive under 5% channel-sampling → 1 of 4 channels → 30 obs), bounded signal backlog (< 10× concurrency = 100), concurrency exercised (≥5 in_progress), and the **#39 flake watch** — `tenant_onboarding_completed` fires exactly once per tenant (completion-signal distribution documented).

**A30.5 closeout — the synthetic-testability chain is complete.** Z1 → Q1-minimal → M6.7 → X3 fixes → M-Validate-spine → M-Validate-Live. The M6 ingestion pipeline is empirically validated end-to-end across all four sources for both backfill and live paths, with cross-path dedup proven for the three sources where it is substantively testable.

**Cross-references:** A18–A29 (the full substrate chain), HLD §02 L208 + L278 (the dedup contract), system-design N5. A27.5 (per-source external_id parity — the per-path dedup Discord relies on). Tickets: #44 (partition coverage — operational, queued), #45 (consumer shutdown — A29.3's rc annotations auto-green once shipped), #46 (writer failure_kind — separate work-unit), #47 (THIS work-unit — **resolved**).

---

## A29 — Composed validation-run spine: standalone runner with fixture-realism pre-flight, state reset, and consumer-rc policy

**Status:** Spine resolved on `feat/ingestion-validation-runs-spine`. The live phase + Runs 2/3 are deferred (see sub-section below).

**Context.** M6.7 + the X3 harness fixes made backfill produce observations end-to-end across all four sources. M-Validate composes that into an operator-invokable validation surface. This amendment records the **spine** — the runner skeleton plus Run 1 (clean-path E2E backfill, all four sources) — and the four new decisions that shaped it. It lives in `services/synthetic/validation_runs/` and runs as `python -m services.synthetic.validation_runs.runner --run=1`.

**Architecture.** A standalone asyncio runner (Decision 1 — not pytest; the run needs real Kafka + Postgres + moto-S3 + the 7 M6 subprocesses, which is operator infrastructure). It is phase-structured: `moto-up → state-reset → pre-flight → backfill(+drain) → assertions → report`. The backfill phase reuses the proven `BackfillHarness` (whose `_wait_for_observations_to_drain` is the Decision-4 consumer-drain step between producer completion and state collection). Run 1 = 16 tenants (4 per source, Decision 3); verified producing exact per-source observation counts (gmail 20, github 24, slack 20, discord 20) with external_id parity and zero partition-missing DLQs.

**A29.1 — Runner spawns its own moto S3 (Decision 9).** `moto_lifecycle.moto_s3()` boots a `ThreadedMotoServer` (port 5600 with ephemeral fallback), exports `S3_ENDPOINT_URL`/`S3_RAW_BUCKET` + dummy creds (inherited by the harness subprocesses via `os.environ.copy()`), creates the bucket, and tears down + restores env on exit. One command brings up everything; the operator doesn't pre-start S3. Mirrors the M6.7 `moto_s3_server` conftest fixture but as a context manager for the standalone process.

**A29.2 — State reset between runs (Decision 10).** `cleanup.reset_state()` delete+recreates the four ingestion topics (`ingestion.{raw,normalized,embedding,dlq}`) via `AIOKafkaAdminClient` and clears the moto raw bucket. Topic deletion also drops the consumer groups' committed offsets — directly defusing the cross-run offset pollution that cost hours in M6.7 (stale `ingestion.normalized` offsets shadow-moded re-read messages). Verified idempotent: back-to-back runs each start from genuinely empty topics + bucket.

**A29.3 — Consumer-rc policy (Decision 11).** The report accepts `rc ∈ {0,-9,-15}` for the two consumer services (`normalizer`, `observation_writer`) — the documented ticket #45 idle-SIGTERM gap — and treats anything else (especially `rc=1`, the partition-crash signature pre-A28) OR any non-zero rc on a framework service as a **real failure**. This tolerates the known shutdown gap without blinding the runner to a genuine crash, and **auto-greens** when ticket #45 ships (rc flips to 0, still accepted). Chosen over blanket "accept non-zero rc."

**A29.4 — Fixture-realism pre-flight (Decision 12).** `preflight.run_preflight()` is the structural defense against the M6.7-class finding (synthetic fixtures missing fields real APIs carry — gmail Message-ID, github node_id, out-of-range timestamps; hit three times in M6.7). It is **behavioral, not static**: per source it drives the *real* backfill fetcher against its mock client, runs each emitted record through the *real* handler (mirroring shard_fetch's `webhook_metadata` lift + the normalizer's blob-unwrap), and asserts (1) the handler doesn't raise, (2) `external_id` is non-null, (3) `occurred_at` falls within the live `observations` partition coverage (parsed from `pg_inherits`, so it adapts to whatever partitions exist). Fails fast before a 90-minute run on a known-bad fixture. Verified green for all four sources.

**Deferred to the M-Validate-Live work-unit (ticket #47).** This commit is intentional scope discipline, NOT partial completion — the spine is fully verified. Explicitly NOT in this commit:
- **Live-phase orchestration** — composing the four in-process live generators (`build_app` for slack/github/gmail webhooks + Discord's `DispatchDeps`/`build_tenant_resolver`) with the runner's live phase, and the live + cross-path assertions that prove a backfilled event and its live twin dedup to one row.
- **Run 2 (fault injection)** across all paths — including the *positive* partition-missing assertion (verify A28's DLQ routing fires when an out-of-range `occurred_at` is deliberately injected).
- **Run 3 (50-tenant concurrency stress)** — per-tenant isolation + bounded signal backlog under load.
The live composition is its own architectural surface (four heterogeneous in-process drivers) deserving the same verify-before-green discipline; cramming it into the spine would risk shipping unverified composition.

**Cross-references:** A22 (X3 harness — the backfill engine the spine reuses). A27 / A27.6 (M6.7 — what made backfill produce observations; A27.6's continuation documents the harness fixes the spine depends on). A28 (partition-missing DLQ — A29.4's pre-flight + the zero-partition-missing assertion guard against re-triggering it). Ticket #45 (consumer shutdown — A29.3's rc policy). Ticket #47 (M-Validate-Live — the deferred work-unit).

---

## A28 — Observation writer permanent-error classification for missing partition (DLQ instead of crash-loop)

**Status:** Resolved on `feat/ingestion-x3-harness-e2e-fixes` (commit 2). Surfaced during M6.7 verification.

**Trigger.** Running the X3 harness E2E to verification surfaced a writer crash-loop: an observation whose `occurred_at` falls outside the range-partitioned `observations` table's coverage makes `ingest_from_draft`'s INSERT raise `asyncpg.exceptions.CheckViolationError` ("no partition of relation \"observations\" found for row"). The original `observation_writer._handle_message` classification caught only `(ValidationError, HandlerNotFound, PayloadTooLarge)` as permanent; `CheckViolationError` fell through to the transient path, which re-raises so the consumer loop exits and Kafka redelivers from the last committed offset — i.e. an **indefinite crash-loop** on the first out-of-range message, blocking the partition for all subsequent messages.

**Decision.** A missing-partition routing failure is **permanent** — retrying never creates the partition. The writer now routes it to the DLQ (`writer.invariant_failure` → DB `observation_insert_error`) with an operational diagnostic and commits, rather than crash-looping.

**Detection (structural, not message-pattern).** asyncpg raises the missing-partition case as `CheckViolationError` (sqlstate `23514`) with **`constraint_name is None`** — structurally distinct from a *named* CHECK constraint violation, which carries a `constraint_name`. The writer keys off `constraint_name is None`; a named CHECK violation is re-raised (prior transient behavior preserved). `error_summary` = `"partition_missing: occurred_at=<ts> outside partition range; observations partitioning may need extension"`, and `error_context = {"reason": "partition_missing", "occurred_at": <ts>, "table": "observations"}`. New metric `writer.partition_missing`. Covered by `test_observation_writer_m5.py::test_writer_missing_partition_dlqs_not_crash_loop` (out-of-range `occurred_at` → DLQ + diagnostic, no raise, no observation written).

**Rationale.** Real backfill of historical data (e.g. a Slack workspace with 2023 messages) legitimately produces old `occurred_at`. Treating partition-missing as permanent unblocks the consumer and gives operators the actionable signal "extend partitions and reprocess from the DLQ" instead of an opaque crash-loop. Whether to *extend* partition coverage (vs. accept DLQ-routing of pre-coverage data) is an operational decision, deliberately left out of this code change.

**Cross-references:** A19 (broad exception handling — A28 narrows the missing-partition case from generic transient to specific permanent). A27 / A27.6 (M6.7 — A28 was discovered during M6.7 verification). Ticket #44 (operational decision on observations partition-coverage range — the follow-up this defers).

---

## A27 — M6 backfill producer completion: shard_fetch S3-write + RawEnvelope + channel_mapping + handler conformance + writer flag-gating

**Status:** Resolved with this commit (M6.7). Supersedes A26's "pending M6.7" status.

**Trigger:** The Q1 audit ([`q1-backfill-producer-gap-scope.md`](../decisions/q1-backfill-producer-gap-scope.md), A26) surfaced that M6 backfill orchestration completes successfully but produces **zero observations**, because four layers were unbuilt: (1) `shard_fetch` published an inline envelope the normalizer can't consume; (2) `channel_mapping` had no `backfill` entries; (3) per-source fetcher records didn't match the webhook handler input shape; (4) the `observation_writer` is flag-gated to a no-op by default. M6.7 closes all four.

**Decision (five sub-sections):**

**A27.1 — shard_fetch S3-write + RawEnvelope publish.** `ShardFetch` is now a real backfill producer. Each fetched record is written to S3 (content-addressed via `put_if_absent`, the SAME key scheme + bucket the webhook shadow path uses — see `services/ingestion/shadow_write.py`), then a `RawEnvelope(ingress_kind="backfill", raw_s3_key, content_hash)` pointer is published to `ingestion.raw`. The S3 write happens BEFORE `advance_cursor_atomic_with_kafka_publish`; the **N1 primitive's contract is unchanged** — it still receives opaque `KafkaMessage` bytes and owns the publish→flush→advance barrier. N1 extends to "S3-write → publish → flush → advance": content-addressing makes the S3 write idempotent under Kafka-retry (re-fetch → same `content_hash` → same key → no-op `put_if_absent`), so a flush failure that re-runs the page duplicates nothing. S3 failures propagate and mark the shard 'failed' per A19 (tagged "S3 raw-tier write failed" to distinguish from Kafka/cursor failures). The S3 blob wraps `{record, shard_context, webhook_metadata}` (see A27.3).

**A27.2 — channel_mapping backfill entries.** `normalizer/channel_mapping.py` gains four `(source, "backfill")` entries resolving to the SAME handler channel as the source's live surface: gmail→`gmail:`, github→`github:webhook`, slack→`slack:message`, discord→`discord:message`. Note Discord: the live surface for *messages* is the Gateway (IN-12, `discord:message`), NOT the interaction webhook (`discord:interaction`) — backfill matches the Gateway. Gmail's Pub/Sub *notification* ingress stays unmapped (it carries no message resource); backfill points at the canonical `gmail:` handler that consumes fetched message resources.

**A27.3 — per-source fetcher record-shape conformance.** The M6.3–M6.6 fetchers now emit records in the shape the webhook/gateway handler expects, NOT the old wrapper (`read_path`, `event_type`, per-source nesting are gone):
  - **GitHub:** the REST list item is reshaped into the webhook event body `{action, issue|pull_request: <item>, repository, sender}`; the event type travels as `webhook_metadata = {"X-GitHub-Event": "issues"|"pull_request"}`.
  - **Slack:** emitted as the `event_callback` shape with `channel` injected into the event (`conversations.history` messages omit it).
  - **Discord:** emitted as the MESSAGE_CREATE shape with `guild_id` injected (REST message objects omit it).
  - **Gmail:** already conformant except `read_path`, which the handler validates ∈ {push, poll}; backfill + gap conform as `"poll"`.

  The producer (A27.1) lifts the reserved `webhook_metadata` key out of the record into the blob; the **normalizer** (`worker.py`) — for `ingress_kind="backfill"` only — unwraps `blob["record"]` as the handler payload and replays `blob["webhook_metadata"]` as the handler's `headers`. Live ingress is untouched (bare body, `headers={}`). The webhook path is NOT modified — fetchers conform to handlers, never the reverse.

**A27.4 — observation_writer flag-gating + harness wiring.** The X3 harness (`services/synthetic/backfill_harness/harness.py`) writes `tenant_flags.ingestion.kafka_path_enabled=TRUE` per tenant at setup (so `observation_writer` writes instead of shadow-logging a no-op), spawns **7** subprocesses (the 5 M6 framework services + `normalizer` + `observation_writer`), wires `S3_ENDPOINT_URL`/`S3_RAW_BUCKET`/`INGESTION_ENV` into the producer + normalizer, and creates the moto raw bucket at setup (idempotent; no-op when `S3_ENDPOINT_URL` is unset). The two new subprocesses import the `_HELPER_TEMPLATE` like the others; they don't use its monkeypatched fetcher/reconciler factories, so no broken-import cascade.

**A27.5 — external_id parity as the load-bearing constraint.** Per HLD §02 L278, a webhook event and a backfill of the same event MUST derive the identical `external_id` so the `observations UNIQUE(source_channel, external_id, occurred_at)` index dedups them to one row. Verified per source by `services/ingestion/normalizer/tests/test_backfill_external_id_parity.py::test_backfill_record_produces_same_external_id_as_webhook_<source>`, which runs the SAME logical event through the canonical handler (webhook side) and the REAL normalizer (backfill side) and asserts equal external_id: gmail `gmail:{install}:{message_id}`, github `node_id`, slack `{channel}:{ts}`, discord `discord:{snowflake}`. This is the load-bearing test; without it, parity is a hope rather than a property. All four pass.

**A27.6 — `moto[server]` test dependency for the S3-required `shard_fetch`.** Decision 1.3 makes `shard_fetch`'s subprocess entrypoint hard-require S3 at startup. That entrypoint is shared by the M6.1/M6.2 OAuth→completion subprocess E2E tests (`test_oauth_to_{github,slack,discord,gmail,source,tenant}_completion*` + the gmail/tenant reshare variants) and the M6.2a `test_shard_fetch_subprocess.py` resume test, which spawn the real `shard_fetch` process. M6.7 adds **`moto[s3,server]` + `flask`** to the `dev` test dependencies to provide a real fake-S3 HTTP endpoint, and a shared session-scoped fixture `moto_s3_server` (in `services/ingestion/workflows/tests/conftest.py`) that boots moto's server, exports `S3_ENDPOINT_URL` / `S3_RAW_BUCKET` + dummy AWS creds into the environment, and creates the bucket. The spawned subprocesses inherit those env vars via `os.environ.copy()`; tests opt in with `pytest.mark.usefixtures("moto_s3_server")`. **No prior-milestone test logic changed** — the record-producing fetchers are used unchanged, so these tests still exercise the full producer→S3→normalizer→writer path with real records. This was chosen over a skip-on-missing-infra pattern (which would have silently dropped the 3 reshare tests' coverage in S3-less environments — those reshare reconcilers read a record-derived cursor watermark, so empty records would starve gap detection). The in-process `shard_fetch` unit tests (`test_shard_fetch.py`, `test_gmail_n1_invariant.py`, `test_shard_fetch_backfill_producer.py`) keep using an injected in-memory `FakeS3Client` (no server needed). The earlier in-process `moto.mock_aws` is incompatible with `aiobotocore`; a real moto HTTP server is not (it's plain HTTP), verified by an `S3Client` put/get roundtrip against it.

**A27.6 continuation — running the X3 harness E2E to verification (`feat/ingestion-x3-harness-e2e-fixes`).** When the substrate-gated X3 harness E2E was first run live against real Kafka + moto S3, it produced zero observations. Root-causing surfaced five orthogonal gaps — none in the M6.7 producer/normalizer/writer production code (verified correct in isolation: shard_fetch writes content-addressed blobs; the normalizer transform + writer full-mode write each succeed on real blobs per source). Three were harness scaffolding, two were fixture realism, one was a pre-existing production gap surfaced (finding #4 below):

- **Harness scaffolding (test-only):** (1) the harness collected the `observations` table immediately after producer-side completion (`tenant_onboarding_completed`) without waiting for the asynchronous normalizer→writer Kafka chain to drain — added `_wait_for_observations_to_drain` (30s budget, 2s poll, returns on per-tenant target or timeout); (2) the install writer never seeded `gmail_mailbox_watches`, so the gmail planner emitted zero shards — now seeds one active mailbox watch; (3) the helper's `_build_source_client` omitted discord, but `plan_shards_discord` requires a client — now wired (parallel to github/slack).
- **Fixture realism (test-only):** synthetic fixtures omitted fields that real provider responses always carry, so handlers/writer correctly rejected them. (1) slack/gmail base timestamps were 2023 (`1_700_000_000`), outside the `observations` partition coverage (2025-01→2027-01) — moved to 2026-01; (2) the gmail fixture lacked the `Message-ID` header the `gmail:` handler requires; (3) the github fixture lacked `node_id`, from which the `github:webhook` handler derives `external_id` (the dedup key). All three are the same class — completing fixtures to match real-API shape, not test-fudging.

With these six fixes, four of five E2E tests pass: `test_harness_single_tenant_gmail_completes`, `..._per_source_produces_observations`, `..._backfill_to_observation_chain`, `..._parallel_4_tenants_mixed_sources` — all four sources produce observations on the correct channels with external_id parity, zero leaked subprocesses, drain completing in seconds.

**Finding #4 (pre-existing production gap surfaced, NOT introduced):** `test_harness_sigterm_cleanly_stops_all_seven` stays RED — normalizer + observation_writer don't honor SIGTERM cleanly when idle. The normalizer's `async for msg in consumer` blocks with no timeout, so its SIGTERM-set stop flag is never observed at teardown (→ SIGKILL, rc=-9); the writer installs no handler (→ default-kill, rc=-15). The 5 framework services exit rc=0 via `LongRunningService`'s `stop_event`-polled loop. Pre-existing — surfaced because M6.7's harness teardown is the first SIGTERM exercise on these consumers in a live subprocess test. Filed as ticket #45; the failing test is the visible regression-prevention surface until that work-unit ships (DO NOT suppress or `@xfail` — same discipline as Q1-minimal's observation-count assertion before M6.7 closed it).

**Cross-references:** A12/A15/A16 (M6.0 substrate — **unchanged**; the S3 write precedes the cursor-advance primitive). A18 (per-source backfill — A27 completes it: the fetchers now emit handler-conformant records). A19 (broad exception handling — covers S3 failures). A22 (X3 harness — A27 makes its E2E genuinely produce observations). A26 (the gap this resolves). A28 (writer permanent-error classification for missing partition — also surfaced during this verification). HLD §02 L208 + L278. System-design N5 (one normalizer consumes webhook + gateway + backfill). Ticket #43. Ticket #44 (observations partition coverage). Ticket #45 (consumer graceful shutdown).

**What is NOT done:** real per-source production clients (M6.3–M6.6 still use mocks); mega-prompt 5's composed validation runs; staging dry-run; customer pilot.

---

## A26 — M6 backfill producer gap: shard_fetch envelope doesn't conform to the RawEnvelope contract; observation production never wired

**Status:** **Resolved by A27 (M6.7).** (Originally: documented; resolution pending M6.7 / ticket #43.)

**Trigger:** Pre-implementation substrate audit for mega-prompt 5 (the all-sources backfill+live validation runs). Smoke-testing the X3 harness — the foundation those runs build on — surfaced that **M6 backfill has never produced an observation end-to-end in any test or environment.** The backfill orchestration (plan → fetch → cursor-advance → completion signal) completes and reports success, but the final hop to the `observations` table is unbuilt. Full enumeration in [`docs/decisions/q1-backfill-producer-gap-scope.md`](../decisions/q1-backfill-producer-gap-scope.md).

**Root cause (four layers):**

1. **Harness collection bug.** `services/synthetic/backfill_harness/harness.py` `_collect_state` selected `observations.observed_at`; the column is `occurred_at` (`db/migrations/0001_foundation.sql:68`). `harness.run()` always raised after the wait phase, so the E2E path never reached its assertions. **Fixed in this commit (Q1-minimal).**

2. **Producer/consumer envelope mismatch.** `shard_fetch._build_kafka_message` publishes an **inline** envelope `{tenant_id, source, shard_id, record}` with no S3 write; `advance_cursor_atomic_with_kafka_publish` ships those bytes verbatim. The normalizer (`normalizer/worker.py:370`) requires a `RawEnvelope` S3-pointer (`raw_s3_key`, `extra="forbid"`) and drops the inline shape. This contradicts the documented design (HLD §02 L208: "write the raw response to S3 … publish a tiny pointer envelope"; system-design N5: "one normalizer pool consumes all three"). **M6.7.**

3. **No backfill normalization path.** `normalizer/channel_mapping.py` has no `(*, "backfill")` entries → `resolve_channel(source, "backfill")` returns `None` → envelope dropped as `unsupported_combination`. And the per-source fetcher records are wrapper-shaped (tagged `read_path:"backfill"`) that don't match the handlers the normalizer dispatches to — GitHub reads `X-GitHub-Event` from headers the normalizer passes empty; Slack/Discord wrap the message; Gmail is likely the only conformant source. **M6.7.** (The high-risk layer: any reshape must preserve `external_id` derivation so a webhook event and the same backfilled event dedup to one observation — HLD §02 L278.)

4. **Writer flag-gated.** `writers/observation_writer.py` no-ops unless `ingestion.kafka_path_enabled=TRUE` for the tenant (default FALSE). The harness never sets it. **M6.7** (harness-side flag wiring).

**Decision (this commit — Q1-minimal):**

- Fix layer 1 (column name) so `harness.run()` returns.
- Add `assert_observation_count_matches_fixture` to the X3 E2E test (`test_harness_single_tenant_gmail_completes`). It is **EXPECTED TO FAIL** until M6.7 ships — the failing assertion is the regression-prevention surface, converting a silent invariant violation into a visible, tracked failure. The assertion call site and the test docstring both instruct future contributors NOT to suppress/xfail/skip it.
- Defer layers 2 + 3 + writer-flag to the M6.7 backfill-producer work-unit (ticket #43), per the scope document's recommendation (framework-scope, ~3–4 sessions, high-risk handler-conformance layer — folding it into a harness hardening task would silently expand the work-unit).

**Why this is the right split:** layers 2–3 are M6 *framework* changes touching the N1 hot path and per-source observation derivation. Q1's mandate was additive harness hardening. Shipping the failing assertion now (a) makes the gap impossible to miss and (b) gives M6.7 a green-when-done acceptance signal, without smuggling framework scope into a hardening commit.

**Harness hardening done in Q1-minimal (NOT M6.7 scope):** Q1-minimal also fixed **two latent harness bugs** that were invisible because the X3 E2E path had never executed end-to-end (gated off by default; timed out under pytest's 30s limit before reaching state collection):

1. `_collect_state` selected the non-existent column `observed_at` (actual: `occurred_at`) — `harness.run()` always raised after the wait phase.
2. The completion-wait constants watched the wrong inbox: `(tenant_onboarding, tenant_onboarding)` instead of `(bridge, bridge)`. Production emits `tenant_onboarding_completed` to the Bridge inbox (`tenant_onboarding.py:160-161,508-509`), which the harness's own docstring already documents ("Bridge inbox") — the constants contradicted it, so `_wait_for_completions` never observed completion and `assert_all_complete` failed first with a misleading "M6 chain stalled" message.

These are pure harness hardening (harness-internal constants/queries that never ran), not part of M6.7's framework scope. Fixing them is what lets the new `assert_observation_count_matches_fixture` assertion surface *accurately* (failing on "0 observations, expected N") rather than being masked by an earlier, misleading failure.

**Cross-references:** A12 / A15 / A16 (M6.0 substrate — **not affected**; the recommended M6.7 fix preserves the cursor-advance primitive's contract, doing the S3 write before it). A22 (X3 harness — A26 corrects its implicit "harness produces observations" claim; A22's `assert_observation_count_matches_fixture` existed but was never wired into the E2E test). HLD §02 L208 (the S3-pointer design `shard_fetch` must conform to). System-design N5 (one normalizer consumes webhook + gateway + backfill). Ticket #43 (M6.7 work-unit). [`q1-backfill-producer-gap-scope.md`](../decisions/q1-backfill-producer-gap-scope.md) (full per-file scope + risk + effort).

**Resolution path:** M6.7 ships the four-layer fix (shard_fetch S3+RawEnvelope, channel_mapping backfill entries, per-source handler conformance, writer-flag harness wiring + the normalizer/writer subprocess co-spawn). When it lands, the E2E assertion goes green and mega-prompt 5's backfill validation can proceed unchanged.

---

## A25 — Slack + GitHub webhook synthetic drivers: FastAPI in-process invocation with tenant-targeted webhook dispatch

**Status:** Resolved with this commit (covers both `feat/ingestion-z1-slack-webhook-generator` and `feat/ingestion-z1-github-webhook-generator`).

**Trigger:** Mega-prompt 4 Z1 work-units. The Path I validation audit (post-mega-prompt-3) surfaced that Slack + GitHub *live* ingestion lacked tenant-targeted synthetic drivers: `services/synthetic/cutover_load.py` (M-Load) is an HTTP throughput load tester that POSTs to a running webhook server with a *random, deterministic-Zipf* tenant pool whose `team_id` / `installation.id` values resolve to **no** installed tenant (the router returns `UnknownInstallation → 401`), so it produces zero observations for known tenants. The X3 backfill harness (A22) + Y1 Gmail Pub/Sub (A23) + Y2 Discord Gateway (A24) covered backfill-everywhere plus Gmail/Discord live, leaving Slack/GitHub live ingestion as the only synthetic-coverage gap. Closes ticket #42.

**Decision:** Two in-process Python drivers — `SlackWebhookGenerator` (`services/synthetic/live_generators/slack_webhook.py`) and `GithubWebhookGenerator` (`services/synthetic/live_generators/github_webhook.py`) — dispatch webhooks via `httpx.AsyncClient(transport=ASGITransport(app=fastapi_app))` to `/webhooks/slack/events` and `/webhooks/github/events`. The app is the real gateway app (`services.gateway.main.build_app`), so the full path is exercised: body-size precheck → signature verification → tenant resolution → (GitHub) replay-cache + `selected_repositories` filter → inline `ingest()` → observation write.

Both drivers share an architecture:

1. **Tenant-targeting (Z1.2).** Drivers target a *seeded* `provider_installations` row — they do NOT create installs (parallels Y2's reliance on pre-seeded installs). Slack resolves by `installation_id = team_id`; GitHub resolves by `installation.id`. The caller seeds the install; the production resolver maps the webhook to the real tenant.
2. **Real signatures (Z1.1).** Slack uses the `v0` HMAC-SHA256 scheme (`v0:{ts}:{body}`); GitHub uses `sha256=` HMAC-SHA256 over the body. Drivers sign with the same secret the app is configured with (Slack: `WEBHOOK_SECRET_SLACK` + `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`; GitHub: App-level `WEBHOOK_SECRET_GITHUB`). A `tamper_signature` flag produces a deliberately-wrong signature for negative tests (expect 401, no observation).
3. **Mock coordination (Z1.3).** Each dispatched event is first appended to the X2 mock's fixture (`MockSlackClient` channel `messages`; `MockGithubClient` `repos[].events_by_type`) so a subsequent backfill/reconciler probe against the mock sees it. The mock libraries are **not modified** — the drivers write the fixture data structures the mocks already expose (Z1 only adds new driver modules; it does not touch the X2 mocks the way Y1/Y2 did).
4. **Burst patterns + replay (Z1.4 / Z1.5).** `LiveSlackScenario` / `LiveGithubScenario` carry per-tenant `[(delay_ms, count), ...]` patterns with `STEADY_STATE` / `BURSTY` / `MIXED` presets (suffixed `_SLACK` / `_GITHUB` to avoid collision with the existing `_PUBSUB` Gmail presets). `replay_probability` re-delivers an event to test at-least-once idempotency.

**Provider-specific replay/dedup semantics (substrate-documented):**

- **Slack** has no router-level replay cache. Replays reuse the message's `ts`; the `slack:message` handler derives `external_id = f"{channel}:{ts}"`, so the observation-layer `(source_channel, external_id, occurred_at)` UNIQUE constraint dedups the redelivery. Net: no double-count.
- **GitHub** *does* have a router-level replay cache (`build_app` wires `app.state.github_replay_cache`). A redelivery with the same `(installation_id, X-GitHub-Delivery)` is dropped at the router with HTTP 200 `handled:replay` *before* ingest. Drivers reuse the original delivery id + `node_id` on replay, so the redelivery is caught by the cache; were the cache absent, the `external_id = node_id` observation dedup is the backstop. Net: no double-count via either layer.

**Fault-profile inertness (documented limitation):** the webhook *ingest* path does not call the Slack/GitHub Web API — it ingests the webhook payload directly. A `RATE_LIMITED` / `FLAKY` mock fault profile is therefore inert for webhook dispatch; the drivers accept a `fault_profile` on the scenario for signature parity with the Pub/Sub / Gateway scenarios, and the `*_fault_profile_rate_limit` test asserts the webhook still succeeds (documenting the independence). Fault behaviour for these sources is exercised on the *backfill* path via the X3 harness (A22), where the mock API *is* called.

**Rationale:** ASGI in-process invocation exercises signature validation + tenant resolution + dispatch + Kafka/inline path without port binding (inherits the Y1 pattern, A23). Tenant-targeting enables backfill-then-live composition (X3 installs + Z1 drives live for the same tenant → full lifecycle in shared observation state; inherits the Y2 pattern, A24). Both drivers share enough architectural shape that one amendment captures both.

**Cross-references:** A20 (F4 retrofit — drivers target installs the callbacks create), A21 (X2 mock servers used by drivers for state coordination), A22 (X3 harness composes with Z1), A23 (Y1 — Z1 inherits the in-process ASGI pattern), A24 (Y2 — Z1 inherits the seeded-install tenant-targeting pattern), ticket #42 (resolved here).

**Effect on future work:** the synthetic-coverage trilogy is now complete — X3 backfill (all four sources) + Y1 Gmail Pub/Sub + Y2 Discord Gateway + Z1 Slack/GitHub webhooks. All four sources are testable end-to-end for both backfill and live paths with synthetic data targeting specific seeded tenants. Validation runs composing all four sources × (backfill + live) can now be authored without a coverage gap (mega-prompt 5 territory).

---

## A24 — Discord Gateway synthetic generator: in-process event injection without WebSocket simulation

**Status:** Resolved with Y2 commit on `feat/ingestion-y2-discord-gateway-generator`.

**Trigger:** Mega-prompt 3 Y2 work-unit. Completes the synthetic-coverage trilogy (M-Load webhooks + X3 backfill + Y1 Gmail Pub/Sub + Y2 Discord Gateway). M4's Gateway client tests already cover the WebSocket protocol layer (HELLO / IDENTIFY / heartbeat / RESUME); Y2's scope is the **dispatch and ingestion** layer — what the protocol layer delivers to the rest of the system.

**Decision:** `DiscordGatewayGenerator` (in `services/synthetic/live_generators/discord_gateway.py`) invokes `services.integrations.discord.gateway.dispatch.handle_message_create` directly in-process. The handler is already directly callable per its production signature:

```python
async def handle_message_create(message: dict[str, Any], deps: DispatchDeps) -> None
```

The generator:

1. Holds a `DispatchDeps` reference (built the same way M4 tests build it — `pool`, `tenant_resolver`, `actor_repo`, `alias_repo`, `embedder=None`, `application_id`).
2. Holds a `GuildBinding` map keyed by `guild_id`, each containing a `MockDiscordClient`.
3. Per `simulate_message_create(guild_id, channel_id, content, author_id)`: builds the Discord MESSAGE_CREATE payload shape, calls `MockDiscordClient.append_message` (new method added by Y2), then awaits `handle_message_create(payload, deps)`. Returns a `SimulatedEventResult` carrying handler outcome.

**Explicit non-coverage (Y2.3):**

The generator does NOT exercise:

- WebSocket framing.
- HELLO (op 10) heartbeat-interval negotiation.
- IDENTIFY (op 2) + READY (op 0, t=READY) handshake.
- Heartbeat protocol (op 1 send / op 11 ACK).
- Session resume (op 6 + sequence numbers).
- INVALID_SESSION (op 9) fallback.
- Connection / reconnection / disconnect lifecycle.

These remain M4-tested-only in `services/integrations/discord/gateway/tests/test_client_lifecycle.py` and `test_client_reconnect.py`. The runnable enforcement: `test_gateway_generator_does_not_simulate_connection_lifecycle` asserts the generator's source does not import `websockets` or the Gateway `client.py` module. A future contributor adding lifecycle simulation must write A25 before changing this test.

**Trade-off acknowledged:** Y2 does NOT test connection/reconnection/session-resumption scenarios. If lifecycle synthetic coverage is ever required, a future work-unit ships a WebSocket simulator (Option A from the mega-prompt's decision matrix) at that point. Until then, real-Discord-monitoring + M4's existing test suite covers that surface.

**MESSAGE_UPDATE / MESSAGE_DELETE handling:** Production has no v1 handler for these events (see `services/integrations/discord/gateway/dispatch.py` line 93–95: "Other events (MESSAGE_UPDATE, MESSAGE_DELETE, TYPING_START, …) are not in v1 scope"). The generator's `simulate_message_update` and `simulate_message_delete` methods are **runnable documentation** of this non-coverage: they return a `SimulatedEventResult` with `handler_invoked=False` and a `notes` field citing A24. If MESSAGE_UPDATE/DELETE ever ships in v2, those methods can be promoted to real handler invocations without changing the API surface.

**Rationale (Option B vs A vs C — settled):**

- **Option A (full WebSocket simulator)** rejected: implementing the Gateway protocol fidelitously is ~3-5 sessions of work for dubious additional coverage. The M4 client tests already exercise this surface against a `FakeGateway` server.
- **Option C (library-level stubbing of the websockets module)** rejected: asyncio synchronization fragility against websockets library upgrades. Tests would become brittle and hard to debug.
- **Option B (direct handler invocation)** chosen: validates event processing — what backfill + live ingestion correctness depend on — without re-litigating protocol fidelity.

**Coverage end-to-end:**

| Layer | Exercised? |
|-------|-----------|
| WebSocket frame parsing | ✗ (M4-tested, not Y2) |
| HELLO / IDENTIFY / READY handshake | ✗ (M4-tested, not Y2) |
| Heartbeat protocol | ✗ (M4-tested, not Y2) |
| Session resume / sequence numbers | ✗ (M4-tested, not Y2) |
| MESSAGE_CREATE dispatch (bot/webhook filters) | ✓ real `handle_message_create` |
| Tenant resolution via `provider_installations` | ✓ real `tenant_resolver` |
| Discord ingest core | ✓ real `ingest()` |
| Observation write + `(source_channel, external_id, occurred_at)` dedup | ✓ real |
| MESSAGE_UPDATE / DELETE | ✗ documented v1 non-coverage |

**Tests (9 in Y2):**

- `test_gateway_generator_basic_event_processed` — MESSAGE_CREATE → observation row.
- `test_gateway_generator_coordinates_mock_discord_state` — append precedes handler call.
- `test_gateway_generator_message_update_event_documents_noncoverage` — runnable A24 documentation.
- `test_gateway_generator_message_delete_event_documents_noncoverage` — runnable A24 documentation.
- `test_gateway_generator_multi_channel_scenario` — 3 channels × 2 messages = 6 observations.
- `test_gateway_generator_high_volume_burst` — 30 events serialized through one channel.
- `test_gateway_generator_fault_profile_transient_failure` — documents that mock-FaultProfile is a backfill-path concern, not a dispatch-path concern (dispatch doesn't call the mock's async surface).
- `test_gateway_generator_composable_with_x3_seeding` — prior backfill-style obs + live event coexist.
- `test_gateway_generator_does_not_simulate_connection_lifecycle` — structural enforcement: no `import websockets`, no `gateway.client` import. The test will break if anyone adds lifecycle simulation; they should write A25 before changing it.

**MockDiscordClient extension (Y2 substrate addition):** Added `append_message(channel_id, message)` mirroring Y1's `MockGmailClient.append_messages` shape. Additive; existing backfill tests unaffected.

**Cross-references:**

- [A23](#a23--gmail-pubsub-synthetic-generator-fastapi-in-process-invocation-with-mock-gmail-coordination) — Gmail Pub/Sub generator; Y2 inherits the in-process invocation pattern, just with direct handler call instead of HTTP.
- [A21](#a21--mock-api-server-architecture-stateful-in-process-libraries-with-fixture-generators-and-fault-injection) — mock servers; Y2 uses `MockDiscordClient` and adds the `append_message` extension.
- [A19](#a19--framework-exception-handling-for-per-source-dispatch-failures) — framework exception handling; the dispatch handler's existing tenant-resolution + ingest error handling embodies the same robustness contract.
- M4 (Discord Gateway) — Y2 exercises its event handlers but explicitly NOT its protocol layer.

**Effect on future work:** Mega-prompt 3 closes the synthetic-coverage trilogy. The system now has webhook synthetics (M-Load), backfill synthetics (X3), Gmail live synthetics (Y1), and Discord live synthetics (Y2) — **every code path in the M6 ingestion pipeline is testable with synthetic data**. Composition patterns: install + backfill + live ingestion can be exercised in one test scenario (see `docs/ingestion/synthetic-testing-guide.md` §9).

---

## A23 — Gmail Pub/Sub synthetic generator: FastAPI in-process invocation with mock Gmail coordination

**Status:** Resolved with Y1 commit on `feat/ingestion-y1-gmail-pubsub-generator`.

**Trigger:** Mega-prompt 3 Y1 work-unit. M-Load covered webhook synthetic traffic (Slack + GitHub); X3 covered backfill across all four sources. The remaining gap — live-ingestion paths — starts here with Gmail Pub/Sub.

**Decision:** `GmailPubSubGenerator` (in `services/synthetic/live_generators/gmail_pubsub.py`) drives the Gmail Pub/Sub live-ingestion path end-to-end in-process via `httpx.AsyncClient(transport=ASGITransport(app=fastapi_app))`. The generator:

1. Seeds `tenants`, `gmail_installations`, `gmail_mailbox_watches`, and `gmail_pubsub_topics` rows for each registered mailbox — the production handler's tenant resolution path reads from these tables.
2. Monkeypatches `verify_pubsub_oidc_token` to a no-op (proven test pattern from the M2.2 shadow-write tests at `services/integrations/gmail/tests/test_pubsub_shadow.py`).
3. Monkeypatches `services.integrations.gmail.push_handler._drain_history` to call the real `drain_mailbox_history` with the X2 `MockGmailClient` instead of constructing a real `GoogleHttpClient` + `GmailClient`.
4. Points `lib.shared.db._pool` at the test pool so `tenant_transaction()` (used downstream by `dispatch_gmail_message_resource`) resolves to the test database.
5. Per `simulate_push(mailbox_email, new_messages=N)`: appends N new messages to the X2 mock via `MockGmailClient.append_messages` (new method shipped with Y1), advances the mock's `current_history_id`, builds a standard Pub/Sub envelope with the new historyId, POSTs to `/webhooks/gmail/pubsub`.

The patches are installed in `__aenter__` and unwound in `__aexit__`; the generator is an async context manager.

**Scenarios** (in `services/synthetic/scenarios/live_scenarios.py`):
- `LivePubSubScenario(tenants=[PerTenantBurst(...), ...], replay_probability=0.0, fault_profile=HAPPY_PATH)`.
- `PerTenantBurst(tenant_slug, mailbox_email, burst_pattern=[(delay_ms, msg_count), ...])`.
- Presets: `STEADY_STATE_PUBSUB` (1 msg/s × 10), `BURSTY_PUBSUB` (50 in burst + 30s idle), `MIXED_PUBSUB` (5 tenants × varied patterns).

**Coverage end-to-end:**
| Layer | Exercised? |
|-------|-----------|
| FastAPI routing | ✓ via ASGITransport |
| OIDC envelope validation surface | ✓ (test-mode no-op'd; same pattern as existing tests) |
| Pub/Sub envelope decoding | ✓ real `decode_pubsub_message` |
| `gmail_pubsub_topics` tenant resolution | ✓ generator seeds; handler queries |
| `handle_push` rate-limit + Google-error branches | ✓ exposed via X2 FaultProfile presets |
| `drain_mailbox_history` page-by-page logic | ✓ real (the meaty path) |
| `dispatch_gmail_message_resource` thread canonicalization | ✓ real |
| Observation table write + dedup on `external_id` | ✓ real |
| DWD token minting | ✗ bypassed (not part of M6 chain logic) |
| Real Google httpx client | ✗ replaced by MockGmailClient |
| Real OIDC cert fetch | ✗ replaced by no-op verifier |

**Replay simulation:** `LivePubSubScenario.replay_probability` ∈ [0.0, 1.0]; the generator's RNG (seeded for determinism) duplicates a fraction of pushes (same historyId, same payload). Tests verify the writer's `external_id` UNIQUE constraint dedupes them — observation count tracks unique messages, not push count.

**Rationale:**

- **In-process ASGI** matches X3 harness pattern (no port management, deterministic teardown, no flaky timing).
- **Direct mock coordination** (generator holds a reference to MockGmailClient and appends to it directly) eliminates indirection that wouldn't exist in test code anyway.
- **Burst patterns** model real-world Gmail traffic shape better than uniform sequential/parallel — operators can encode "this customer gets bursts of activity then idles" scenarios for soak testing.
- **Patching `_drain_history` rather than `GmailClient`** keeps the real `drain_mailbox_history` page-by-page logic in the test path; that's the meaty Gmail-specific code that we want exercised.

**Cross-references:**

- [A18](#a18--per-source-backfill-is-net-new-code-framework--existing-steady-state-coexist-until-m7) — per-source backfill; live-ingestion path is the steady-state side of that coexistence.
- [A19](#a19--framework-exception-handling-for-per-source-dispatch-failures) — framework exception handling; the handler's existing rate-limit + Google-error branches embody the same robustness contract.
- [A20](#a20--f4-oauth-retrofit-all-callbacks-write-onboarding_triggers-atomically-with-install) — F4 retrofit; the generator composes with X3 (install via F4 retrofit, then drive live notifications).
- [A21](#a21--mock-api-server-architecture-stateful-in-process-libraries-with-fixture-generators-and-fault-injection) — mock servers; Y1 uses `MockGmailClient` and adds the `append_messages` extension.
- [A22](#a22--backfill-synthetic-harness-oauth-callback-driven-install-simulation-with-parallel-concurrency-and-properties-based-assertions) — backfill harness; Y1 inherits the in-process invocation pattern.

**Effect on future work:** Y2 (Discord Gateway) inherits the in-process invocation pattern (different protocol — direct event-handler call, not HTTP). Future live-ingestion paths follow same shape. Composition example: install a tenant via X3, run backfill, then drive ongoing live notifications via Y1 — verifies that backfill observations + live observations coexist in the same `observations` table without duplicate-key violations.

---

## A22 — Backfill synthetic harness: OAuth-callback-driven install simulation with parallel concurrency and properties-based assertions

**Status:** Resolved with X3 commit on `feat/ingestion-x3-backfill-harness`.

**Trigger:** Mega-prompt 2 X3 work-unit. The M6 framework requires end-to-end synthetic testing across all four sources for single-tenant + concurrent-tenant scenarios. M6.3-M6.6 5-subprocess E2E tests cover the single-tenant case per source × clean+reshare; X3 extends to multi-tenant under fault profiles.

**Decision:**

The `BackfillHarness` orchestrator (in `services/synthetic/backfill_harness/`) operates in three phases against a real Postgres + a real Kafka broker:

1. **Phase A — Setup.** For each `BackfillScenario`: seed a `tenants` row, build a fixture via the X2 generators, write a per-run helper module to a temp directory, and write a fixture registry JSON file the helper will read at subprocess startup.
2. **Phase B — Run.** Invoke the install + `onboarding_triggers` write (atomic per A20) for each scenario directly via the test pool, bounded by `concurrency`. Spawn FIVE shared subprocesses (one each of oauth_poller, tenant_onboarding, source_onboarding, shard_fetch, reconciler) via `python -c "import <helper>; from <svc> import main; main()"` — the helper module registers fixture-aware mock-client factories at import. Concurrently poll for each tenant's `tenant_onboarding_completed` signal in the Bridge inbox.
3. **Phase C — Teardown.** SIGTERM all subprocesses; assert rc=0 within 15s. Collect observations, completion-signal counts, cursor-history snapshots, and reconciliation pass counts into per-tenant `TenantOutcome` records.

**Concurrency model:** Five **shared** subprocesses serve all tenants (not 5N). Per the X3 audit, the M6 services are tenant-agnostic at the claim layer — they claim signals across all tenants. Subprocess startup is the dominant per-run cost (~50ms × 5), so per-tenant subprocess isolation would scale poorly with N. The harness exposes `concurrency` as the bound on in-flight install + completion-polling work, not as a subprocess multiplier.

**Install simulation depth:** The harness writes install + onboarding_triggers rows **directly** via the test pool (with the same atomic-transaction shape as the production callbacks; see A20). It does NOT drive the full HTTP OAuth callback stack. The X3 contract is "exercise the M6 chain from `onboarding_triggers` onward"; A20's invariants (atomic install+trigger, idempotency via partial unique index) are verified by the X1 retrofit tests independently. X3 trusts those tests and skips the OAuth-layer plumbing — fewer moving parts (no FastAPI app construction, no respx HTTP mocks, no state-token issuance) for a higher-leverage test surface.

**Per-tenant mock dispatch:** The helper module reads `X3_FIXTURE_REGISTRY_PATH` at subprocess startup, loads the per-tenant fixture registry into memory, and installs fixture-aware `_open_<source>_client` factories. Each factory looks up the install row's `tenant_id` in the registry, constructs the appropriate mock client with the tenant's fixture + fault profile, and returns it via the standard `(client, close)` tuple. The same single helper module serves all five subprocesses; each subprocess imports it via `python -c` before invoking `main()`.

**Properties-based assertions** (`services/synthetic/backfill_harness/assertions.py`):
- `assert_all_complete` — every tenant reached completion.
- `assert_no_duplicate_observations` — per tenant, no duplicate `external_id` values in `observations`.
- `assert_cursor_monotonic_per_shard` — cursor `pages_fetched` advances monotonically.
- `assert_completion_emitted_per_tenant` — exactly one `tenant_onboarding_completed` signal per tenant.
- `assert_observation_count_matches_fixture` — `len(observations) == expected` (with `tolerance` for sources with sampling, e.g., Discord 5%).
- `assert_reshare_cycles_completed` — when a scenario triggers reshare, `reconciliation_pass_count >= 1`.

These assertions verify framework guarantees rather than exact fixture data; the harness is robust to fixture-generator evolution.

**Rationale:**

- **In-process install** exercises the F4 retrofit's atomicity + idempotency without HTTP-layer noise. Production OAuth callbacks have their own tests (X1); X3 tests what they enable downstream.
- **Shared subprocess set** matches production architecture (the M6 services are per-cluster, not per-tenant). Per-tenant subprocess isolation would test a model that production doesn't use.
- **JSON-file fixture registry** is the simplest cross-process state-sharing mechanism. Picking a database-backed registry would add coordination overhead; environment-variable encoding is too size-constrained for realistic fixtures. The helper module reads the file once at import and keeps it in memory for the subprocess lifetime.
- **Properties-based assertions** are robust to fixture evolution. Exact-record-match assertions would break every time a fixture generator added a field; property checks survive that churn.

**Tests:**

- `test_scenarios.py` (5) — `BackfillScenario` validation + dataclass shape.
- `test_assertions.py` (15) — every assertion with deliberately-violating + clean fixtures, plus the `tolerance` behavior and the `expected=0` skip path.
- `test_harness_unit.py` (5) — install+trigger writes per source (gmail, slack), idempotency on retry, helper-module generation + importability, registry JSON correctness. Marked `pytest.mark.integration` (DB-required).
- `test_harness_e2e.py` (2) — full 5-subprocess single-tenant Gmail + 4-tenant mixed-sources runs. **Default-skipped**; opt-in via `X3_HARNESS_E2E=1` + real `KAFKA_BOOTSTRAP_SERVERS`. Same shape as M-Load's `tests/load/test_cutover_dryrun.py`.

**Cross-references:**

- [A20](#a20--f4-oauth-retrofit-all-callbacks-write-onboarding_triggers-atomically-with-install) — F4 retrofit; the harness exercises this via direct install+trigger writes that mirror the callback's transaction shape.
- [A21](#a21--mock-api-server-architecture-stateful-in-process-libraries-with-fixture-generators-and-fault-injection) — mock servers; the harness uses these for per-source client substitution.
- [A19](#a19--framework-exception-handling-for-per-source-dispatch-failures) — framework exception handling; harness fault profiles trigger A19's broad catches under the `FLAKY` / `RATE_LIMITED` / `AUTH_EXPIRED` presets.

**Effect on future work:** Mega-prompt 3 (live-ingestion synthetics) will reuse the harness shape, extending it with Pub/Sub and Gateway driver patterns. First-customer pilot work will reuse the harness for regression testing — define BackfillScenarios that mirror the pilot's source mix.

---

## A21 — Mock API server architecture: stateful in-process libraries with fixture generators and fault injection

**Status:** Resolved with X2 commit on `feat/ingestion-x2-mock-api-servers`.

**Trigger:** Mega-prompt 2 X2 work-unit. Synthetic testing of M6 backfill requires mock per-source clients that faithfully simulate real API behavior (cursor progression, etag handling, rate limits, transient failures). M6.3-M6.6 fetcher / reconciler / planner tests had per-test ad-hoc fakes; X2 centralizes the mock surface so X3's harness (and future testing) reuses one canonical set of mocks instead of duplicating.

**Decision:** In-process Python class libraries replacing production per-source clients at the `_open_<source>_client` factory seams. Three orthogonal abstractions:

1. **Mock clients** (`services/synthetic/mock_clients/{gmail,github,slack,discord}.py`):
   - Each class implements ONLY the methods called by M6 backfill code (planner / fetcher / reconciler) — not the full provider SDK surface.
   - Stateful per session — `MockGmailClient` tracks `history_id`; `MockGithubClient` tracks `etag` state per `(owner, repo, event_type)`; `MockSlackClient` paginates via opaque `next_cursor`; `MockDiscordClient` paginates via snowflake `before` / `after`.
   - Each method consults a `FaultProfile` first (via `_MockBase._check_fault`); on the happy path serves the fixture data; on a configured fault raises the source's real error type.
   - Constructor takes `fixture` (the data to serve) + `profile` (fault configuration); both are dataclasses.

2. **Fixture generators** (`services/synthetic/fixtures/{gmail,github,slack,discord}_generator.py`):
   - `make_gmail_mailbox(email, messages=N, history_events=M, message_size_kb=K, page_size=P)`.
   - `make_github_repos(org_or_user, repos=N, events_per_repo=M, per_page=P)`.
   - `make_slack_workspace(team_id, channels=N, messages_per_channel=M, page_size=P)`.
   - `make_discord_guild(guild_id, channels=N, messages_per_channel=M, page_size=P)`.
   - Each generator is deterministic: same parameters → identical fixture (test verified: `test_fixture_generators_are_deterministic`). Internal randomness uses hash-based digests, not RNG.

3. **Fault profiles** (`services/synthetic/fault_profiles/profiles.py`):
   - `FaultProfile` dataclass with four orthogonal knobs: `rate_limit_after_n_requests`, `random_5xx_probability`, `auth_expires_after_n_seconds`, `transient_network_error_probability`. RNG seeded by `rng_seed` for deterministic probabilistic faults.
   - Presets: `HAPPY_PATH` (no faults), `RATE_LIMITED` (rate-limit after 50), `FLAKY` (10% 5xx), `AUTH_EXPIRED` (auth dies after 30s).

**Per-source error type mapping** (the mocks raise these on configured faults, matching the production clients' surface):

| Source  | Rate limit | 5xx              | Auth          | Transient        |
|---------|------------|------------------|---------------|------------------|
| Gmail   | `GoogleRateLimited` | `GoogleApiError` | `GoogleApiError` (401) | `GoogleApiError` |
| GitHub  | `GithubApiError` | `GithubApiError` | `GithubApiError` (401) | `GithubApiError` |
| Slack   | `SlackApiError`  | `SlackApiError`  | `SlackApiError` (invalid_auth) | `SlackApiError`  |
| Discord | `DiscordApiError` | `DiscordApiError` | `DiscordApiError` (401) | `DiscordApiError` |

**Wiring at test time:**

```python
from services.synthetic.mock_clients.gmail import MockGmailClient
from services.synthetic.fixtures import make_gmail_mailbox
from services.synthetic.fault_profiles import HAPPY_PATH

fixture = make_gmail_mailbox(email="alice@x.com", messages=10)
client = MockGmailClient(fixture=fixture, profile=HAPPY_PATH)

async def _open(install):
    async def close(): return None
    return client, close
monkeypatch.setattr(gmail_fetcher_mod, "_open_gmail_client", _open)
```

The seam contract (the `(client, close)` tuple returned from `_open_*_client`) is unchanged from M6.3-M6.6.

**Rationale:**

- **In-process** avoids port management, startup sequencing, and Docker complexity. Mock clients are Python classes, instantiated and passed in.
- **Stateful** mocks reproduce real cursor-advance and reshare patterns. A reconciler's `get_profile` / `head_repo_events` / `conversations_history(oldest=...)` / `get_messages(after=...)` probe surfaces a higher watermark or new records exactly as the production API would; X3's harness can drive reshare cycles by parameterizing the fixture appropriately.
- **Fixture generators** enable load-scale testing (100+ tenants, varied sizes) without manual fixture authoring. Determinism is essential — flaky synthetic tests are worse than no synthetic tests.
- **Fault injection** tests framework resilience contracts ([A19](#a19--framework-exception-handling-for-per-source-dispatch-failures)'s broad exception handling). A `FLAKY` profile drives the framework's per-shard failure marking; a `RATE_LIMITED` profile drives the cursor-resume path.

**Tests:**

- `test_mock_clients.py` — 22 tests covering: happy-path serve (4 sources), cursor advance (4), rate-limit threshold (4), error-type correctness on fault (4), source-specific stateful probes (Gmail history_id, GitHub etag, Slack oldest_ts, Discord after_snowflake). Plus determinism + profile shape.
- `test_compatibility_with_m6.py` — 5 tests verifying each mock implements the methods called by M6 code (introspection-based shape check) + the `(client, close)` factory tuple contract.

**Cross-references:**

- [A18](#a18--per-source-backfill-is-net-new-code-framework--existing-steady-state-coexist-until-m7) — per-source dispatch; the mocks plug into the per-source planner / fetcher / reconciler seams.
- [A18.3](#a183--reconciler-pool-provider-seam) — reconciler pool-provider seam; the mocks don't touch DB, but they coexist with the pool-provider pattern (tests inject both).
- [A19](#a19--framework-exception-handling-for-per-source-dispatch-failures) — framework exception handling; fault profiles let mock servers trigger A19's broad catches.

**Effect on testing:** X3 harness uses these mock libraries instead of ad-hoc per-test fakes. Mega-prompt 3 (live-ingestion synthetics) will extend the mock surface with push/Gateway driver patterns. Post-mega-prompt-3 work (first-customer pilot regression suites) reuses the same mocks; the canonical mock library is the long-term home for source-side test substrate.

---

## A20 — F4 OAuth retrofit: all callbacks write `onboarding_triggers` atomically with install

**Status:** Resolved with X1 commit on `feat/ingestion-x1-oauth-onboarding-triggers-retrofit`.

**Trigger:** Pre-customer-cutover audit revealed the M6 framework was inert in production because OAuth callbacks (Gmail / Slack / GitHub / Discord) never wrote `onboarding_triggers` rows. Filed as [ticket #36](../decisions/ticket-36-oauth-callbacks-onboarding-triggers-retrofit.md) during M6.3 closeout; resolved here.

**Decision:** Each OAuth callback inserts an `onboarding_triggers` row in the same transaction as the install row insert. Idempotent on retry via partial unique indexes (migration 0057) + `ON CONFLICT DO NOTHING`. Forward-only — no backfill of existing installs (none in production currently).

**Rationale:**

- **Atomicity** removes the install-succeeded-but-no-trigger failure mode. Pre-retrofit, each callback ran install UPSERT + audit (+ side effects) as separate auto-commit statements; a crash between the install commit and a separate trigger write would have silently dropped onboarding. The retrofit makes install + trigger one transaction; either both land or neither does.
- **DB-level idempotency** prevents application-level race windows. OAuth retries (browser refresh, network retransmit, user clicking "install" twice) and reinstalls (user re-completing the OAuth flow on an existing workspace) are all common in production; the unique-index-driven `ON CONFLICT DO NOTHING` makes the path silently safe rather than depending on application logic for dedup.
- **Forward-only** avoids speculative migration of nonexistent data — no installs predate this retrofit in production, so no backfill is needed.

**Implementation:**

- **Migration 0057** — `0057_onboarding_triggers_unique_per_install.sql`:
  - `UNIQUE (tenant_id, source, installation_row_id) WHERE installation_row_id IS NOT NULL` — applies to slack/github/discord triggers (which reference `provider_installations.id`).
  - `UNIQUE (tenant_id, source, gmail_installation_id) WHERE gmail_installation_id IS NOT NULL` — applies to gmail triggers (which reference `gmail_installations.id`).
  - Two partial indexes because the schema (from migration 0047) uses mutually exclusive install-id columns; a single multi-column unique would treat both NULLs as distinct.
- **Gmail** (`services/integrations/gmail/oauth.py::connect_finalize`) — install + trigger inside the existing `async with tenant_transaction(tenant_id) as tctx:` block.
- **Slack** (`services/integrations/slack/oauth.py::callback_handler`) — wrapped the previously-autocommit install UPSERT in `async with pool.acquire() as conn: async with conn.transaction():` (the pre-retrofit code ran each statement with autocommit per asyncpg default); added `_emit_onboarding_trigger` helper. `_upsert_installation` accepts pool OR connection per [A12](#a12--executor-typed-substrate-signatures-for-transactional-participation).
- **GitHub** (`services/integrations/github/oauth.py::callback_handler`) — same shape as Slack; added `_upsert_installation_in_tx` (connection-bound variant) + `_emit_onboarding_trigger`.
- **Discord** (`services/integrations/discord/oauth.py::callback_handler`) — same shape as Slack/GitHub.

**Trigger row shape (column-name reality):**

The `onboarding_triggers` schema (migration 0047) uses TWO mutually-exclusive install-id columns: `installation_row_id` (for slack/github/discord — references `provider_installations.id`) and `gmail_installation_id` (for gmail — references `gmail_installations.id`). The retrofit fills exactly one column per source; the other stays NULL. The `trigger_kind` value reflects the install lifecycle: `'install'` for fresh inserts, `'reinstall'` when the UPSERT updated an existing row (detected via `xmax = 0` for non-Gmail; Gmail's UPSERT does not surface this flag — Gmail relies entirely on the partial unique index for retry idempotency).

**Tests:**

- `test_{gmail,slack,github,discord}_oauth_callback_writes_onboarding_trigger` — for each source, drives the callback end-to-end (HTTP), asserts the install row and trigger row both exist, asserts the trigger references the install via the correct id column.
- `test_oauth_callback_atomic_rollback_includes_trigger` — monkeypatches `_emit_onboarding_trigger` to raise; asserts the install row also rolls back (neither row present in DB).
- `test_oauth_callback_idempotent_on_retry_with_unique_constraint` — issues two callbacks for the same install identity; asserts exactly one trigger row exists.

**Cross-references:**

- [A12](#a12--executor-typed-substrate-signatures-for-transactional-participation) — executor-typed substrate signatures; the per-source `_upsert_installation` helpers now accept `Pool | Connection`.
- [A18](#a18--per-source-backfill-is-net-new-code-framework--existing-steady-state-coexist-until-m7) — per-source backfill is net-new code; F4 is the trigger source for that net-new code. Without A20, the M6 chain has no real-traffic source in production.
- [Ticket #36](../decisions/ticket-36-oauth-callbacks-onboarding-triggers-retrofit.md) — this amendment is the resolution.

**Effect on future per-source additions:** M6.7+ (if added) follows the same pattern. New OAuth callbacks write `onboarding_triggers` atomically with the install row; new sources extend the partial unique index pattern as needed.

---

## A19 — Framework exception handling for per-source dispatch failures

**Status:** Resolved with the post-M-Load follow-up commit on `integration/ingestion-hardening`.

**Trigger:** Post-M6.5 merge surfaced that `SourceOnboarding`'s narrow `except NotImplementedError` crashed the orchestrator subprocess when Slack's real planner raised `RuntimeError` on missing `source_client`. The narrow-catch pattern was inherited at multiple framework dispatch call sites; this amendment broadens them uniformly so any per-source dispatch failure is absorbed by the framework, marked as a per-run failure, and the service keeps serving subsequent work.

**Decision:** Framework dispatch call sites — `SourceOnboarding`'s planner dispatch, `ShardFetch`'s fetcher dispatch, `Reconciler`'s reconciler dispatch — catch `Exception`, not narrow subclasses. On exception: mark the relevant entity (run or shard) as failed with the exception's repr in `failure_reason`; keep the service serving. The `NotImplementedError` branch is preserved purely for `failure_reason` formatting (operator-facing "not yet implemented" distinction); control flow is identical between the two branches.

**Rationale:** Per-source dispatch entries are net-new code (per [A18.1](#a181--per-source-backfill-is-net-new-code-not-a-behavior-preserving-refactor)). Real per-source implementations have realistic failure modes beyond `NotImplementedError`:
- Rate limits (`RuntimeError`, provider-specific exception types).
- Expired credentials (auth-layer exceptions).
- Transient network failures (`httpx.HTTPError`, `asyncio.TimeoutError`).
- Unexpected API responses (`KeyError`, `pydantic.ValidationError`).
- Configuration errors at runtime (`RuntimeError` from missing client wiring — the M6.5 case that motivated this amendment).

Narrow catches let those failures bypass the framework's per-run failure-marking and crash the service. A single bad signal must NOT take down the orchestrator. Broad catch with explicit per-run failure marking is the framework's resilience contract; the catch is the boundary between "per-source code can fail freely" and "framework guarantees forward progress."

**Implementation:**
- `services/ingestion/workflows/source_onboarding.py::_handle_source_requested` — narrow `NotImplementedError` + broad `Exception` (broadened in commit `29b797c`).
- `services/ingestion/workflows/shard_fetch.py::_run_fetch_loop` — narrow `NotImplementedError` + broad `Exception` (already present pre-A19; this amendment codifies the pattern).
- `services/ingestion/workflows/reconciler.py::_handle_source_shards_completed` — wraps the dispatch call with narrow `NotImplementedError` + broad `Exception`; on exception, calls `_mark_run_failed` (new helper) and `_emit_source_completed` with the failure reason. WHERE-guard on the `UPDATE` accepts `('pending', 'in_progress', 'completed')` because the reconciler's dispatch typically fires when status is already `'completed'` (post-SourceOnboarding rollup); the `reconciled_at IS NULL` guard prevents clobbering a successful reconciliation.

**Tests:**
- `test_shard_fetch_handles_unexpected_fetcher_exception` — fetcher raising `RuntimeError`; shard marked failed; `last_error` contains exception repr; service keeps serving.
- `test_source_onboarding_handles_unexpected_planner_exception` — planner raising `RuntimeError`; run marked failed; `failure_reason` contains exception repr; service keeps serving. Load-bearing for the `29b797c` fix.
- `test_reconciler_handles_unexpected_dispatch_exception` — reconciler raising `RuntimeError`; run marked failed; `reconciled_at` stays NULL; `source_onboarding_completed` emitted with failure_reason; service keeps serving.

Each test exercises the broadened catch via `monkeypatch.setitem` on the dispatch table — the same shape as the existing `_not_implemented_*` tests but with an `_exploding_*` stub.

**Cross-references:**
- [A18.1](#a181--per-source-backfill-is-net-new-code-not-a-behavior-preserving-refactor) — per-source backfill is net-new code; this resilience contract follows directly.
- [A18.3](#a183--reconciler-pool-provider-seam) — Reconciler pool-provider seam; an unregistered provider raises at dispatch time, now absorbed by the A19 broad catch.

**Effect on future sub-blocks:** M6.7+ sources, F4 retrofit (mega-prompt 2), backfill harness (mega-prompt 2), and any future framework consumer inherits this pattern. Per-source modules can raise any exception; the framework absorbs it, marks the relevant entity failed with the exception repr, and keeps serving subsequent work. Stub messages can still raise `NotImplementedError` specifically; the framework's catch is broad regardless.

**Pattern documentation:** `docs/ingestion/pattern-alignment-rules.md` references A19 under framework resilience patterns. The static analyzer does NOT enforce A19 (it's a runtime resilience contract, not a structural one); the reference is for human contributors auditing dispatch call sites.

---

## Resolved amendments archive

(Empty — A1 and A2 land here at M3.4 closeout once the LLD edits ship.)
