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

---

## Resolved amendments archive

(Empty — A1 and A2 land here at M3.4 closeout once the LLD edits ship.)
