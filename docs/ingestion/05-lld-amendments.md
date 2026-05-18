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

---

## Resolved amendments archive

(Empty — A1 and A2 land here at M3.4 closeout once the LLD edits ship.)
