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

- **Status:** Open (M3.2 will implement; M3.4 documents the LLD edit).
- **LLD section:** §5.4 (Embedding worker pool — `embed_and_update`).
- **Implementation surface:**
  [docs/ingestion/03-low-level-design.md:1737-1743](03-low-level-design.md#L1737-L1743)
  (current LLD pseudocode) vs. the actual
  `observations` schema's `embedding_pending BOOLEAN` column.
- **What the LLD says today:** §5.4's `embed_and_update` pseudocode
  uses `WHERE id = $2 AND embedding_pending = TRUE` — which IS the
  correct guard. The M3 prompt restated this as
  `WHERE id = $2 AND embedding IS NULL`, which would race with the
  inline ingestion path if both writers try to claim the same row
  during the coexistence window (the inline path sets
  `embedding_pending = FALSE` and `embedding != NULL` atomically,
  while the worker would see `embedding IS NULL` as still-claimable).
- **What's actually true:** `embedding_pending = TRUE` is the
  load-bearing guard; `embedding IS NULL` is wrong for the M3.2
  coexistence model. The discrepancy lives in the prompt-vs-LLD
  delta, not the LLD itself — but M3.2 implementation MUST track the
  LLD wording, and the prompt's deviation should be explicitly noted
  in the worker docstring so a future reader doesn't "fix" it back.
- **LLD edit pending in M3.4:** none in the LLD itself (it's already
  correct). M3.2 PR description must call out that the prompt's
  `WHERE embedding IS NULL` was rejected in favour of the LLD's
  `WHERE embedding_pending = TRUE`, with the race rationale.

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
