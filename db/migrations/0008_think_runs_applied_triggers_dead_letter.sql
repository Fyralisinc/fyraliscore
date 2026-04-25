-- =====================================================================
-- 0008_think_runs_applied_triggers_dead_letter.sql — Wave 3-B Think
-- operational tables.
-- =====================================================================
-- Three sidecars owned by Agent 3-B per BUILD-PLAN §4 Prompt 3.B:
--   * applied_triggers         — idempotency ledger (spec §7 "Idempotency")
--   * think_runs               — one row per Think invocation (observability)
--   * model_reeval_dead_letter — sidecar for model_reeval_queue after N=5
--                                failed attempts (Q8 consumer contract)
--   * think_anomalies_raw      — durable queue for anomaly publish from apply;
--                                consumer is Wave 4-B anomaly_processor
--
-- All tables are tenant-scoped. Reads from these tables are indexed on
-- (tenant_id, <time>) so per-tenant dashboards stay fast as the system
-- grows. No cross-table FKs — trigger_id / run_id / model_id are not
-- FK-enforced because the target rows may live in partitioned tables
-- (observations) or be cascaded through Wave-4 archivals where the
-- target row no longer exists but the audit row should.
--
-- Idempotent — IF NOT EXISTS on every CREATE.
-- Immutable once committed.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- applied_triggers — idempotency ledger for Think runs.
-- ---------------------------------------------------------------------
-- Spec §7 "Idempotency". Before a Think run applies its diff, we
-- INSERT a row here keyed by trigger_id with outcome='pending'. If the
-- apply transaction commits, outcome is updated to 'success' in the
-- same transaction. If the transaction rolls back, the row rolls back
-- with it — the trigger can be retried.
--
-- Double-apply prevention: a second Think run with the same trigger_id
-- sees the existing row and short-circuits (status='skipped_idempotent'
-- in think_runs).
CREATE TABLE IF NOT EXISTS applied_triggers (
  trigger_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  diff_hash TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN (
      'pending',
      'success',
      'validation_failure',
      'reasoning_failure',
      'database_error'
    )
  )
);

CREATE INDEX IF NOT EXISTS applied_triggers_tenant_time_idx
  ON applied_triggers (tenant_id, applied_at);

-- ---------------------------------------------------------------------
-- think_runs — one row per Think invocation. Observability.
-- ---------------------------------------------------------------------
-- Writes happen in the same transaction as the apply so that a rolled-
-- back apply doesn't leave a dangling 'running' row. Fields are
-- populated progressively:
--   * started_at      — at the top of think()
--   * status          — 'running' initially; final status set before commit
--   * retrieval_*     — set after retrieval
--   * llm_latency_ms  — set after LLM call (deterministic runs leave NULL)
--   * validation_error_count — set after validate()
--   * ops_applied     — JSON summary of the applied diff
--   * cascade_depth   — set after cascade() returns
--   * region_*        — hashes computed pre-lock
CREATE TABLE IF NOT EXISTS think_runs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  trigger_id UUID NOT NULL,
  trigger_kind TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running' CHECK (
    status IN ('running', 'success', 'failed', 'skipped_idempotent')
  ),
  error TEXT,
  retrieval_model_count INTEGER,
  retrieval_observation_count INTEGER,
  llm_latency_ms INTEGER,
  validation_error_count INTEGER,
  ops_applied JSONB,
  cascade_depth INTEGER DEFAULT 0,
  region_tenant_hash INTEGER,
  region_entity_hash INTEGER
);

CREATE INDEX IF NOT EXISTS think_runs_tenant_time_idx
  ON think_runs (tenant_id, started_at DESC);

CREATE INDEX IF NOT EXISTS think_runs_trigger_idx
  ON think_runs (trigger_id);

CREATE INDEX IF NOT EXISTS think_runs_status_idx
  ON think_runs (tenant_id, status)
  WHERE status != 'success';

-- ---------------------------------------------------------------------
-- model_reeval_dead_letter — sidecar for the N=5 retry policy.
-- ---------------------------------------------------------------------
-- Per Wave 2→3 amendment W3.Q8 consumer contract: after 5 failed T4
-- runs, the model_reeval_queue row is moved here and its
-- processed_at is set to freeze the original row (so the dedup
-- constraint on (tenant, model, cause, processed_at) collapses any
-- re-enqueue into a NEW row whose processed_at is NULL).
--
-- Operators can drain this table and manually requeue after fixing
-- the upstream issue (LLM prompt regression, dep inconsistency, etc.).
CREATE TABLE IF NOT EXISTS model_reeval_dead_letter (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  original_queue_id UUID NOT NULL,
  model_id UUID NOT NULL,
  cause_model_id UUID,
  cause_kind TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  last_error TEXT NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL,
  dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS model_reeval_dead_letter_tenant_idx
  ON model_reeval_dead_letter (tenant_id, dead_lettered_at DESC);

CREATE INDEX IF NOT EXISTS model_reeval_dead_letter_model_idx
  ON model_reeval_dead_letter (model_id);

-- ---------------------------------------------------------------------
-- think_anomalies_raw — durable in-DB queue for anomalies detected
-- during apply. Consumed by Wave 4-B's anomaly_processor which may
-- debounce and emit T3 triggers.
-- ---------------------------------------------------------------------
-- Spec §7 "publish_anomalies" writes to an 'anomalies_raw' stream. We
-- back the stream with a Postgres table so (a) the anomaly survives a
-- worker crash between commit and post-commit publish, and (b) Wave
-- 4-B's consumer can use a FOR UPDATE SKIP LOCKED dispatch pattern
-- identical to think_trigger_queue.
CREATE TABLE IF NOT EXISTS think_anomalies_raw (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  think_run_id UUID NOT NULL,
  kind TEXT NOT NULL,
  region JSONB NOT NULL,
  significance DOUBLE PRECISION NOT NULL,
  triggering_op JSONB NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS think_anomalies_raw_pending_idx
  ON think_anomalies_raw (tenant_id, published_at)
  WHERE consumed_at IS NULL;

CREATE INDEX IF NOT EXISTS think_anomalies_raw_run_idx
  ON think_anomalies_raw (think_run_id);

COMMIT;
