-- =====================================================================
-- 0007_q4_q8_resolutions.sql — Q4 + Q8 schema resolutions (pre-Wave-3-B)
-- =====================================================================
-- Resolutions authored by Rachin; applied before Wave 3-B launches. See
-- BUILD-LOG.md entry "Wave 2→3 Q4/Q8 resolution" and SCHEMA-LOCK.md
-- "Post-Wave-2 amendments" section.
--
-- Q4 — region locks. Decision: Postgres advisory locks
-- (pg_advisory_xact_lock) as the enforcement mechanism. No enforcement
-- table. A separate observability-only log table records every
-- acquisition for debugging / contention analysis. Writes to the log
-- are best-effort, async, fire-and-forget.
--
-- Q8 — model_reeval_queue. Decision: real durable Postgres table with
-- attempts + last_error, and a NULLS-NOT-DISTINCT dedup constraint so
-- unprocessed duplicates collapse but processed rows can be re-enqueued
-- later. Cause_kind enum has five values.
--
-- Migration 0006 is owned by Wave 3-A (relationship_maintenance_log);
-- this migration takes 0007.
-- Immutable once committed.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- Q8 — model_reeval_queue
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_reeval_queue (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id),
  cause_model_id UUID REFERENCES models(id),
  cause_kind TEXT NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  CONSTRAINT model_reeval_queue_cause_kind_check CHECK (
    cause_kind IN (
      'supporting_archived',
      'supporting_deprecated',
      'supporting_superseded',
      'contested_cluster',
      'falsifier_triggered_upstream'
    )
  ),
  -- Dedup: unprocessed entries with the same (tenant, model, cause)
  -- collapse into one row. Once processed_at is set, a new identical
  -- row can be enqueued later (NULLS NOT DISTINCT — Postgres 15+).
  CONSTRAINT model_reeval_queue_dedup UNIQUE NULLS NOT DISTINCT
    (tenant_id, model_id, cause_model_id, processed_at)
);

CREATE INDEX IF NOT EXISTS model_reeval_queue_pending_idx
  ON model_reeval_queue (tenant_id, enqueued_at)
  WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS model_reeval_queue_model_idx
  ON model_reeval_queue (model_id);

-- ---------------------------------------------------------------------
-- Q4 — think_region_lock_log (observability only; NOT enforcement)
-- ---------------------------------------------------------------------
-- Enforcement is pg_advisory_xact_lock inside Think's apply
-- transaction. This table is for post-hoc instrumentation: which
-- regions contend most, how long Think runs waited, which entities
-- were touched. Writes are best-effort — Think must keep working if
-- this table is full / corrupt / missing.
CREATE TABLE IF NOT EXISTS think_region_lock_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  think_run_id UUID NOT NULL,
  tenant_hash INTEGER NOT NULL,
  entity_hash INTEGER NOT NULL,
  entity_ids JSONB NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ,
  wait_duration_ms INTEGER,
  hold_duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS think_region_lock_log_run_idx
  ON think_region_lock_log (think_run_id);

CREATE INDEX IF NOT EXISTS think_region_lock_log_time_idx
  ON think_region_lock_log (tenant_id, acquired_at DESC);

COMMIT;
