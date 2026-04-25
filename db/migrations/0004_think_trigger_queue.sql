-- =====================================================================
-- 0004_think_trigger_queue.sql — T1/T2/T3/T4 Think trigger queue
-- =====================================================================
-- BUILD-PLAN §3 Prompt 2.A step 7: after ingest, enqueue a T1 trigger
-- for the Think worker. Wave 2-A owns the producer side; Wave 3-B owns
-- the consumer. This migration stands up the durable queue so the
-- producer can land rows safely.
--
-- PARTIALLY RESOLVES SCHEMA-QUESTION.md Q4 for `think_trigger_queue`
-- (region_locks and pattern_candidates remain open).
--
-- Design decision: Postgres table, not Redis / in-memory. Rationale:
--   - Durability across restarts (spec §7 treats T1 as "event arrival
--     triggered cognition"; losing events in a crash is unacceptable).
--   - Leases via `locked_by / locked_at` for at-least-once semantics.
--   - Ready-rows-only partial index on (scheduled_for) keeps the hot
--     consumer query O(log N).
--   - Tenant isolation column even though Think may process multi-tenant
--     batches — lets operators drain by tenant if needed.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS think_trigger_queue (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  trigger_kind TEXT NOT NULL,      -- 'T1' | 'T2' | 'T3' | 'T4'
  trigger_subkind TEXT,            -- e.g. 'event_arrival', 'prediction_due'
  observation_id UUID,             -- nullable: T2/T3 may lack one
  model_id UUID,                   -- T2 prediction-due, T3 pattern
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  scheduled_for TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts INTEGER NOT NULL DEFAULT 0,
  locked_by TEXT,
  locked_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

-- Ready-to-run partial index: drops completed and currently-leased rows.
CREATE INDEX IF NOT EXISTS think_trigger_queue_ready_idx
  ON think_trigger_queue (scheduled_for)
  WHERE completed_at IS NULL AND locked_by IS NULL;

COMMIT;
