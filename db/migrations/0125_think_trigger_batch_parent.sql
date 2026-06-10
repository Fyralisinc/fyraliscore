-- =====================================================================
-- 0125_think_trigger_batch_parent.sql
--
-- Minimal support for worker-side T1 batching. Individual signal triggers
-- stay durable rows; when the worker coalesces them, each member points at
-- a synthetic T1:event_batch parent row and is hidden from normal polling.
-- If the batch exhausts retries, members are released back to scalar T1.
-- =====================================================================

BEGIN;

ALTER TABLE think_trigger_queue
  ADD COLUMN IF NOT EXISTS batch_parent_id UUID;

CREATE INDEX IF NOT EXISTS think_trigger_queue_batch_parent_idx
  ON think_trigger_queue (batch_parent_id)
  WHERE batch_parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS think_trigger_queue_ready_unbatched_idx
  ON think_trigger_queue (tenant_id, trigger_kind, trigger_subkind, enqueued_at)
  WHERE completed_at IS NULL
    AND locked_by IS NULL
    AND batch_parent_id IS NULL;

COMMIT;
