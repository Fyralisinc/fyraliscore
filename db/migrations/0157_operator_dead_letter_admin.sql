-- =====================================================================
-- 0157_operator_dead_letter_admin.sql
-- =====================================================================
-- Production operator controls for durable dead-letter queues.
--
-- Adds:
--   * operator_action_log — auditable admin mutations/reads.
--   * quarantine metadata for every DLQ surface.
--   * retry-resolution metadata for model_reeval_dead_letter sidecars.
--   * last_error on think_trigger_queue so exhausted trigger rows carry
--     the terminal failure reason.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS operator_action_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  actor_id UUID NOT NULL REFERENCES actors(id),
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT operator_action_log_action_check CHECK (
    action IN (
      'dead_letter.list',
      'dead_letter.retry',
      'dead_letter.quarantine'
    )
  )
);

CREATE INDEX IF NOT EXISTS operator_action_log_tenant_time_idx
  ON operator_action_log (tenant_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS operator_action_log_actor_idx
  ON operator_action_log (tenant_id, actor_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS operator_action_log_resource_idx
  ON operator_action_log (tenant_id, resource_type, resource_id, occurred_at DESC);

ALTER TABLE think_trigger_queue
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS quarantined_by UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;

ALTER TABLE pending_post_commit_actions
  ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS quarantined_by UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;

ALTER TABLE model_reeval_dead_letter
  ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS quarantined_by UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS quarantine_reason TEXT,
  ADD COLUMN IF NOT EXISTS retried_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS retried_by UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS retry_queue_id UUID;

CREATE INDEX IF NOT EXISTS think_trigger_queue_dead_letter_idx
  ON think_trigger_queue (tenant_id, completed_at DESC)
  WHERE completed_at IS NOT NULL
    AND last_error IS NOT NULL;

CREATE INDEX IF NOT EXISTS post_commit_dead_letter_idx
  ON pending_post_commit_actions (tenant_id, dead_lettered_at DESC)
  WHERE dead_lettered_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS model_reeval_dead_letter_open_idx
  ON model_reeval_dead_letter (tenant_id, dead_lettered_at DESC)
  WHERE quarantined_at IS NULL
    AND retried_at IS NULL;

COMMIT;
