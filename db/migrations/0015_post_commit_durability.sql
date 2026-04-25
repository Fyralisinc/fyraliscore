-- =====================================================================
-- 0015_post_commit_durability.sql — Wave 5 OP-1
-- =====================================================================
-- THINK-DESIGN-AUDIT §8.1 and §10 argument 1: post-commit actions
-- (publish_anomalies / schedule_predictions / broadcast_realtime /
--  invalidate_metrics) run inline after apply commits. If the worker
-- crashes between commit and post-commit, work is lost; subsequent
-- retries short-circuit on the idempotency ledger and never re-run
-- the post-commit side effects.
--
-- Fix: enqueue post-commit actions INSIDE the apply transaction. The
-- enqueue is atomic with the apply. A separate post-commit worker
-- drains the queue with visible-for-update-skip-locked dispatch,
-- exponential backoff, and a dead-letter threshold of 5 attempts.
--
-- Style mirrors 0008_think_runs_applied_triggers_dead_letter.sql:
--   * CREATE TABLE IF NOT EXISTS for idempotency
--   * tenant-scoped; indexed on (tenant, time) for dashboards
--   * no cross-table FKs (trigger rows may be archived)
--   * single transactional migration
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- pending_post_commit_actions — durable queue for post-commit actions.
-- ---------------------------------------------------------------------
-- One row per (trigger, action_kind) pair. The action_payload JSONB
-- carries the full dispatch data (anomaly ids, predictions, etc.) so
-- the dispatch worker never needs to re-query the apply's state.
--
-- scheduled_at: if an attempt fails with a transient error, the worker
-- bumps scheduled_at with exponential backoff (base 2^attempts seconds,
-- capped at 5 min). Worker's poll query filters by scheduled_at <= now().
--
-- processed_at: NULL while pending; set to the attempt's timestamp on
-- successful dispatch. Part of the UNIQUE dedup key so a single trigger
-- can be re-enqueued safely after its previous attempt processed —
-- NULLS NOT DISTINCT means two NULLs collide (dedup) while a NULL and
-- a non-NULL do not collide (allow re-enqueue after processing).
CREATE TABLE IF NOT EXISTS pending_post_commit_actions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  trigger_id UUID NOT NULL,
  action_kind TEXT NOT NULL CHECK (
    action_kind IN (
      'publish_anomalies',
      'schedule_predictions',
      'broadcast_realtime',
      'invalidate_metrics'
    )
  ),
  action_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  dead_lettered_at TIMESTAMPTZ,

  -- NULLS NOT DISTINCT is a Postgres 15+ feature; it treats two NULLs as
  -- equal for uniqueness purposes. Without it, the dedup would silently
  -- allow duplicate pending rows for the same (tenant, trigger, kind).
  CONSTRAINT post_commit_dedup UNIQUE NULLS NOT DISTINCT (
    tenant_id, trigger_id, action_kind, processed_at
  )
);

-- Partial index for the worker's poll query. Keeps the hot path tiny
-- (only un-processed rows) while processed rows accumulate cheaply.
CREATE INDEX IF NOT EXISTS post_commit_pending_idx
  ON pending_post_commit_actions (scheduled_at)
  WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

-- Secondary: inspect every action for a specific trigger.
CREATE INDEX IF NOT EXISTS post_commit_trigger_idx
  ON pending_post_commit_actions (trigger_id);

-- Tenant-scoped dashboards (per-tenant queue depth).
CREATE INDEX IF NOT EXISTS post_commit_tenant_idx
  ON pending_post_commit_actions (tenant_id, created_at DESC);

COMMIT;
