-- 0133_learning_loop_obligations_feedback.sql
--
-- Durable loop-closing surfaces for the learning-loop plan.
--
-- * think_obligations records future reasoning obligations in a generic form
--   while the legacy model_reeval_queue remains compatible.
-- * think_feedback_stats stores bounded aggregate feedback about validator /
--   applier drops and product recommendation outcomes.

BEGIN;

CREATE TABLE IF NOT EXISTS think_obligations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  kind TEXT NOT NULL,
  object_kind TEXT NOT NULL,
  object_id UUID,
  due_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  trigger_kind TEXT NOT NULL DEFAULT 'T4',
  trigger_subkind TEXT,
  observation_id UUID,
  model_id UUID REFERENCES models(id) ON DELETE CASCADE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'fired', 'completed', 'cancelled', 'failed')),
  fires INTEGER NOT NULL DEFAULT 0 CHECK (fires >= 0),
  max_fires INTEGER NOT NULL DEFAULT 1 CHECK (max_fires > 0),
  last_trigger_id UUID,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS think_obligations_open_object_idx
  ON think_obligations (tenant_id, kind, object_kind, object_id)
  WHERE status = 'open' AND object_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS think_obligations_due_idx
  ON think_obligations (due_at, tenant_id)
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS think_obligations_model_idx
  ON think_obligations (tenant_id, model_id)
  WHERE model_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS think_feedback_stats (
  tenant_id UUID NOT NULL,
  surface TEXT NOT NULL,
  op_type TEXT NOT NULL,
  op_kind TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  success_count BIGINT NOT NULL DEFAULT 0 CHECK (success_count >= 0),
  dropped_count BIGINT NOT NULL DEFAULT 0 CHECK (dropped_count >= 0),
  failure_count BIGINT NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
  last_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, surface, op_type, op_kind, reason)
);

CREATE INDEX IF NOT EXISTS think_feedback_stats_surface_idx
  ON think_feedback_stats (tenant_id, surface, last_seen_at DESC);

COMMIT;
