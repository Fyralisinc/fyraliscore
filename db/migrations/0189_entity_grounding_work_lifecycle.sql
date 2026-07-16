-- =====================================================================
-- 0189_entity_grounding_work_lifecycle.sql
--
-- Durable total-fate lifecycle for every accepted deferred grounding unit.
-- Retryable provider/configuration/budget outcomes survive worker restarts;
-- terminal work links to the immutable grounding trace from migration 0188.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS entity_grounding_work_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_observation_id UUID NOT NULL,
  phrase TEXT NOT NULL,
  processing_generation INTEGER NOT NULL DEFAULT 1 CHECK (processing_generation > 0),
  status TEXT NOT NULL CHECK (
    status IN (
      'pending', 'retry_scheduled', 'resolved_for_consumer', 'review',
      'unresolved', 'abstained', 'exhausted', 'escalated'
    )
  ),
  processing_class TEXT NOT NULL DEFAULT 'R2' CHECK (
    processing_class IN ('R0', 'R1', 'R2', 'R3', 'R4', 'R5')
  ),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at TIMESTAMPTZ,
  last_failure_class TEXT,
  last_failure_reason TEXT,
  current_trace_id UUID REFERENCES grounding_traces(id),
  useful_safe_fate JSONB NOT NULL CHECK (jsonb_typeof(useful_safe_fate) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, source_observation_id, phrase, processing_generation)
);

CREATE INDEX IF NOT EXISTS entity_grounding_work_due_idx
  ON entity_grounding_work_items (tenant_id, next_attempt_at, updated_at)
  WHERE status IN ('pending', 'retry_scheduled');
CREATE INDEX IF NOT EXISTS entity_grounding_work_observation_idx
  ON entity_grounding_work_items (tenant_id, source_observation_id, updated_at DESC);

ALTER TABLE entity_grounding_work_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_grounding_work_items FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON entity_grounding_work_items;
CREATE POLICY tenant_isolation ON entity_grounding_work_items
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON TABLE entity_grounding_work_items IS
  'One durable total-fate head per observation phrase and processing generation.';

COMMIT;
