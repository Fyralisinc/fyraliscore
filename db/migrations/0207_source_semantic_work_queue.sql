-- =====================================================================
-- 0207_source_semantic_work_queue.sql
--
-- Durable orchestration head for the source-semantics belief vertical.
-- Semantic interpretations and admission decisions remain append-only;
-- this table owns only retry, lease, and terminal work state.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS source_semantic_work_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  grounding_trace_id UUID NOT NULL REFERENCES grounding_traces(id),
  status TEXT NOT NULL CHECK (
    status IN (
      'awaiting_embedding', 'pending', 'processing', 'belief_applied',
      'no_admission', 'retry_scheduled', 'failed_terminal'
    )
  ),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by TEXT,
  claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  interpretation_id UUID REFERENCES source_semantic_interpretations(id),
  admission_decision_id UUID REFERENCES source_semantic_admission_decisions(id),
  admitted_model_id UUID REFERENCES models(id),
  last_failure_class TEXT,
  last_failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, grounding_trace_id),
  CHECK (
    (
      status = 'processing'
      AND claimed_by IS NOT NULL
      AND claim_token IS NOT NULL
      AND lease_expires_at IS NOT NULL
    )
    OR (
      status <> 'processing'
      AND claimed_by IS NULL
      AND claim_token IS NULL
      AND lease_expires_at IS NULL
    )
  ),
  CHECK (
    status NOT IN ('retry_scheduled', 'failed_terminal')
    OR (last_failure_class IS NOT NULL AND last_failure_reason IS NOT NULL)
  ),
  CHECK (
    status <> 'belief_applied'
    OR (
      interpretation_id IS NOT NULL
      AND admission_decision_id IS NOT NULL
      AND admitted_model_id IS NOT NULL
    )
  ),
  CHECK (
    status <> 'no_admission'
    OR (
      interpretation_id IS NOT NULL
      AND admission_decision_id IS NOT NULL
      AND admitted_model_id IS NULL
    )
  )
);

CREATE INDEX IF NOT EXISTS source_semantic_work_due_idx
  ON source_semantic_work_items (status, available_at, created_at, id)
  WHERE status IN ('awaiting_embedding', 'pending', 'retry_scheduled');

CREATE INDEX IF NOT EXISTS source_semantic_work_expired_lease_idx
  ON source_semantic_work_items (lease_expires_at, id)
  WHERE status = 'processing';

ALTER TABLE source_semantic_work_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_semantic_work_items FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON source_semantic_work_items;
CREATE POLICY tenant_isolation ON source_semantic_work_items
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON TABLE source_semantic_work_items IS
  'Mutable leased work head for one tenant grounding trace; semantic facts remain append-only.';
COMMENT ON COLUMN source_semantic_work_items.claim_token IS
  'Per-claim fencing token; every processing transition must match the live token.';

COMMIT;
