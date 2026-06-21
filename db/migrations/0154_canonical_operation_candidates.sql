-- 0154_canonical_operation_candidates.sql
--
-- Durable review queue for canonical topology operations that do not fit the
-- existing relationship_candidates shape. Multi-model merge/promote proposals
-- continue to use relationship_candidates. Single-model split/demote proposals
-- land here for validation instead of remaining report-only payloads.

BEGIN;

CREATE TABLE IF NOT EXISTS canonical_operation_candidates (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source TEXT NOT NULL DEFAULT 'sage_topology_optimizer',
  operation TEXT NOT NULL CHECK (operation IN ('split', 'demote')),
  op_key TEXT NOT NULL,
  proposed_kind TEXT NOT NULL DEFAULT '',
  source_model_id UUID REFERENCES models(id) ON DELETE SET NULL,
  source_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  reason TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  review_status TEXT NOT NULL DEFAULT 'needs_review' CHECK (
    review_status IN ('candidate', 'needs_review', 'accepted', 'rejected', 'retired')
  ),
  decided_at TIMESTAMPTZ,
  decided_by UUID REFERENCES actors(id) ON DELETE SET NULL,
  decision_note TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, source, operation, op_key)
);

CREATE INDEX IF NOT EXISTS canonical_operation_candidates_review_idx
  ON canonical_operation_candidates (
    tenant_id, review_status, operation, created_at DESC
  )
  WHERE review_status IN ('candidate', 'needs_review');

CREATE INDEX IF NOT EXISTS canonical_operation_candidates_source_model_idx
  ON canonical_operation_candidates (tenant_id, source_model_id, created_at DESC)
  WHERE source_model_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS canonical_operation_candidates_payload_gin
  ON canonical_operation_candidates USING gin (payload);

ALTER TABLE canonical_operation_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE canonical_operation_candidates FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON canonical_operation_candidates;
CREATE POLICY tenant_isolation ON canonical_operation_candidates
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'company_os') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON TABLE canonical_operation_candidates TO company_os;
  END IF;
END
$$;

COMMIT;
