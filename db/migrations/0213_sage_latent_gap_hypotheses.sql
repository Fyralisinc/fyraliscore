-- =====================================================================
-- 0213_sage_latent_gap_hypotheses.sql
--
-- Non-canonical latent-gap hypotheses born from measured residual debt.
--
-- These rows are SAGE lifecycle obligations, not company truth. They summarize
-- missingness that may explain repeated residuals, open questions, validation
-- drops, or relation gaps. A row must be confirmed through ordinary model-layer
-- evidence before it can influence canonical Models.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS sage_latent_gap_hypotheses (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  gap_kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK (
    status IN ('candidate', 'confirmed', 'rejected', 'expired', 'superseded')
  ),
  residual_cluster_hash TEXT NOT NULL,
  supporting_residual_ids UUID[] NOT NULL CHECK (
    array_length(supporting_residual_ids, 1) IS NOT NULL
    AND array_length(supporting_residual_ids, 1) >= 1
  ),
  supporting_observation_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  missing_evidence_statement TEXT NOT NULL,
  falsifier TEXT NOT NULL,
  next_evidence_needed TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  hypothesis_text TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolution_object_kind TEXT,
  resolution_object_id UUID,
  resolution_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  CHECK (
    status = 'candidate'
    OR resolved_at IS NOT NULL
  ),
  CHECK (
    resolution_object_kind IS NULL
    OR resolution_object_kind IN (
      'model',
      'model_signal_reading',
      'model_edge',
      'relation_claim',
      'relation_instance',
      'model_open_question',
      'projection_snapshot',
      'inquiry_outcome_event',
      'clarification_request',
      'human_review'
    )
  )
);

COMMENT ON TABLE sage_latent_gap_hypotheses IS
  'SAGE non-canonical latent missingness hypotheses; not company truth.';

CREATE UNIQUE INDEX IF NOT EXISTS sage_latent_gap_hypotheses_active_dedup_idx
  ON sage_latent_gap_hypotheses (
    tenant_id,
    residual_cluster_hash,
    gap_kind
  )
  WHERE status = 'candidate';

CREATE INDEX IF NOT EXISTS sage_latent_gap_hypotheses_tenant_status_idx
  ON sage_latent_gap_hypotheses (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS sage_latent_gap_hypotheses_residual_ids_idx
  ON sage_latent_gap_hypotheses USING GIN (supporting_residual_ids);

CREATE INDEX IF NOT EXISTS sage_latent_gap_hypotheses_observation_ids_idx
  ON sage_latent_gap_hypotheses USING GIN (supporting_observation_ids);

ALTER TABLE sage_latent_gap_hypotheses ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_latent_gap_hypotheses FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON sage_latent_gap_hypotheses;
CREATE POLICY tenant_isolation ON sage_latent_gap_hypotheses
  USING (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
