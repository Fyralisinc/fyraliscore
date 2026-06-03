-- =====================================================================
-- 0090_sage_reader_activation_traces.sql
--
-- SAGE Phase 4 gap closer: durable soft-activation traces.
--
-- The Synthesis Reader computes query-conditioned Model activation
-- scores before subgraph selection. These rows store the explainable
-- activation reasons required by Phase 4, while staying firmly in the
-- Discovery Utility Layer: activation is learned retrieval bookkeeping,
-- not canonical truth about a Model.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS sage_reader_activations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL
    REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  question_id TEXT NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  activation_score DOUBLE PRECISION NOT NULL CHECK (
    activation_score >= 0 AND activation_score <= 1
  ),
  activation_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  selected BOOLEAN NOT NULL DEFAULT FALSE,
  selection_rank INTEGER CHECK (
    selection_rank IS NULL OR selection_rank >= 0
  ),
  source_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (inquiry_session_id, question_id, model_id)
);

COMMENT ON TABLE sage_reader_activations IS
  'SAGE reader soft-activation trace — learned retrieval utility, not canonical truth.';

CREATE INDEX IF NOT EXISTS sage_reader_activations_session_idx
  ON sage_reader_activations (inquiry_session_id, question_id, activation_score DESC);

CREATE INDEX IF NOT EXISTS sage_reader_activations_model_idx
  ON sage_reader_activations (tenant_id, model_id, created_at DESC);

CREATE INDEX IF NOT EXISTS sage_reader_activations_selected_idx
  ON sage_reader_activations (tenant_id, selected, created_at DESC);

ALTER TABLE sage_reader_activations ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_reader_activations FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON sage_reader_activations;
CREATE POLICY tenant_isolation ON sage_reader_activations
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
