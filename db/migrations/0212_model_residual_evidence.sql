-- =====================================================================
-- 0212_model_residual_evidence.sql
--
-- General residual channel for model-metabolism compression debt.
--
-- Prediction-specific residuals remain in model_prediction_errors. This table
-- captures broader unpaid compression debt: valuable unmodeled signals,
-- unattached counterevidence, unanchored relationship evidence, validation-
-- dropped value, authority blocks, and uncertain no-ops.
--
-- Residuals are not canonical truth. They are lifecycle obligations that must
-- eventually become absorbed, rejected, expired, or routed to a human/repair
-- path. Canonical truth remains in models, readings, edges, relation frames,
-- projections, and inquiry outcome events.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS model_residual_evidence (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_observation_id UUID,
  think_run_id UUID,
  trigger_id UUID,
  model_id UUID,
  residual_kind TEXT NOT NULL CHECK (
    residual_kind IN (
      'valuable_unmodeled',
      'counterevidence_unattached',
      'relation_unanchored',
      'open_question_needed',
      'validation_dropped_value',
      'authority_blocked',
      'compression_uncertain'
    )
  ),
  compact_summary TEXT NOT NULL,
  reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (
    status IN ('open', 'absorbed', 'rejected', 'expired')
  ),
  absorption_object_kind TEXT,
  absorption_object_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  CHECK (
    status = 'open'
    OR resolved_at IS NOT NULL
  ),
  CHECK (
    absorption_object_kind IS NULL
    OR absorption_object_kind IN (
      'model',
      'model_signal_reading',
      'model_edge',
      'relation_claim',
      'relation_instance',
      'model_open_question',
      'projection_snapshot',
      'inquiry_outcome_event',
      'clarification_request'
    )
  )
);

-- Idempotency for repeated detection of the same open compression debt.
CREATE UNIQUE INDEX IF NOT EXISTS model_residual_evidence_open_dedup_idx
  ON model_residual_evidence (
    tenant_id,
    COALESCE(source_observation_id, '00000000-0000-0000-0000-000000000000'::uuid),
    residual_kind,
    md5(reason)
  )
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS model_residual_evidence_open_idx
  ON model_residual_evidence (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS model_residual_evidence_observation_idx
  ON model_residual_evidence (tenant_id, source_observation_id, created_at DESC)
  WHERE source_observation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS model_residual_evidence_model_idx
  ON model_residual_evidence (tenant_id, model_id, created_at DESC)
  WHERE model_id IS NOT NULL;

ALTER TABLE model_residual_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_residual_evidence FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_residual_evidence;
CREATE POLICY tenant_isolation ON model_residual_evidence
  USING (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
