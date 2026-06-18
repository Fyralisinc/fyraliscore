-- 0153_substrate_candidates.sql
--
-- Durable provisional substrate discovered during Think context assembly.
--
-- These rows are deliberately not canonical actors/customers/resources/acts.
-- They are evidence-backed "this probably exists" handles that the reasoning
-- layer may scope Models to while preserving uncertainty. Promotion/merge into
-- canonical tables remains an explicit later step.

BEGIN;

CREATE TABLE IF NOT EXISTS substrate_candidates (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (
    kind IN (
      'actor',
      'actor_alias',
      'customer',
      'workstream',
      'system',
      'vendor',
      'commitment',
      'pattern'
    )
  ),
  label TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'proposed' CHECK (
    status IN (
      'proposed',
      'needs_clarification',
      'promoted',
      'rejected',
      'merged',
      'stale'
    )
  ),
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  fingerprint TEXT NOT NULL,
  aliases JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
    jsonb_typeof(aliases) = 'array'
  ),
  evidence_observation_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  related_candidate_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  proposed_canonical_ref JSONB,
  promotion_ref JSONB,
  merge_target_id UUID REFERENCES substrate_candidates(id) ON DELETE SET NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
    jsonb_typeof(metadata) = 'object'
  ),
  created_by_run_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, kind, fingerprint)
);

CREATE INDEX IF NOT EXISTS substrate_candidates_tenant_status_kind_idx
  ON substrate_candidates (tenant_id, status, kind, updated_at DESC);

CREATE INDEX IF NOT EXISTS substrate_candidates_evidence_observations_idx
  ON substrate_candidates USING gin (evidence_observation_ids);

CREATE INDEX IF NOT EXISTS substrate_candidates_evidence_models_idx
  ON substrate_candidates USING gin (evidence_model_ids);

CREATE INDEX IF NOT EXISTS substrate_candidates_aliases_gin
  ON substrate_candidates USING gin (aliases);

CREATE INDEX IF NOT EXISTS substrate_candidates_metadata_gin
  ON substrate_candidates USING gin (metadata);

ALTER TABLE substrate_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE substrate_candidates FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON substrate_candidates;
CREATE POLICY tenant_isolation ON substrate_candidates
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
      ON TABLE substrate_candidates TO company_os;
  END IF;
END
$$;

COMMIT;
