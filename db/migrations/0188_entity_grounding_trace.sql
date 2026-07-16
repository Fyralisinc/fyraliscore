-- =====================================================================
-- 0188_entity_grounding_trace.sql
--
-- Durable, append-only company-physics grounding sidecars.  These tables
-- deliberately separate source context, candidate generation, evidence-
-- relative assessment, consumer-specific admission, and downstream fate.
-- Resolver/model output is not company evidence and none of these tables is
-- an authority to mutate canonical identity or the source Observation.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS interpretation_context_snapshots (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  focal_observation_id UUID NOT NULL,
  phrase TEXT NOT NULL,
  snapshot_version INTEGER NOT NULL DEFAULT 1 CHECK (snapshot_version > 0),
  source_channel TEXT NOT NULL,
  source_space TEXT NOT NULL,
  evidence_cutoff TIMESTAMPTZ NOT NULL,
  processing_authority_fingerprint TEXT NOT NULL CHECK (
    processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  snapshot_content_hash TEXT NOT NULL CHECK (
    snapshot_content_hash ~ '^[0-9a-f]{64}$'
  ),
  snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id, snapshot_version)
);

CREATE TABLE IF NOT EXISTS entity_candidate_generation_requests (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  context_snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  source_observation_id UUID NOT NULL,
  phrase TEXT NOT NULL,
  mention_ref TEXT NOT NULL,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  processing_authority_fingerprint TEXT NOT NULL CHECK (
    processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  required_lanes TEXT[] NOT NULL CHECK (cardinality(required_lanes) > 0),
  request JSONB NOT NULL CHECK (jsonb_typeof(request) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, request_digest)
);

CREATE TABLE IF NOT EXISTS entity_candidate_sets (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  request_id UUID NOT NULL UNIQUE REFERENCES entity_candidate_generation_requests(id),
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  candidate_set_version INTEGER NOT NULL DEFAULT 1 CHECK (candidate_set_version > 0),
  lane_fates JSONB NOT NULL CHECK (jsonb_typeof(lane_fates) = 'array'),
  candidates JSONB NOT NULL CHECK (jsonb_typeof(candidates) = 'array'),
  candidate_set_hash TEXT NOT NULL CHECK (candidate_set_hash ~ '^[0-9a-f]{64}$'),
  candidate_set JSONB NOT NULL CHECK (jsonb_typeof(candidate_set) = 'object'),
  registry_version TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, request_digest),
  UNIQUE (tenant_id, id, candidate_set_version)
);

CREATE TABLE IF NOT EXISTS resolution_assessments (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  candidate_set_id UUID NOT NULL REFERENCES entity_candidate_sets(id),
  assessment_version INTEGER NOT NULL DEFAULT 1 CHECK (assessment_version > 0),
  candidate_distribution JSONB NOT NULL CHECK (
    jsonb_typeof(candidate_distribution) = 'object'
  ),
  selected_candidate_id TEXT,
  suggested_canonical_ref JSONB,
  model_output JSONB NOT NULL CHECK (jsonb_typeof(model_output) = 'object'),
  assessment JSONB NOT NULL CHECK (jsonb_typeof(assessment) = 'object'),
  scorer_and_calibration_version TEXT NOT NULL,
  assessed_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id, assessment_version)
);

CREATE TABLE IF NOT EXISTS grounding_admission_decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  assessment_id UUID NOT NULL REFERENCES resolution_assessments(id),
  decision_version INTEGER NOT NULL DEFAULT 1 CHECK (decision_version > 0),
  consumer TEXT NOT NULL,
  purpose TEXT NOT NULL,
  operation TEXT NOT NULL,
  risk_tier TEXT NOT NULL,
  disposition TEXT NOT NULL CHECK (
    disposition IN (
      'single_referent', 'candidate_distribution', 'mention_local_only',
      'clarification', 'review', 'abstention'
    )
  ),
  selected_referent JSONB,
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  consumption_authority_fingerprint TEXT NOT NULL CHECK (
    consumption_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  decision JSONB NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
  decided_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, assessment_id, consumer, purpose, operation, decision_version)
);

CREATE TABLE IF NOT EXISTS grounding_traces (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_observation_id UUID NOT NULL,
  phrase TEXT NOT NULL,
  context_snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  candidate_request_id UUID NOT NULL REFERENCES entity_candidate_generation_requests(id),
  candidate_set_id UUID NOT NULL REFERENCES entity_candidate_sets(id),
  resolution_assessment_id UUID NOT NULL REFERENCES resolution_assessments(id),
  grounding_admission_id UUID NOT NULL REFERENCES grounding_admission_decisions(id),
  current_fate TEXT NOT NULL CHECK (
    current_fate IN ('resolved_for_consumer', 'review', 'unresolved', 'abstained')
  ),
  selected_referent JSONB,
  identity_registry_mutated BOOLEAN NOT NULL DEFAULT FALSE,
  source_observation_mutated BOOLEAN NOT NULL DEFAULT FALSE,
  trace JSONB NOT NULL CHECK (jsonb_typeof(trace) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS interpretation_context_observation_idx
  ON interpretation_context_snapshots (tenant_id, focal_observation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS candidate_requests_observation_idx
  ON entity_candidate_generation_requests (tenant_id, source_observation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS resolution_assessments_set_idx
  ON resolution_assessments (tenant_id, candidate_set_id, assessed_at DESC);
CREATE INDEX IF NOT EXISTS grounding_admissions_assessment_idx
  ON grounding_admission_decisions (tenant_id, assessment_id, decided_at DESC);
CREATE INDEX IF NOT EXISTS grounding_traces_observation_idx
  ON grounding_traces (tenant_id, source_observation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS grounding_traces_open_fate_idx
  ON grounding_traces (tenant_id, current_fate, created_at DESC)
  WHERE current_fate IN ('review', 'unresolved', 'abstained');

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'interpretation_context_snapshots',
    'entity_candidate_generation_requests',
    'entity_candidate_sets',
    'resolution_assessments',
    'grounding_admission_decisions',
    'grounding_traces'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') '
      'WITH CHECK ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      t
    );
  END LOOP;
END $$;

COMMENT ON TABLE grounding_traces IS
  'Append-only grounding continuity; model output never mutates evidence or canonical identity.';
COMMENT ON COLUMN grounding_traces.identity_registry_mutated IS
  'Must remain false for resolver-only assessment; identity appliers own genuine bindings and lifecycle mutations.';
COMMENT ON COLUMN grounding_traces.source_observation_mutated IS
  'Must remain false: grounding is a sidecar and cannot rewrite source evidence.';

COMMIT;
