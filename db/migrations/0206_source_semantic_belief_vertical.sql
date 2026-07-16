-- =====================================================================
-- 0206_source_semantic_belief_vertical.sql
--
-- First isolated source-semantics -> grounded belief vertical.  Source
-- interpretations stay append-only grounding annotations; only the narrow
-- asserted/report admission may name one canonical belief Model.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS source_semantic_interpretations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  grounding_trace_id UUID NOT NULL REFERENCES grounding_traces(id),
  source_observation_id UUID NOT NULL,
  context_snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  entity_mention_id UUID NOT NULL,
  resolution_assessment_id UUID NOT NULL REFERENCES resolution_assessments(id),
  grounding_admission_id UUID NOT NULL REFERENCES grounding_admission_decisions(id),
  source_content_hash TEXT NOT NULL CHECK (
    source_content_hash ~ '^[0-9a-f]{64}$'
  ),
  source_assertion JSONB NOT NULL CHECK (jsonb_typeof(source_assertion) = 'object'),
  semantic_frame JSONB NOT NULL CHECK (jsonb_typeof(semantic_frame) = 'object'),
  speech_act JSONB NOT NULL CHECK (jsonb_typeof(speech_act) = 'object'),
  grounding_continuity JSONB NOT NULL CHECK (
    jsonb_typeof(grounding_continuity) = 'object'
  ),
  bundle_digest TEXT NOT NULL CHECK (bundle_digest ~ '^[0-9a-f]{64}$'),
  extractor_version TEXT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, grounding_trace_id),
  UNIQUE (tenant_id, bundle_digest)
);

CREATE TABLE IF NOT EXISTS source_semantic_admission_decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  interpretation_id UUID NOT NULL UNIQUE REFERENCES source_semantic_interpretations(id),
  disposition TEXT NOT NULL CHECK (
    disposition IN ('belief_applied', 'no_admission')
  ),
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  proposed_belief_assertion JSONB,
  admitted_model_id UUID,
  decision_digest TEXT NOT NULL CHECK (decision_digest ~ '^[0-9a-f]{64}$'),
  decided_at TIMESTAMPTZ NOT NULL,
  CHECK (
    (
      disposition = 'belief_applied'
      AND jsonb_typeof(proposed_belief_assertion) = 'object'
      AND admitted_model_id IS NOT NULL
    )
    OR (
      disposition = 'no_admission'
      AND proposed_belief_assertion IS NULL
      AND admitted_model_id IS NULL
    )
  ),
  UNIQUE (tenant_id, decision_digest)
);

CREATE INDEX IF NOT EXISTS source_semantic_interpretations_observation_idx
  ON source_semantic_interpretations (
    tenant_id, source_observation_id, recorded_at DESC
  );

ALTER TABLE source_semantic_interpretations ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_semantic_interpretations FORCE ROW LEVEL SECURITY;
ALTER TABLE source_semantic_admission_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_semantic_admission_decisions FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'source_semantic_interpretations',
    'source_semantic_admission_decisions'
  ]
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', table_name);
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
      table_name
    );
  END LOOP;
END $$;

DROP TRIGGER IF EXISTS source_semantic_interpretations_immutable
  ON source_semantic_interpretations;
CREATE TRIGGER source_semantic_interpretations_immutable
  BEFORE UPDATE OR DELETE ON source_semantic_interpretations
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

DROP TRIGGER IF EXISTS source_semantic_admission_decisions_immutable
  ON source_semantic_admission_decisions;
CREATE TRIGGER source_semantic_admission_decisions_immutable
  BEFORE UPDATE OR DELETE ON source_semantic_admission_decisions
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

COMMENT ON TABLE source_semantic_interpretations IS
  'Append-only SourceAssertion/SemanticFrame/SpeechAct bundle with exact grounding continuity.';
COMMENT ON TABLE source_semantic_admission_decisions IS
  'Explicit asserted-report belief application or no-admission fate; no intent or edge authority.';

COMMIT;
