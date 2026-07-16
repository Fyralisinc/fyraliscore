-- =====================================================================
-- 0205_entity_mention_detection_protocol.sql
--
-- Versioned mention detection over exact evidence coordinates.  Candidate
-- phrase opportunities receive one durable detected or rejected fate before
-- candidate identity resolution.  Mentions remain grounding annotations and
-- cannot mutate source Evidence or the canonical entity registry.
-- =====================================================================

BEGIN;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'agency_command_results'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%writer_id%'
  LOOP
    EXECUTE format(
      'ALTER TABLE agency_command_results DROP CONSTRAINT %I', constraint_name
    );
  END LOOP;
  ALTER TABLE agency_command_results
    ADD CONSTRAINT agency_command_results_writer_id_check
    CHECK (writer_id IN (
      'ProposalAppender', 'EpisodeCoordinator', 'PredictionWriter',
      'AuthorizationApplier', 'OutcomeRecorder', 'SettlementApplier',
      'AttributionApplier', 'PolicyRegistryApplier', 'AgencyStateApplier',
      'WorkLedgerApplier', 'ExecutionLedgerApplier', 'RepairLedgerApplier',
      'WriterEpochApplier', 'GroundingAnnotationAppender'
    ));
END $$;

CREATE TABLE IF NOT EXISTS entity_mention_detections (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  detection_key TEXT NOT NULL CHECK (detection_key ~ '^[0-9a-f]{64}$'),
  detection_version INTEGER NOT NULL CHECK (detection_version > 0),
  source_observation_id UUID NOT NULL,
  source_revision_id TEXT NOT NULL,
  candidate_surface TEXT NOT NULL,
  context_snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  context_snapshot_digest TEXT NOT NULL CHECK (
    context_snapshot_digest ~ '^[0-9a-f]{64}$'
  ),
  source_content_hash TEXT NOT NULL CHECK (
    source_content_hash ~ '^[0-9a-f]{64}$'
  ),
  fate TEXT NOT NULL CHECK (
    fate IN (
      'detected', 'rejected_not_anchored', 'rejected_not_entity',
      'unsupported_implicit'
    )
  ),
  mention_id UUID,
  mention JSONB,
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  extractor_version TEXT NOT NULL,
  detection_digest TEXT NOT NULL CHECK (detection_digest ~ '^[0-9a-f]{64}$'),
  supersedes_detection_id UUID REFERENCES entity_mention_detections(id),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  detected_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL,
  CHECK (
    (fate = 'detected' AND mention_id IS NOT NULL AND jsonb_typeof(mention) = 'object')
    OR (fate <> 'detected' AND mention_id IS NULL AND mention IS NULL)
  ),
  UNIQUE (tenant_id, detection_key, detection_version),
  UNIQUE (tenant_id, mention_id)
);

CREATE TABLE IF NOT EXISTS entity_mention_detection_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  detection_key TEXT NOT NULL CHECK (detection_key ~ '^[0-9a-f]{64}$'),
  source_observation_id UUID NOT NULL,
  source_revision_id TEXT NOT NULL,
  candidate_surface TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  current_detection_version INTEGER NOT NULL CHECK (
    current_detection_version > 0
  ),
  current_detection_id UUID NOT NULL REFERENCES entity_mention_detections(id),
  current_detection_digest TEXT NOT NULL CHECK (
    current_detection_digest ~ '^[0-9a-f]{64}$'
  ),
  current_command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, detection_key)
);

ALTER TABLE entity_candidate_generation_requests
  ADD COLUMN IF NOT EXISTS entity_mention_detection_id UUID,
  ADD COLUMN IF NOT EXISTS entity_mention_id UUID;

ALTER TABLE grounding_traces
  ADD COLUMN IF NOT EXISTS entity_mention_detection_id UUID,
  ADD COLUMN IF NOT EXISTS entity_mention_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'entity_candidate_generation_requests'::regclass
      AND conname = 'candidate_request_mention_detection_fkey'
  ) THEN
    ALTER TABLE entity_candidate_generation_requests
      ADD CONSTRAINT candidate_request_mention_detection_fkey
      FOREIGN KEY (entity_mention_detection_id)
      REFERENCES entity_mention_detections(id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'grounding_traces'::regclass
      AND conname = 'grounding_trace_mention_detection_fkey'
  ) THEN
    ALTER TABLE grounding_traces
      ADD CONSTRAINT grounding_trace_mention_detection_fkey
      FOREIGN KEY (entity_mention_detection_id)
      REFERENCES entity_mention_detections(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS entity_mention_detections_observation_idx
  ON entity_mention_detections (
    tenant_id, source_observation_id, candidate_surface, detected_at
  );

ALTER TABLE entity_mention_detections ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_mention_detections FORCE ROW LEVEL SECURITY;
ALTER TABLE entity_mention_detection_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_mention_detection_heads FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'entity_mention_detections',
    'entity_mention_detection_heads'
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

DROP TRIGGER IF EXISTS entity_mention_detections_immutable
  ON entity_mention_detections;
CREATE TRIGGER entity_mention_detections_immutable
  BEFORE UPDATE OR DELETE ON entity_mention_detections
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

COMMENT ON TABLE entity_mention_detections IS
  'Append-only detected or rejected mention fate over exact source coordinates; never canonical entity truth.';
COMMENT ON TABLE entity_mention_detection_heads IS
  'Current extractor-relative mention-detection head; correction history remains immutable.';

COMMIT;
