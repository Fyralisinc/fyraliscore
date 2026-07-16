-- =====================================================================
-- 0204_conversation_context_selection_protocol.sql
--
-- Durable candidate -> probe -> selection state for Slack-like evidence
-- neighborhoods.  Source events remain evidence; temporary conversation
-- hypotheses remain embedded search state; only the selected snapshot is a
-- canonical grounding annotation.
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

ALTER TABLE interpretation_context_snapshots
  ALTER COLUMN focal_observation_id DROP NOT NULL,
  ADD COLUMN IF NOT EXISTS selection_key TEXT,
  ADD COLUMN IF NOT EXISTS aggregate_version INTEGER,
  ADD COLUMN IF NOT EXISTS focal_event_revision_ids TEXT[],
  ADD COLUMN IF NOT EXISTS interpretation_mode TEXT,
  ADD COLUMN IF NOT EXISTS selection_dependency JSONB,
  ADD COLUMN IF NOT EXISTS candidate_manifest_digest TEXT,
  ADD COLUMN IF NOT EXISTS probe_manifest_digest TEXT,
  ADD COLUMN IF NOT EXISTS selection_decision_digest TEXT,
  ADD COLUMN IF NOT EXISTS supersedes_snapshot_id UUID,
  ADD COLUMN IF NOT EXISTS command_result_id UUID,
  ADD COLUMN IF NOT EXISTS contract_version TEXT NOT NULL
    DEFAULT 'legacy-grounding-sidecar-v1';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'interpretation_context_snapshots'::regclass
      AND conname = 'interpretation_context_v2_shape_check'
  ) THEN
    ALTER TABLE interpretation_context_snapshots
      ADD CONSTRAINT interpretation_context_v2_shape_check CHECK (
        contract_version <> 'conversation-context-selection-v1'
        OR (
          selection_key ~ '^[0-9a-f]{64}$'
          AND aggregate_version > 0
          AND cardinality(focal_event_revision_ids) > 0
          AND interpretation_mode IN (
            'as_known_at_cutoff', 'retrospective_current'
          )
          AND jsonb_typeof(selection_dependency) = 'object'
          AND candidate_manifest_digest ~ '^[0-9a-f]{64}$'
          AND probe_manifest_digest ~ '^[0-9a-f]{64}$'
          AND selection_decision_digest ~ '^[0-9a-f]{64}$'
          AND command_result_id IS NOT NULL
        )
      );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'interpretation_context_snapshots'::regclass
      AND conname = 'interpretation_context_supersedes_snapshot_fkey'
  ) THEN
    ALTER TABLE interpretation_context_snapshots
      ADD CONSTRAINT interpretation_context_supersedes_snapshot_fkey
      FOREIGN KEY (supersedes_snapshot_id)
      REFERENCES interpretation_context_snapshots(id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'interpretation_context_snapshots'::regclass
      AND conname = 'interpretation_context_command_result_fkey'
  ) THEN
    ALTER TABLE interpretation_context_snapshots
      ADD CONSTRAINT interpretation_context_command_result_fkey
      FOREIGN KEY (command_result_id) REFERENCES agency_command_results(id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS interpretation_context_selection_version_uq
  ON interpretation_context_snapshots (
    tenant_id, selection_key, aggregate_version
  )
  WHERE contract_version = 'conversation-context-selection-v1';

CREATE UNIQUE INDEX IF NOT EXISTS interpretation_context_selection_digest_uq
  ON interpretation_context_snapshots (
    tenant_id, selection_key, snapshot_content_hash
  )
  WHERE contract_version = 'conversation-context-selection-v1';

CREATE TABLE IF NOT EXISTS interpretation_context_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  selection_key TEXT NOT NULL CHECK (selection_key ~ '^[0-9a-f]{64}$'),
  selection_subject TEXT NOT NULL,
  focal_event_revision_ids TEXT[] NOT NULL CHECK (
    cardinality(focal_event_revision_ids) > 0
  ),
  purpose TEXT NOT NULL,
  operation TEXT NOT NULL,
  interpretation_mode TEXT NOT NULL CHECK (
    interpretation_mode IN ('as_known_at_cutoff', 'retrospective_current')
  ),
  source_partition TEXT NOT NULL,
  current_aggregate_version INTEGER NOT NULL CHECK (
    current_aggregate_version > 0
  ),
  current_snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  current_snapshot_digest TEXT NOT NULL CHECK (
    current_snapshot_digest ~ '^[0-9a-f]{64}$'
  ),
  current_decision_digest TEXT NOT NULL CHECK (
    current_decision_digest ~ '^[0-9a-f]{64}$'
  ),
  current_command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, selection_key)
);

CREATE TABLE IF NOT EXISTS conversation_context_candidate_records (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  selection_key TEXT NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  snapshot_id UUID NOT NULL REFERENCES interpretation_context_snapshots(id),
  candidate_id UUID NOT NULL,
  candidate_content_hash TEXT NOT NULL CHECK (
    candidate_content_hash ~ '^[0-9a-f]{64}$'
  ),
  selected BOOLEAN NOT NULL,
  eligible BOOLEAN NOT NULL,
  layer_coverage TEXT[] NOT NULL CHECK (cardinality(layer_coverage) > 0),
  cost JSONB NOT NULL CHECK (jsonb_typeof(cost) = 'object'),
  candidate JSONB NOT NULL CHECK (jsonb_typeof(candidate) = 'object'),
  probe JSONB NOT NULL CHECK (jsonb_typeof(probe) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  recorded_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, selection_key)
    REFERENCES interpretation_context_heads(tenant_id, selection_key),
  UNIQUE (tenant_id, selection_key, aggregate_version, candidate_id)
);

CREATE INDEX IF NOT EXISTS conversation_context_candidates_snapshot_idx
  ON conversation_context_candidate_records (tenant_id, snapshot_id, candidate_id);

ALTER TABLE interpretation_context_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE interpretation_context_heads FORCE ROW LEVEL SECURITY;
ALTER TABLE conversation_context_candidate_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversation_context_candidate_records FORCE ROW LEVEL SECURITY;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'interpretation_context_heads',
    'conversation_context_candidate_records'
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

DROP TRIGGER IF EXISTS interpretation_context_snapshots_immutable
  ON interpretation_context_snapshots;
CREATE TRIGGER interpretation_context_snapshots_immutable
  BEFORE UPDATE OR DELETE ON interpretation_context_snapshots
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

DROP TRIGGER IF EXISTS conversation_context_candidate_records_immutable
  ON conversation_context_candidate_records;
CREATE TRIGGER conversation_context_candidate_records_immutable
  BEFORE UPDATE OR DELETE ON conversation_context_candidate_records
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

COMMENT ON TABLE interpretation_context_heads IS
  'Current purpose/cutoff-relative conversational context selection head; history remains append-only in interpretation_context_snapshots.';
COMMENT ON TABLE conversation_context_candidate_records IS
  'Neutral pre-truth candidate and probe audit records; selected status never promotes temporary episode hypotheses into company evidence.';

COMMIT;
