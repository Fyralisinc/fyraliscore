-- 0213_failure_and_owner_terminalization_protocol.sql
--
-- FailureRecord, quarantine and cross-owner terminalization protocol.

BEGIN;

CREATE TABLE IF NOT EXISTS failure_record_specs (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  failure_id UUID NOT NULL,
  lineage_id UUID NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  parent_failure_id UUID,
  work_obligation_id UUID NOT NULL,
  work_obligation_generation INTEGER NOT NULL CHECK (work_obligation_generation > 0),
  causal_operation TEXT NOT NULL,
  owner_writer_id TEXT NOT NULL CHECK (owner_writer_id = 'WorkLedgerApplier'),
  semantic_owner_writer_id TEXT NOT NULL,
  target_object_type TEXT NOT NULL,
  target_object_id UUID NOT NULL,
  original_semantic_idempotency_key TEXT NOT NULL,
  maximum_attempts INTEGER NOT NULL CHECK (maximum_attempts > 0),
  deadline TIMESTAMPTZ NOT NULL,
  initial_record_digest TEXT NOT NULL CHECK (
    initial_record_digest ~ '^[0-9a-f]{64}$'
  ),
  initial_record JSONB NOT NULL CHECK (jsonb_typeof(initial_record) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, failure_id),
  UNIQUE (tenant_id, lineage_id, generation),
  FOREIGN KEY (tenant_id, parent_failure_id)
    REFERENCES failure_record_specs (tenant_id, failure_id)
      DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, work_obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  CHECK (
    (generation = 1 AND parent_failure_id IS NULL)
    OR (generation > 1 AND parent_failure_id IS NOT NULL)
  ),
  CHECK (deadline > created_at)
);

CREATE TABLE IF NOT EXISTS failure_record_lineage_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lineage_id UUID NOT NULL,
  current_failure_id UUID NOT NULL,
  current_generation INTEGER NOT NULL CHECK (current_generation > 0),
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, lineage_id),
  UNIQUE (tenant_id, current_failure_id)
);

CREATE TABLE IF NOT EXISTS failure_record_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  failure_id UUID NOT NULL,
  lineage_id UUID NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  work_obligation_id UUID NOT NULL,
  work_obligation_generation INTEGER NOT NULL CHECK (work_obligation_generation > 0),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'detected', 'classified', 'retry_scheduled', 'quarantined',
    'reconciliation_required', 'terminal_rejected', 'redrive_authorized',
    'redrive_in_progress', 'owner_terminalization_pending', 'resolved',
    'exhausted', 'escalated'
  )),
  current_record_digest TEXT NOT NULL CHECK (
    current_record_digest ~ '^[0-9a-f]{64}$'
  ),
  current_owner_terminalization_request_id UUID,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, failure_id),
  FOREIGN KEY (tenant_id, failure_id)
    REFERENCES failure_record_specs (tenant_id, failure_id),
  FOREIGN KEY (tenant_id, lineage_id)
    REFERENCES failure_record_lineage_heads (tenant_id, lineage_id)
      DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, work_obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  UNIQUE (tenant_id, lineage_id, generation)
);

ALTER TABLE failure_record_lineage_heads
  DROP CONSTRAINT IF EXISTS failure_record_lineage_heads_current_fk;
ALTER TABLE failure_record_lineage_heads
  ADD CONSTRAINT failure_record_lineage_heads_current_fk
  FOREIGN KEY (tenant_id, current_failure_id)
    REFERENCES failure_record_heads (tenant_id, failure_id)
      DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS failure_record_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  failure_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  record_digest TEXT NOT NULL CHECK (record_digest ~ '^[0-9a-f]{64}$'),
  record JSONB NOT NULL CHECK (jsonb_typeof(record) = 'object'),
  transition_kind TEXT NOT NULL,
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, failure_id)
    REFERENCES failure_record_heads (tenant_id, failure_id),
  UNIQUE (tenant_id, failure_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS owner_terminalization_requests (
  request_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  failure_id UUID NOT NULL,
  failure_generation INTEGER NOT NULL CHECK (failure_generation > 0),
  failure_version INTEGER NOT NULL CHECK (failure_version > 0),
  work_obligation_id UUID NOT NULL,
  work_obligation_generation INTEGER NOT NULL CHECK (work_obligation_generation > 0),
  work_obligation_version INTEGER NOT NULL CHECK (work_obligation_version > 0),
  semantic_owner_writer_id TEXT NOT NULL,
  target_object_type TEXT NOT NULL,
  target_object_id UUID NOT NULL,
  request JSONB NOT NULL CHECK (jsonb_typeof(request) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  requested_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, failure_id)
    REFERENCES failure_record_heads (tenant_id, failure_id),
  FOREIGN KEY (tenant_id, work_obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  UNIQUE (tenant_id, failure_id, request_digest),
  UNIQUE (tenant_id, request_id)
);

ALTER TABLE failure_record_heads
  DROP CONSTRAINT IF EXISTS failure_record_heads_current_request_fk;
ALTER TABLE failure_record_heads
  ADD CONSTRAINT failure_record_heads_current_request_fk
  FOREIGN KEY (tenant_id, current_owner_terminalization_request_id)
    REFERENCES owner_terminalization_requests(tenant_id, request_id)
      DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS owner_terminalization_resolutions (
  resolution_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  resolution_digest TEXT NOT NULL CHECK (resolution_digest ~ '^[0-9a-f]{64}$'),
  request_id UUID NOT NULL,
  failure_id UUID NOT NULL,
  failure_generation INTEGER NOT NULL CHECK (failure_generation > 0),
  failure_version INTEGER NOT NULL CHECK (failure_version > 0),
  work_obligation_id UUID NOT NULL,
  work_obligation_generation INTEGER NOT NULL CHECK (work_obligation_generation > 0),
  work_obligation_version INTEGER NOT NULL CHECK (work_obligation_version > 0),
  owner_command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  resolution JSONB NOT NULL CHECK (jsonb_typeof(resolution) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  resolved_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, failure_id)
    REFERENCES failure_record_heads (tenant_id, failure_id),
  FOREIGN KEY (tenant_id, work_obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, request_id)
    REFERENCES owner_terminalization_requests (tenant_id, request_id),
  UNIQUE (tenant_id, request_id),
  UNIQUE (tenant_id, owner_command_result_id)
);

CREATE INDEX IF NOT EXISTS failure_record_due_idx
  ON failure_record_heads (tenant_id, updated_at)
  WHERE current_state IN (
    'detected', 'classified', 'retry_scheduled', 'quarantined',
    'reconciliation_required', 'redrive_authorized', 'redrive_in_progress',
    'owner_terminalization_pending'
  );

CREATE INDEX IF NOT EXISTS owner_terminalization_pending_idx
  ON owner_terminalization_requests (tenant_id, requested_at);

DO $$
DECLARE
  t TEXT;
  trigger_name TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'failure_record_specs',
    'failure_record_versions',
    'owner_terminalization_requests',
    'owner_terminalization_resolutions'
  ]
  LOOP
    trigger_name := 'reject_' || t || '_mutation';
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger WHERE tgname=trigger_name AND NOT tgisinternal
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION reject_execution_immutable_mutation()',
        trigger_name,
        t
      );
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'failure_record_specs',
    'failure_record_lineage_heads',
    'failure_record_heads',
    'failure_record_versions',
    'owner_terminalization_requests',
    'owner_terminalization_resolutions'
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

COMMIT;
