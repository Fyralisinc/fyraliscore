-- =====================================================================
-- 0212_workflow_work_and_external_effect_ledgers.sql
--
-- Three separate canonical responsibilities:
--   * AgencyStateApplier: business WorkflowRun and Task state;
--   * WorkLedgerApplier: scheduling, generation and lease fencing;
--   * ExecutionLedgerApplier: adapter guarantees, effect attempts and receipts.
-- Task completion is not an Outcome, and dispatch intent is committed before
-- any provider call so an unknown result can never masquerade as no effect.
-- =====================================================================

BEGIN;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  IF to_regclass('public.agency_command_results') IS NULL THEN
    RAISE EXCEPTION '0194 consequential agency protocol must be installed first';
  END IF;
  FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'agency_command_results'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%writer_id%'
  LOOP
    EXECUTE format(
      'ALTER TABLE agency_command_results DROP CONSTRAINT %I',
      constraint_name
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

CREATE TABLE IF NOT EXISTS action_adapter_capability_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  capability_id UUID NOT NULL,
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_capability_version TEXT NOT NULL,
  current_capability_digest TEXT NOT NULL CHECK (
    current_capability_digest ~ '^[0-9a-f]{64}$'
  ),
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, capability_id)
);

CREATE TABLE IF NOT EXISTS action_adapter_capability_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  capability_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  capability_version TEXT NOT NULL,
  capability_digest TEXT NOT NULL CHECK (capability_digest ~ '^[0-9a-f]{64}$'),
  adapter_name TEXT NOT NULL,
  provider_name TEXT NOT NULL,
  verified_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  capabilities JSONB NOT NULL CHECK (jsonb_typeof(capabilities) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, capability_id)
    REFERENCES action_adapter_capability_heads (tenant_id, capability_id),
  UNIQUE (tenant_id, capability_id, aggregate_version),
  UNIQUE (tenant_id, capability_id, capability_version),
  UNIQUE (tenant_id, capability_digest),
  CHECK (expires_at > verified_at)
);

CREATE TABLE IF NOT EXISTS agency_workflow_run_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  workflow_run_id UUID NOT NULL,
  episode_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'planned', 'active', 'blocked', 'suspended', 'completed', 'failed',
    'cancelled', 'expired'
  )),
  current_snapshot_digest TEXT NOT NULL CHECK (
    current_snapshot_digest ~ '^[0-9a-f]{64}$'
  ),
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, workflow_run_id),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id)
);

CREATE TABLE IF NOT EXISTS agency_workflow_run_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  workflow_run_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL CHECK (snapshot_digest ~ '^[0-9a-f]{64}$'),
  snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, workflow_run_id)
    REFERENCES agency_workflow_run_heads (tenant_id, workflow_run_id),
  UNIQUE (tenant_id, workflow_run_id, aggregate_version),
  UNIQUE (tenant_id, command_result_id)
);

CREATE TABLE IF NOT EXISTS agency_task_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  task_id UUID NOT NULL,
  workflow_run_id UUID NOT NULL,
  episode_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'planned', 'ready', 'in_progress', 'blocked', 'completed', 'failed',
    'skipped', 'cancelled', 'expired'
  )),
  current_snapshot_digest TEXT NOT NULL CHECK (
    current_snapshot_digest ~ '^[0-9a-f]{64}$'
  ),
  external_effect_required BOOLEAN NOT NULL,
  current_effect_attempt_id UUID,
  current_execution_receipt_id UUID,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, task_id),
  FOREIGN KEY (tenant_id, workflow_run_id)
    REFERENCES agency_workflow_run_heads (tenant_id, workflow_run_id),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id)
);

CREATE TABLE IF NOT EXISTS agency_task_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  task_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  snapshot_digest TEXT NOT NULL CHECK (snapshot_digest ~ '^[0-9a-f]{64}$'),
  snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, task_id)
    REFERENCES agency_task_heads (tenant_id, task_id),
  UNIQUE (tenant_id, task_id, aggregate_version),
  UNIQUE (tenant_id, command_result_id)
);

CREATE TABLE IF NOT EXISTS work_obligation_specs (
  obligation_id UUID NOT NULL,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lineage_id UUID NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  parent_obligation_id UUID,
  semantic_dedupe_key TEXT NOT NULL,
  obligation_digest TEXT NOT NULL CHECK (obligation_digest ~ '^[0-9a-f]{64}$'),
  target_object_type TEXT NOT NULL,
  target_object_id UUID NOT NULL,
  owner_writer_id TEXT NOT NULL,
  purpose TEXT NOT NULL,
  risk_tier TEXT NOT NULL,
  maximum_attempts INTEGER NOT NULL CHECK (maximum_attempts > 0),
  deadline TIMESTAMPTZ NOT NULL,
  effect_possible BOOLEAN NOT NULL,
  obligation JSONB NOT NULL CHECK (jsonb_typeof(obligation) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  registered_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, obligation_id),
  UNIQUE (tenant_id, lineage_id, generation),
  UNIQUE (tenant_id, semantic_dedupe_key, generation),
  UNIQUE (tenant_id, obligation_digest),
  FOREIGN KEY (tenant_id, parent_obligation_id)
    REFERENCES work_obligation_specs (tenant_id, obligation_id)
      DEFERRABLE INITIALLY DEFERRED,
  CHECK (
    (generation = 1 AND parent_obligation_id IS NULL)
    OR (generation > 1 AND parent_obligation_id IS NOT NULL)
  ),
  CHECK (deadline > registered_at)
);

CREATE TABLE IF NOT EXISTS work_obligation_lineage_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lineage_id UUID NOT NULL,
  current_obligation_id UUID NOT NULL,
  current_generation INTEGER NOT NULL CHECK (current_generation > 0),
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, lineage_id),
  FOREIGN KEY (tenant_id, current_obligation_id)
    REFERENCES work_obligation_specs (tenant_id, obligation_id)
      DEFERRABLE INITIALLY DEFERRED,
  UNIQUE (tenant_id, current_obligation_id)
);

CREATE TABLE IF NOT EXISTS work_obligation_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  obligation_id UUID NOT NULL,
  lineage_id UUID NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'registered', 'eligible', 'deferred', 'suppressed', 'rejected',
    'cancelled', 'expired', 'leased', 'completed', 'no_op', 'retry_wait',
    'quarantined', 'reconciliation_required', 'lease_lost',
    'redrive_authorized', 'superseded_by_new_generation',
    'owner_terminalization_pending', 'exhausted', 'escalated'
  )),
  current_lease_token_id UUID,
  current_fence INTEGER NOT NULL DEFAULT 0 CHECK (current_fence >= 0),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_eligible_at TIMESTAMPTZ,
  wake_predicate TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_specs (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, lineage_id)
    REFERENCES work_obligation_lineage_heads (tenant_id, lineage_id)
      DEFERRABLE INITIALLY DEFERRED,
  UNIQUE (tenant_id, lineage_id, generation)
);

CREATE TABLE IF NOT EXISTS work_obligation_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  obligation_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  transition_kind TEXT NOT NULL,
  transition_payload JSONB NOT NULL CHECK (
    jsonb_typeof(transition_payload) = 'object'
  ),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  UNIQUE (tenant_id, obligation_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS work_decisions (
  decision_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  obligation_id UUID NOT NULL,
  obligation_generation INTEGER NOT NULL CHECK (obligation_generation > 0),
  obligation_version INTEGER NOT NULL CHECK (obligation_version > 0),
  decision_digest TEXT NOT NULL CHECK (decision_digest ~ '^[0-9a-f]{64}$'),
  selected_processing_class TEXT NOT NULL CHECK (
    selected_processing_class IN ('R0', 'R1', 'R2', 'R3', 'R4', 'R5')
  ),
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  decision JSONB NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  decided_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  UNIQUE (tenant_id, decision_digest)
);

CREATE TABLE IF NOT EXISTS work_lease_token_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lease_token_id UUID NOT NULL,
  obligation_id UUID NOT NULL,
  obligation_generation INTEGER NOT NULL CHECK (obligation_generation > 0),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'active', 'completed', 'released', 'expired', 'revoked',
    'superseded_by_new_lease', 'reconciliation_required', 'terminal'
  )),
  fence INTEGER NOT NULL CHECK (fence > 0),
  attempt INTEGER NOT NULL CHECK (attempt > 0),
  owner_ref TEXT NOT NULL,
  heartbeat_deadline TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  effect_possible BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, lease_token_id),
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  UNIQUE (tenant_id, obligation_id, fence)
);

CREATE UNIQUE INDEX IF NOT EXISTS work_one_active_lease_per_obligation_idx
  ON work_lease_token_heads (tenant_id, obligation_id)
  WHERE current_state = 'active';

CREATE TABLE IF NOT EXISTS work_lease_token_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lease_token_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  lease_payload JSONB NOT NULL CHECK (jsonb_typeof(lease_payload) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, lease_token_id)
    REFERENCES work_lease_token_heads (tenant_id, lease_token_id),
  UNIQUE (tenant_id, lease_token_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS external_effect_provider_keys (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  capability_id UUID NOT NULL,
  provider_idempotency_key TEXT NOT NULL,
  lineage_id UUID NOT NULL,
  canonical_request_hash TEXT NOT NULL CHECK (
    canonical_request_hash ~ '^[0-9a-f]{64}$'
  ),
  registered_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, capability_id, provider_idempotency_key),
  FOREIGN KEY (tenant_id, capability_id)
    REFERENCES action_adapter_capability_heads (tenant_id, capability_id)
);

CREATE TABLE IF NOT EXISTS external_effect_attempt_lineage_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  lineage_id UUID NOT NULL,
  current_effect_attempt_id UUID NOT NULL,
  current_generation INTEGER NOT NULL CHECK (current_generation > 0),
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, lineage_id),
  UNIQUE (tenant_id, current_effect_attempt_id)
);

CREATE TABLE IF NOT EXISTS external_effect_attempt_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  effect_attempt_id UUID NOT NULL,
  lineage_id UUID NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  prior_attempt_id UUID,
  episode_id UUID NOT NULL,
  task_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  authorization_decision_id UUID NOT NULL REFERENCES consequential_authorization_decisions(id),
  capability_id UUID NOT NULL,
  capability_version TEXT NOT NULL,
  capability_digest TEXT NOT NULL CHECK (capability_digest ~ '^[0-9a-f]{64}$'),
  operation TEXT NOT NULL,
  canonical_request_hash TEXT NOT NULL CHECK (
    canonical_request_hash ~ '^[0-9a-f]{64}$'
  ),
  provider_idempotency_key TEXT NOT NULL,
  work_obligation_id UUID NOT NULL,
  work_obligation_generation INTEGER NOT NULL CHECK (work_obligation_generation > 0),
  lease_token_id UUID NOT NULL,
  lease_fence INTEGER NOT NULL CHECK (lease_fence > 0),
  dispatch_deadline TIMESTAMPTZ NOT NULL,
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'reserved', 'cancelled', 'expired', 'dispatch_intent_recorded',
    'acknowledged', 'rejected', 'unknown', 'reconciling', 'succeeded',
    'failed', 'partially_executed', 'reconciled_no_effect',
    'terminal_partial', 'compensation_proposed', 'compensation_authorized',
    'compensation_rejected', 'compensation_expired',
    'compensation_attempt_linked', 'compensated', 'compensation_failed',
    'compensation_unknown', 'compensation_reconciling'
  )),
  current_attempt_digest TEXT NOT NULL CHECK (
    current_attempt_digest ~ '^[0-9a-f]{64}$'
  ),
  reserved_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, effect_attempt_id),
  FOREIGN KEY (tenant_id, lineage_id)
    REFERENCES external_effect_attempt_lineage_heads (tenant_id, lineage_id)
      DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, prior_attempt_id)
    REFERENCES external_effect_attempt_heads (tenant_id, effect_attempt_id)
      DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  FOREIGN KEY (tenant_id, task_id)
    REFERENCES agency_task_heads (tenant_id, task_id),
  FOREIGN KEY (tenant_id, capability_id)
    REFERENCES action_adapter_capability_heads (tenant_id, capability_id),
  FOREIGN KEY (tenant_id, work_obligation_id)
    REFERENCES work_obligation_heads (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, lease_token_id)
    REFERENCES work_lease_token_heads (tenant_id, lease_token_id),
  UNIQUE (tenant_id, lineage_id, generation),
  CHECK (
    (generation = 1 AND prior_attempt_id IS NULL)
    OR (generation > 1 AND prior_attempt_id IS NOT NULL)
  ),
  CHECK (dispatch_deadline > reserved_at)
);

ALTER TABLE external_effect_attempt_lineage_heads
  DROP CONSTRAINT IF EXISTS external_effect_attempt_lineage_heads_current_fk;
ALTER TABLE external_effect_attempt_lineage_heads
  ADD CONSTRAINT external_effect_attempt_lineage_heads_current_fk
  FOREIGN KEY (tenant_id, current_effect_attempt_id)
    REFERENCES external_effect_attempt_heads (tenant_id, effect_attempt_id)
      DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS external_effect_attempt_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  effect_attempt_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL,
  transition_kind TEXT NOT NULL,
  attempt_payload JSONB NOT NULL CHECK (jsonb_typeof(attempt_payload) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, effect_attempt_id)
    REFERENCES external_effect_attempt_heads (tenant_id, effect_attempt_id),
  UNIQUE (tenant_id, effect_attempt_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS execution_receipts (
  receipt_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  effect_attempt_id UUID NOT NULL,
  effect_version INTEGER NOT NULL CHECK (effect_version >= 2),
  effect_state TEXT NOT NULL,
  receipt_digest TEXT NOT NULL CHECK (receipt_digest ~ '^[0-9a-f]{64}$'),
  requested BOOLEAN NOT NULL CHECK (requested),
  provider_accepted BOOLEAN,
  externally_observed BOOLEAN NOT NULL,
  partial BOOLEAN NOT NULL,
  reconciled BOOLEAN NOT NULL,
  compensated BOOLEAN NOT NULL,
  receipt JSONB NOT NULL CHECK (jsonb_typeof(receipt) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  observed_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, effect_attempt_id)
    REFERENCES external_effect_attempt_heads (tenant_id, effect_attempt_id),
  UNIQUE (tenant_id, effect_attempt_id, effect_version),
  UNIQUE (tenant_id, receipt_digest),
  UNIQUE (tenant_id, command_result_id)
);

CREATE INDEX IF NOT EXISTS work_obligation_due_idx
  ON work_obligation_heads (tenant_id, next_eligible_at, updated_at)
  WHERE current_state IN ('registered', 'eligible', 'deferred', 'retry_wait');
CREATE INDEX IF NOT EXISTS work_lease_expiry_idx
  ON work_lease_token_heads (tenant_id, expires_at)
  WHERE current_state = 'active';
CREATE INDEX IF NOT EXISTS external_effect_reconciliation_idx
  ON external_effect_attempt_heads (tenant_id, updated_at)
  WHERE current_state IN (
    'unknown', 'reconciling', 'partially_executed',
    'compensation_unknown', 'compensation_reconciling'
  );

CREATE OR REPLACE FUNCTION reject_execution_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is append-only; use a governed successor transition',
    TG_TABLE_NAME USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

DO $$
DECLARE
  t TEXT;
  trigger_name TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'action_adapter_capability_versions',
    'agency_workflow_run_versions',
    'agency_task_versions',
    'work_obligation_specs',
    'work_obligation_versions',
    'work_decisions',
    'work_lease_token_versions',
    'external_effect_provider_keys',
    'external_effect_attempt_versions',
    'execution_receipts'
  ]
  LOOP
    trigger_name := 'reject_' || t || '_mutation';
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger WHERE tgname = trigger_name AND NOT tgisinternal
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
    'action_adapter_capability_heads',
    'action_adapter_capability_versions',
    'agency_workflow_run_heads',
    'agency_workflow_run_versions',
    'agency_task_heads',
    'agency_task_versions',
    'work_obligation_specs',
    'work_obligation_lineage_heads',
    'work_obligation_heads',
    'work_obligation_versions',
    'work_decisions',
    'work_lease_token_heads',
    'work_lease_token_versions',
    'external_effect_provider_keys',
    'external_effect_attempt_lineage_heads',
    'external_effect_attempt_heads',
    'external_effect_attempt_versions',
    'execution_receipts'
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
