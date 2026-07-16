-- =====================================================================
-- 0218_leased_work_effect_execution.sql
--
-- Durable effect-execution plans derived from exact leased Work events.
-- ExecutionLedgerApplier remains the sole writer of effect attempts and
-- receipts; this queue records only fenced delivery and explicit result fate.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS leased_work_effect_execution_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_event_id UUID NOT NULL REFERENCES agency_canonical_events(id),
  plan_version INTEGER NOT NULL DEFAULT 1 CHECK (plan_version = 1),
  obligation_id UUID NOT NULL,
  obligation_generation INTEGER NOT NULL CHECK (obligation_generation > 0),
  source_obligation_version INTEGER NOT NULL DEFAULT 3 CHECK (
    source_obligation_version = 3
  ),
  lease_token_id UUID NOT NULL,
  lease_version INTEGER NOT NULL DEFAULT 1 CHECK (lease_version = 1),
  lease_fence INTEGER NOT NULL DEFAULT 1 CHECK (lease_fence = 1),
  task_id UUID NOT NULL,
  task_version INTEGER NOT NULL CHECK (task_version > 0),
  workflow_run_id UUID NOT NULL,
  workflow_version INTEGER NOT NULL CHECK (workflow_version > 0),
  episode_id UUID NOT NULL,
  authorization_decision_id UUID NOT NULL
    REFERENCES consequential_authorization_decisions(id),
  authorization_decision_version INTEGER NOT NULL DEFAULT 1 CHECK (
    authorization_decision_version = 1
  ),
  intervention_spec_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  capability_id UUID NOT NULL,
  capability_version TEXT NOT NULL,
  capability_digest TEXT NOT NULL CHECK (
    capability_digest ~ '^[0-9a-f]{64}$'
  ),
  effect_attempt_id UUID NOT NULL,
  effect_lineage_id UUID NOT NULL,
  operation TEXT NOT NULL,
  canonical_request_hash TEXT NOT NULL CHECK (
    canonical_request_hash ~ '^[0-9a-f]{64}$'
  ),
  provider_idempotency_key TEXT NOT NULL,
  target_grounding_refs TEXT[] NOT NULL CHECK (
    cardinality(target_grounding_refs) > 0
  ),
  reserved_at TIMESTAMPTZ NOT NULL,
  dispatch_deadline TIMESTAMPTZ NOT NULL,
  reconciliation_owner_ref TEXT NOT NULL,
  compensation_policy_ref TEXT NOT NULL,
  plan_digest TEXT NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'processing', 'retry_scheduled', 'dispatched',
    'provider_rejected', 'provider_failed', 'unknown',
    'reconciliation_required', 'failed_terminal'
  )),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL,
  claimed_by TEXT,
  claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  applied_effect_version INTEGER CHECK (applied_effect_version > 0),
  execution_receipt_id UUID,
  applied_effect_state TEXT,
  outcome_at TIMESTAMPTZ,
  last_failure_class TEXT,
  last_failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_specs (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, lease_token_id)
    REFERENCES work_lease_token_heads (tenant_id, lease_token_id),
  FOREIGN KEY (tenant_id, task_id)
    REFERENCES agency_task_heads (tenant_id, task_id),
  FOREIGN KEY (tenant_id, workflow_run_id)
    REFERENCES agency_workflow_run_heads (tenant_id, workflow_run_id),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  FOREIGN KEY (tenant_id, intervention_spec_id)
    REFERENCES consequential_intervention_specs (tenant_id, spec_id),
  FOREIGN KEY (tenant_id, capability_id)
    REFERENCES action_adapter_capability_heads (tenant_id, capability_id),
  FOREIGN KEY (execution_receipt_id) REFERENCES execution_receipts(receipt_id),
  UNIQUE (tenant_id, source_event_id),
  UNIQUE (tenant_id, obligation_id),
  UNIQUE (tenant_id, effect_attempt_id),
  UNIQUE (tenant_id, effect_lineage_id),
  UNIQUE (tenant_id, capability_id, provider_idempotency_key),
  UNIQUE (tenant_id, plan_digest),
  CHECK (dispatch_deadline > reserved_at),
  CHECK (
    (
      status = 'processing'
      AND claimed_by IS NOT NULL
      AND claim_token IS NOT NULL
      AND lease_expires_at IS NOT NULL
    )
    OR (
      status <> 'processing'
      AND claimed_by IS NULL
      AND claim_token IS NULL
      AND lease_expires_at IS NULL
    )
  ),
  CHECK (
    (
      status IN (
        'dispatched', 'provider_rejected', 'provider_failed',
        'unknown', 'reconciliation_required'
      )
      AND applied_effect_version IS NOT NULL
      AND execution_receipt_id IS NOT NULL
      AND applied_effect_state IS NOT NULL
      AND outcome_at IS NOT NULL
    )
    OR (
      status NOT IN (
        'dispatched', 'provider_rejected', 'provider_failed',
        'unknown', 'reconciliation_required'
      )
      AND applied_effect_version IS NULL
      AND execution_receipt_id IS NULL
      AND applied_effect_state IS NULL
      AND outcome_at IS NULL
    )
  ),
  CHECK (
    status NOT IN ('retry_scheduled', 'failed_terminal')
    OR (
      last_failure_class IS NOT NULL
      AND last_failure_reason IS NOT NULL
    )
  )
);

CREATE INDEX IF NOT EXISTS leased_work_effect_execution_due_idx
  ON leased_work_effect_execution_items (
    available_at, created_at, tenant_id, id
  )
  WHERE status IN ('pending', 'retry_scheduled', 'processing');

ALTER TABLE leased_work_effect_execution_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE leased_work_effect_execution_items FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON leased_work_effect_execution_items;
CREATE POLICY tenant_isolation
  ON leased_work_effect_execution_items
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON TABLE leased_work_effect_execution_items IS
  'Fenced deterministic execution plans for exact leased effect-capable Work; never canonical provider truth.';

COMMIT;
