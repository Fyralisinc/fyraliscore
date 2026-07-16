-- =====================================================================
-- 0217_registered_work_scheduling.sql
--
-- Durable scheduling plans derived from exact version-one registered Work
-- events.  The queue owns delivery state only. WorkLedgerApplier remains the
-- sole writer of Work decisions, Work lifecycle, and active lease truth.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS registered_work_scheduling_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_event_id UUID NOT NULL REFERENCES agency_canonical_events(id),
  plan_version INTEGER NOT NULL DEFAULT 1 CHECK (plan_version = 1),
  obligation_id UUID NOT NULL,
  obligation_generation INTEGER NOT NULL CHECK (obligation_generation > 0),
  source_obligation_version INTEGER NOT NULL DEFAULT 1 CHECK (
    source_obligation_version = 1
  ),
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
  decision_id UUID NOT NULL,
  lease_token_id UUID NOT NULL,
  selected_processing_class TEXT NOT NULL CHECK (
    selected_processing_class IN ('R0', 'R1', 'R2', 'R3', 'R4', 'R5')
  ),
  scheduled_at TIMESTAMPTZ NOT NULL,
  lease_owner_ref TEXT NOT NULL,
  planned_heartbeat_deadline TIMESTAMPTZ NOT NULL,
  planned_work_lease_expires_at TIMESTAMPTZ NOT NULL,
  policy_version_ref TEXT NOT NULL,
  plan_digest TEXT NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'processing', 'retry_scheduled', 'leased',
    'work_expired', 'authorization_expired', 'failed_terminal'
  )),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL,
  claimed_by TEXT,
  claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  eligible_work_version INTEGER CHECK (eligible_work_version > 0),
  leased_work_version INTEGER CHECK (leased_work_version > 0),
  applied_lease_version INTEGER CHECK (applied_lease_version > 0),
  applied_lease_fence INTEGER CHECK (applied_lease_fence > 0),
  leased_at TIMESTAMPTZ,
  expired_work_version INTEGER CHECK (expired_work_version > 0),
  work_expired_at TIMESTAMPTZ,
  authorization_expired_at TIMESTAMPTZ,
  last_failure_class TEXT,
  last_failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, obligation_id)
    REFERENCES work_obligation_specs (tenant_id, obligation_id),
  FOREIGN KEY (tenant_id, task_id)
    REFERENCES agency_task_heads (tenant_id, task_id),
  FOREIGN KEY (tenant_id, workflow_run_id)
    REFERENCES agency_workflow_run_heads (tenant_id, workflow_run_id),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  FOREIGN KEY (tenant_id, intervention_spec_id)
    REFERENCES consequential_intervention_specs (tenant_id, spec_id),
  UNIQUE (tenant_id, source_event_id),
  UNIQUE (tenant_id, obligation_id),
  UNIQUE (tenant_id, decision_id),
  UNIQUE (tenant_id, lease_token_id),
  UNIQUE (tenant_id, plan_digest),
  CHECK (
    planned_heartbeat_deadline <= planned_work_lease_expires_at
  ),
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
      status = 'leased'
      AND eligible_work_version IS NOT NULL
      AND leased_work_version IS NOT NULL
      AND applied_lease_version IS NOT NULL
      AND applied_lease_fence IS NOT NULL
      AND leased_at IS NOT NULL
    )
    OR (
      status <> 'leased'
      AND eligible_work_version IS NULL
      AND leased_work_version IS NULL
      AND applied_lease_version IS NULL
      AND applied_lease_fence IS NULL
      AND leased_at IS NULL
    )
  ),
  CHECK (
    (
      status IN ('work_expired', 'authorization_expired')
      AND expired_work_version IS NOT NULL
    )
    OR (
      status NOT IN ('work_expired', 'authorization_expired')
      AND expired_work_version IS NULL
    )
  ),
  CHECK (
    (status = 'work_expired' AND work_expired_at IS NOT NULL)
    OR (status <> 'work_expired' AND work_expired_at IS NULL)
  ),
  CHECK (
    (
      status = 'authorization_expired'
      AND authorization_expired_at IS NOT NULL
    )
    OR (
      status <> 'authorization_expired'
      AND authorization_expired_at IS NULL
    )
  ),
  CHECK (
    status NOT IN (
      'retry_scheduled', 'work_expired',
      'authorization_expired', 'failed_terminal'
    )
    OR (
      last_failure_class IS NOT NULL
      AND last_failure_reason IS NOT NULL
    )
  )
);

CREATE INDEX IF NOT EXISTS registered_work_scheduling_due_idx
  ON registered_work_scheduling_items (
    available_at, created_at, tenant_id, id
  )
  WHERE status IN ('pending', 'retry_scheduled', 'processing');

ALTER TABLE registered_work_scheduling_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE registered_work_scheduling_items FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON registered_work_scheduling_items;
CREATE POLICY tenant_isolation
  ON registered_work_scheduling_items
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

COMMENT ON TABLE registered_work_scheduling_items IS
  'Leased deterministic scheduler plans for exact registered task Work; WorkLedgerApplier remains canonical.';

COMMIT;
