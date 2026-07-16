-- =====================================================================
-- 0216_authorized_agency_activation_work.sql
--
-- Durable authorization-to-planned-agency activation work.  This runtime
-- queue consumes only the immutable version-one AuthorizationApplier event.
-- It freezes a deterministic WorkflowRun/Task activation plan without
-- inheriting, widening, or otherwise minting action authority.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS authorized_agency_activation_work_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_event_id UUID NOT NULL REFERENCES agency_canonical_events(id),
  plan_version INTEGER NOT NULL DEFAULT 1 CHECK (plan_version = 1),
  authorization_decision_id UUID NOT NULL
    REFERENCES consequential_authorization_decisions(id),
  authorization_decision_version INTEGER NOT NULL DEFAULT 1 CHECK (
    authorization_decision_version = 1
  ),
  proposal_id UUID NOT NULL,
  proposal_version INTEGER NOT NULL CHECK (proposal_version > 0),
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  episode_id UUID NOT NULL,
  intervention_spec_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  workflow_run_id UUID NOT NULL,
  task_id UUID NOT NULL,
  activation_at TIMESTAMPTZ NOT NULL,
  workflow_spec_version_ref TEXT NOT NULL,
  exact_target_ref TEXT NOT NULL,
  plan_digest TEXT NOT NULL CHECK (plan_digest ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'processing', 'retry_scheduled', 'activated',
    'authorization_expired', 'failed_terminal'
  )),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL,
  claimed_by TEXT,
  claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  activated_workflow_version INTEGER CHECK (activated_workflow_version > 0),
  activated_task_version INTEGER CHECK (activated_task_version > 0),
  activated_at TIMESTAMPTZ,
  authorization_expired_at TIMESTAMPTZ,
  last_failure_class TEXT,
  last_failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, proposal_id, proposal_version)
    REFERENCES consequential_proposals (tenant_id, id, proposal_version),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  FOREIGN KEY (tenant_id, intervention_spec_id)
    REFERENCES consequential_intervention_specs (tenant_id, spec_id),
  UNIQUE (tenant_id, source_event_id),
  UNIQUE (tenant_id, authorization_decision_id),
  UNIQUE (tenant_id, plan_digest),
  UNIQUE (tenant_id, workflow_run_id),
  UNIQUE (tenant_id, task_id),
  CHECK (workflow_run_id <> task_id),
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
      status = 'activated'
      AND activated_workflow_version IS NOT NULL
      AND activated_task_version IS NOT NULL
      AND activated_at IS NOT NULL
    )
    OR (
      status <> 'activated'
      AND activated_workflow_version IS NULL
      AND activated_task_version IS NULL
      AND activated_at IS NULL
    )
  ),
  CHECK (
    (status = 'authorization_expired' AND authorization_expired_at IS NOT NULL)
    OR (status <> 'authorization_expired' AND authorization_expired_at IS NULL)
  ),
  CHECK (
    status NOT IN ('retry_scheduled', 'failed_terminal')
    OR (
      last_failure_class IS NOT NULL
      AND last_failure_reason IS NOT NULL
    )
  )
);

CREATE INDEX IF NOT EXISTS authorized_agency_activation_work_due_idx
  ON authorized_agency_activation_work_items (
    available_at, created_at, tenant_id, id
  )
  WHERE status IN ('pending', 'retry_scheduled', 'processing');

ALTER TABLE authorized_agency_activation_work_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE authorized_agency_activation_work_items FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON authorized_agency_activation_work_items;
CREATE POLICY tenant_isolation
  ON authorized_agency_activation_work_items
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

COMMENT ON TABLE authorized_agency_activation_work_items IS
  'Leased deterministic activation plans derived from exact authorized version-one canonical events; never an authority source.';

COMMIT;
