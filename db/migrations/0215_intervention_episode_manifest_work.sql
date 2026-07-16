-- =====================================================================
-- 0215_intervention_episode_manifest_work.sql
--
-- Durable, leased projection work for the InterventionEpisode link manifest.
-- Canonical stage writers remain authoritative.  This queue only causes
-- EpisodeCoordinator to link already-committed objects after revalidation.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS intervention_episode_manifest_work_items (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_event_id UUID NOT NULL REFERENCES agency_canonical_events(id),
  episode_id UUID NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN (
    'proposal', 'prediction', 'authorization', 'workflow', 'task',
    'work', 'effect', 'outcome', 'settlement', 'attribution'
  )),
  object_ref TEXT NOT NULL,
  writer_id TEXT NOT NULL,
  source_object_type TEXT NOT NULL,
  source_object_id UUID NOT NULL,
  source_object_version INTEGER NOT NULL CHECK (source_object_version > 0),
  intervention_spec_digest TEXT CHECK (
    intervention_spec_digest IS NULL
    OR intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'processing', 'retry_scheduled', 'applied', 'failed_terminal'
  )),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL,
  claimed_by TEXT,
  claim_token UUID,
  lease_expires_at TIMESTAMPTZ,
  applied_episode_version INTEGER CHECK (applied_episode_version > 0),
  last_failure_class TEXT,
  last_failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, source_event_id),
  UNIQUE (tenant_id, episode_id, stage),
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
    (status = 'applied' AND applied_episode_version IS NOT NULL)
    OR (status <> 'applied' AND applied_episode_version IS NULL)
  ),
  CHECK (
    status NOT IN ('retry_scheduled', 'failed_terminal')
    OR (
      last_failure_class IS NOT NULL
      AND last_failure_reason IS NOT NULL
    )
  ),
  CHECK (
    (stage = 'proposal' AND writer_id = 'ProposalAppender'
      AND source_object_type = 'consequential_proposal')
    OR (stage = 'prediction' AND writer_id = 'PredictionWriter'
      AND source_object_type = 'prediction')
    OR (stage = 'authorization' AND writer_id = 'AuthorizationApplier'
      AND source_object_type = 'authorization_decision')
    OR (stage IN ('workflow', 'task') AND writer_id = 'AgencyStateApplier'
      AND source_object_type IN ('workflow_run', 'task'))
    OR (stage = 'work' AND writer_id = 'WorkLedgerApplier'
      AND source_object_type = 'work_obligation')
    OR (stage = 'effect' AND writer_id = 'ExecutionLedgerApplier'
      AND source_object_type = 'external_effect_attempt')
    OR (stage = 'outcome' AND writer_id = 'OutcomeRecorder'
      AND source_object_type = 'outcome')
    OR (stage = 'settlement' AND writer_id = 'SettlementApplier'
      AND source_object_type = 'settlement')
    OR (stage = 'attribution' AND writer_id = 'AttributionApplier'
      AND source_object_type = 'attribution')
  )
);

CREATE INDEX IF NOT EXISTS intervention_episode_manifest_work_due_idx
  ON intervention_episode_manifest_work_items (
    available_at, created_at, tenant_id, id
  )
  WHERE status IN ('pending', 'retry_scheduled', 'processing');

ALTER TABLE intervention_episode_manifest_work_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE intervention_episode_manifest_work_items FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON intervention_episode_manifest_work_items;
CREATE POLICY tenant_isolation
  ON intervention_episode_manifest_work_items
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

COMMIT;
