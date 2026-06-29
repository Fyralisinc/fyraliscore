-- =====================================================================
-- 0182_byoc_runner_evidence_receipts.sql
--   Sanitized BYOC data-plane runner evidence receipt metadata
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS byoc_runner_evidence_receipts (
  receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^runev_[0-9a-f]{32}$'),
  deployment_id TEXT NOT NULL CHECK (deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  customer_id TEXT NOT NULL CHECK (customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_id TEXT NOT NULL CHECK (agent_id ~ '^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_version TEXT NOT NULL CHECK (agent_version ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  cloud_provider TEXT NOT NULL CHECK (
    cloud_provider IN ('aws', 'gcp', 'azure', 'customer-managed-kubernetes')
  ),
  region TEXT NOT NULL CHECK (region ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  control_plane_mode TEXT NOT NULL CHECK (control_plane_mode IN ('mock', 'live')),
  evidence_digest TEXT NOT NULL CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
  current_artifact_revision TEXT NOT NULL CHECK (
    current_artifact_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'
  ),
  desired_revision TEXT NOT NULL CHECK (desired_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  rollout_action TEXT NOT NULL CHECK (rollout_action IN ('none', 'apply_revision')),
  runner_status TEXT NOT NULL CHECK (runner_status IN ('pass', 'fail')),
  required_checks_passed BOOLEAN NOT NULL,
  apply_plan_count INTEGER NOT NULL CHECK (apply_plan_count >= 0),
  artifact_verification_count INTEGER NOT NULL CHECK (artifact_verification_count >= 0),
  digest_pinned_artifact_count INTEGER NOT NULL CHECK (digest_pinned_artifact_count >= 0),
  local_digest_checked_count INTEGER NOT NULL CHECK (local_digest_checked_count >= 0),
  submitted_at TIMESTAMPTZ NOT NULL,
  accepted_at TIMESTAMPTZ NOT NULL,
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_metadata_only' CHECK (
    stored_scope = 'sanitized_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS byoc_runner_evidence_deployment_accepted_idx
  ON byoc_runner_evidence_receipts (deployment_id, accepted_at DESC);

CREATE INDEX IF NOT EXISTS byoc_runner_evidence_customer_accepted_idx
  ON byoc_runner_evidence_receipts (customer_id, accepted_at DESC);

COMMIT;
