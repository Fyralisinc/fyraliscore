-- =====================================================================
-- 0180_byoc_evidence_package_receipts.sql
--   Sanitized BYOC control-plane evidence package receipt metadata
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS byoc_evidence_package_receipts (
  receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^evpkg_[0-9a-f]{32}$'),
  deployment_id TEXT NOT NULL CHECK (deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  customer_id TEXT NOT NULL CHECK (customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_id TEXT NOT NULL CHECK (agent_id ~ '^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_version TEXT NOT NULL CHECK (agent_version ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  artifact_revision TEXT NOT NULL CHECK (artifact_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  cloud_provider TEXT NOT NULL CHECK (
    cloud_provider IN ('aws', 'gcp', 'azure', 'customer-managed-kubernetes')
  ),
  region TEXT NOT NULL CHECK (region ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  package_digest TEXT NOT NULL CHECK (package_digest ~ '^sha256:[0-9a-f]{64}$'),
  package_generated_at TIMESTAMPTZ NOT NULL,
  ledger_overall_status TEXT NOT NULL CHECK (
    ledger_overall_status IN ('pass', 'fail', 'skipped')
  ),
  required_evidence_passed BOOLEAN NOT NULL,
  live_report_envelope_digest TEXT CHECK (
    live_report_envelope_digest IS NULL
    OR live_report_envelope_digest ~ '^sha256:[0-9a-f]{64}$'
  ),
  submitted_at TIMESTAMPTZ NOT NULL,
  accepted_at TIMESTAMPTZ NOT NULL,
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_metadata_only' CHECK (
    stored_scope = 'sanitized_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS byoc_evidence_receipts_deployment_accepted_idx
  ON byoc_evidence_package_receipts (deployment_id, accepted_at DESC);

CREATE INDEX IF NOT EXISTS byoc_evidence_receipts_customer_accepted_idx
  ON byoc_evidence_package_receipts (customer_id, accepted_at DESC);

COMMIT;
