-- =====================================================================
-- 0183_byoc_preflight_report_receipts.sql
--   Sanitized BYOC preflight report receipt metadata
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS byoc_preflight_report_receipts (
  receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^pfrep_[0-9a-f]{32}$'),
  deployment_id TEXT NOT NULL CHECK (deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  customer_id TEXT NOT NULL CHECK (customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_id TEXT NOT NULL CHECK (agent_id ~ '^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_version TEXT NOT NULL CHECK (agent_version ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  artifact_revision TEXT NOT NULL CHECK (artifact_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  cloud_provider TEXT NOT NULL CHECK (
    cloud_provider IN ('aws', 'gcp', 'azure', 'customer-managed-kubernetes')
  ),
  region TEXT NOT NULL CHECK (region ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  report_digest TEXT NOT NULL CHECK (report_digest ~ '^sha256:[0-9a-f]{64}$'),
  preflight_status TEXT NOT NULL CHECK (
    preflight_status IN ('pass', 'fail', 'skipped')
  ),
  required_sections_passed BOOLEAN NOT NULL,
  section_count INTEGER NOT NULL CHECK (section_count >= 0),
  failed_section_count INTEGER NOT NULL CHECK (failed_section_count >= 0),
  terraform_validate_executed BOOLEAN NOT NULL,
  submitted_at TIMESTAMPTZ NOT NULL,
  accepted_at TIMESTAMPTZ NOT NULL,
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_metadata_only' CHECK (
    stored_scope = 'sanitized_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS byoc_preflight_reports_deployment_accepted_idx
  ON byoc_preflight_report_receipts (deployment_id, accepted_at DESC);

CREATE INDEX IF NOT EXISTS byoc_preflight_reports_customer_accepted_idx
  ON byoc_preflight_report_receipts (customer_id, accepted_at DESC);

COMMIT;
