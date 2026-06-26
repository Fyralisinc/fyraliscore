-- =====================================================================
-- 0181_byoc_agent_registrations.sql
--   Sanitized BYOC data-plane agent registration and heartbeat metadata
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS byoc_agent_registrations (
  deployment_id TEXT NOT NULL CHECK (deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  customer_id TEXT NOT NULL CHECK (customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_id TEXT NOT NULL CHECK (agent_id ~ '^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  agent_version TEXT NOT NULL CHECK (agent_version ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  artifact_revision TEXT NOT NULL CHECK (artifact_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  cloud_provider TEXT NOT NULL CHECK (
    cloud_provider IN ('aws', 'gcp', 'azure', 'customer-managed-kubernetes')
  ),
  region TEXT NOT NULL CHECK (region ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  install_token_secret_ref TEXT NOT NULL CHECK (
    install_token_secret_ref <> ''
    AND install_token_secret_ref !~ '://'
  ),
  desired_revision TEXT NOT NULL CHECK (desired_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  heartbeat_interval_seconds INTEGER NOT NULL CHECK (
    heartbeat_interval_seconds BETWEEN 5 AND 300
  ),
  telemetry_contract TEXT NOT NULL CHECK (telemetry_contract ~ '^[A-Za-z0-9_.:-]{1,100}$'),
  enrolled_at TIMESTAMPTZ NOT NULL,
  latest_heartbeat_sequence BIGINT CHECK (
    latest_heartbeat_sequence IS NULL OR latest_heartbeat_sequence >= 0
  ),
  latest_validation_status TEXT CHECK (
    latest_validation_status IS NULL
    OR latest_validation_status IN ('unknown', 'passing', 'degraded', 'failing')
  ),
  latest_control_plane_connected BOOLEAN,
  latest_telemetry_mode TEXT CHECK (
    latest_telemetry_mode IS NULL
    OR latest_telemetry_mode IN ('aggregate-only', 'disabled')
  ),
  latest_telemetry_contract TEXT CHECK (
    latest_telemetry_contract IS NULL
    OR latest_telemetry_contract ~ '^[A-Za-z0-9_.:-]{1,100}$'
  ),
  latest_component_count INTEGER CHECK (
    latest_component_count IS NULL OR latest_component_count >= 0
  ),
  latest_ok_component_count INTEGER CHECK (
    latest_ok_component_count IS NULL OR latest_ok_component_count >= 0
  ),
  latest_degraded_component_count INTEGER CHECK (
    latest_degraded_component_count IS NULL OR latest_degraded_component_count >= 0
  ),
  latest_failed_component_count INTEGER CHECK (
    latest_failed_component_count IS NULL OR latest_failed_component_count >= 0
  ),
  latest_unknown_component_count INTEGER CHECK (
    latest_unknown_component_count IS NULL OR latest_unknown_component_count >= 0
  ),
  latest_queued_batches BIGINT CHECK (
    latest_queued_batches IS NULL OR latest_queued_batches >= 0
  ),
  latest_dropped_batches BIGINT CHECK (
    latest_dropped_batches IS NULL OR latest_dropped_batches >= 0
  ),
  latest_heartbeat_sent_at TIMESTAMPTZ,
  latest_heartbeat_accepted_at TIMESTAMPTZ,
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_agent_metadata_only' CHECK (
    stored_scope = 'sanitized_agent_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, customer_id, agent_id)
);

CREATE INDEX IF NOT EXISTS byoc_agent_registrations_customer_idx
  ON byoc_agent_registrations (customer_id, deployment_id, agent_id);

CREATE INDEX IF NOT EXISTS byoc_agent_registrations_latest_heartbeat_idx
  ON byoc_agent_registrations (deployment_id, latest_heartbeat_accepted_at DESC);

COMMIT;
