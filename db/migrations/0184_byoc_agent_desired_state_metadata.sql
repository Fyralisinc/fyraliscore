-- =====================================================================
-- 0184_byoc_agent_desired_state_metadata.sql
--   Sanitized BYOC agent desired-state rollout metadata
-- =====================================================================

BEGIN;

ALTER TABLE byoc_agent_registrations
  ADD COLUMN IF NOT EXISTS desired_config_epoch INTEGER NOT NULL DEFAULT 0 CHECK (
    desired_config_epoch >= 0
  ),
  ADD COLUMN IF NOT EXISTS evidence_package_required BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS desired_state_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS desired_state_update_reason TEXT CHECK (
    desired_state_update_reason IS NULL
    OR desired_state_update_reason ~ '^[A-Za-z0-9_.:-]{1,100}$'
  ),
  ADD COLUMN IF NOT EXISTS desired_state_updated_by TEXT CHECK (
    desired_state_updated_by IS NULL
    OR desired_state_updated_by ~ '^[A-Za-z0-9_.:-]{1,100}$'
  );

CREATE INDEX IF NOT EXISTS byoc_agent_desired_state_update_idx
  ON byoc_agent_registrations (
    deployment_id,
    desired_config_epoch,
    desired_state_updated_at DESC
  );

COMMIT;
