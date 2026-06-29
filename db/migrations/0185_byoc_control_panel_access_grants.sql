-- =====================================================================
-- 0185_byoc_control_panel_access_grants.sql
--   Sanitized BYOC control-panel access grant metadata
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS byoc_control_panel_access_grants (
  tenant_id UUID NOT NULL,
  customer_id TEXT NOT NULL CHECK (customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  deployment_id TEXT NOT NULL CHECK (deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'),
  role TEXT NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
  enabled BOOLEAN NOT NULL DEFAULT true,
  granted_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ CHECK (
    expires_at IS NULL OR expires_at > granted_at
  ),
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_control_panel_access_metadata_only' CHECK (
    stored_scope = 'sanitized_control_panel_access_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, customer_id, deployment_id)
);

CREATE INDEX IF NOT EXISTS byoc_control_panel_access_deployment_idx
  ON byoc_control_panel_access_grants (tenant_id, deployment_id)
  WHERE enabled;

CREATE INDEX IF NOT EXISTS byoc_control_panel_access_customer_idx
  ON byoc_control_panel_access_grants (tenant_id, customer_id, deployment_id);

COMMIT;
