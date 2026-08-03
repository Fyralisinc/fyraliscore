-- =====================================================================
-- 0188_byoc_control_panel_access_grants_rls.sql
--   Close the tenant-isolation gap left by migration 0185.
-- =====================================================================

BEGIN;

ALTER TABLE byoc_control_panel_access_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE byoc_control_panel_access_grants FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON byoc_control_panel_access_grants;
CREATE POLICY tenant_isolation ON byoc_control_panel_access_grants
  USING (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
