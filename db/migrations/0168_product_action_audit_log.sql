-- =====================================================================
-- 0168_product_action_audit_log.sql
-- =====================================================================
-- Durable tenant-scoped audit trail for user-facing product actions that
-- mutate product state or accept autonomous output.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS product_action_audit_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  actor_id UUID NOT NULL REFERENCES actors(id),
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT product_action_audit_log_action_check CHECK (
    action IN (
      'decision_delta.accept',
      'decision_delta.delegate',
      'decision_delta.contest',
      'decision_delta.add_context',
      'decision_delta.promote_from_recommendation'
    )
  )
);

CREATE INDEX IF NOT EXISTS product_action_audit_log_tenant_time_idx
  ON product_action_audit_log (tenant_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS product_action_audit_log_actor_idx
  ON product_action_audit_log (tenant_id, actor_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS product_action_audit_log_resource_idx
  ON product_action_audit_log (tenant_id, resource_type, resource_id, occurred_at DESC);

ALTER TABLE product_action_audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE product_action_audit_log FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON product_action_audit_log;
CREATE POLICY tenant_isolation ON product_action_audit_log
  USING (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
