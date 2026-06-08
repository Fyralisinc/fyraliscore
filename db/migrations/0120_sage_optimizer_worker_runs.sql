-- 0120_sage_optimizer_worker_runs.sql
--
-- Operational checkpoint for the SAGE topology optimizer worker.
-- The optimizer updates utility-layer state from inquiry outcome events,
-- so polling workers must not reprocess the same inquiry session on every
-- tick. This table records one durable optimization attempt per session.

BEGIN;

CREATE TABLE IF NOT EXISTS sage_topology_optimizer_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  trigger_event TEXT NOT NULL DEFAULT 'worker_poll',
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  error TEXT,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  UNIQUE (tenant_id, inquiry_session_id)
);

CREATE INDEX IF NOT EXISTS sage_topology_optimizer_runs_tenant_status_idx
  ON sage_topology_optimizer_runs (tenant_id, status, started_at DESC);

CREATE INDEX IF NOT EXISTS sage_topology_optimizer_runs_completed_idx
  ON sage_topology_optimizer_runs (completed_at DESC)
  WHERE status = 'completed';

ALTER TABLE sage_topology_optimizer_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_topology_optimizer_runs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON sage_topology_optimizer_runs;
CREATE POLICY tenant_isolation ON sage_topology_optimizer_runs
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;

