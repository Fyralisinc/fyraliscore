-- 0211_projection_delta_inquiry.sql
--
-- Stateful projection maintenance.
--
-- Projection snapshots remain disposable operating views, but richer
-- projection inquiry needs durable refresh work, dependency refs, watch
-- frontiers, and per-subject mining state so normal maintenance can process
-- deltas instead of sweeping the whole memory graph.

BEGIN;

CREATE TABLE IF NOT EXISTS projection_refresh_jobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL DEFAULT 'v1',
  subject_key TEXT NOT NULL,
  reason TEXT NOT NULL,
  event_ids UUID[] NOT NULL DEFAULT '{}',
  dependency_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  leased_at TIMESTAMPTZ,
  lease_token UUID,
  processed_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT projection_refresh_jobs_status_valid CHECK (
    status IN ('pending', 'leased', 'processed', 'failed', 'dead_letter')
  ),
  CONSTRAINT projection_refresh_jobs_attempts_valid CHECK (
    attempts >= 0 AND max_attempts > 0
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS projection_refresh_jobs_pending_unique_idx
  ON projection_refresh_jobs (
    tenant_id, projection_name, projection_version, subject_key
  )
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS projection_refresh_jobs_ready_idx
  ON projection_refresh_jobs (tenant_id, scheduled_at, created_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS projection_refresh_jobs_status_idx
  ON projection_refresh_jobs (tenant_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS projection_dependencies (
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL DEFAULT 'v1',
  subject_key TEXT NOT NULL,
  ref_kind TEXT NOT NULL,
  ref_value TEXT NOT NULL,
  reason TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    tenant_id, projection_name, projection_version, subject_key,
    ref_kind, ref_value
  )
);

CREATE INDEX IF NOT EXISTS projection_dependencies_ref_idx
  ON projection_dependencies (tenant_id, ref_kind, ref_value);

CREATE TABLE IF NOT EXISTS projection_watch_keys (
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL DEFAULT 'v1',
  subject_key TEXT NOT NULL,
  watch_kind TEXT NOT NULL,
  watch_value TEXT NOT NULL,
  reason TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    tenant_id, projection_name, projection_version, subject_key,
    watch_kind, watch_value
  )
);

CREATE INDEX IF NOT EXISTS projection_watch_keys_lookup_idx
  ON projection_watch_keys (tenant_id, watch_kind, watch_value);

CREATE TABLE IF NOT EXISTS projection_inquiry_state (
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL DEFAULT 'v1',
  subject_key TEXT NOT NULL,
  last_mined_event_id UUID,
  last_mined_event_created_at TIMESTAMPTZ,
  evidence_digest TEXT,
  watch_fingerprint TEXT,
  state_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, projection_name, projection_version, subject_key)
);

CREATE INDEX IF NOT EXISTS projection_inquiry_state_updated_idx
  ON projection_inquiry_state (tenant_id, updated_at DESC);

ALTER TABLE projection_refresh_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_refresh_jobs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_refresh_jobs;
CREATE POLICY tenant_isolation ON projection_refresh_jobs
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

ALTER TABLE projection_dependencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_dependencies FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_dependencies;
CREATE POLICY tenant_isolation ON projection_dependencies
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

ALTER TABLE projection_watch_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_watch_keys FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_watch_keys;
CREATE POLICY tenant_isolation ON projection_watch_keys
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

ALTER TABLE projection_inquiry_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_inquiry_state FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_inquiry_state;
CREATE POLICY tenant_isolation ON projection_inquiry_state
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

COMMIT;
