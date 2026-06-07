-- =====================================================================
-- 0059_resolution_threads.sql — Resolution Tracker backend
-- =====================================================================
-- A Resolution Thread is a monitored state-change contract attached to
-- a situation node and/or the Decision Delta that created it. It stores
-- the durable operational path from "Fyralis found this" to "the
-- company state actually changed".
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS resolution_threads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  source_decision_delta_id UUID REFERENCES decision_deltas(id) ON DELETE SET NULL,
  target_node_kind TEXT,
  target_node_id UUID,
  title TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'draft', 'active', 'waiting_on_owner', 'blocked',
    'monitoring', 'confirmed', 'resolved', 'failed'
  )),
  current_state TEXT NOT NULL,
  target_state TEXT NOT NULL,
  owner_label TEXT NOT NULL,
  next_review_at TIMESTAMPTZ,
  success_criteria JSONB NOT NULL DEFAULT '[]'::jsonb,
  escalation_triggers JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_by UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS resolution_threads_source_delta_uniq
  ON resolution_threads (tenant_id, source_decision_delta_id)
  WHERE source_decision_delta_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS resolution_threads_tenant_status_idx
  ON resolution_threads (tenant_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS resolution_threads_target_idx
  ON resolution_threads (tenant_id, target_node_kind, target_node_id)
  WHERE target_node_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resolution_thread_steps (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  thread_id UUID NOT NULL REFERENCES resolution_threads(id) ON DELETE CASCADE,
  label TEXT NOT NULL,
  owner_label TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'not_started', 'in_progress', 'waiting', 'blocked', 'done', 'failed'
  )),
  due_at TIMESTAMPTZ,
  proof_needed TEXT,
  blocked_by TEXT,
  ordinal INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS resolution_thread_steps_thread_idx
  ON resolution_thread_steps (thread_id, ordinal);

CREATE TABLE IF NOT EXISTS resolution_thread_watched_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  thread_id UUID NOT NULL REFERENCES resolution_threads(id) ON DELETE CASCADE,
  label TEXT NOT NULL,
  source_type TEXT NOT NULL,
  expected TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'watching', 'seen', 'missing', 'contradicted'
  )),
  last_observed_at TIMESTAMPTZ,
  matched_evidence JSONB,
  ordinal INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS resolution_thread_signals_thread_idx
  ON resolution_thread_watched_signals (thread_id, ordinal);

CREATE TABLE IF NOT EXISTS resolution_thread_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  thread_id UUID NOT NULL REFERENCES resolution_threads(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  actor_id UUID,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS resolution_thread_events_thread_idx
  ON resolution_thread_events (thread_id, created_at DESC);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'tenants'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'resolution_threads_tenant_fk'
  ) THEN
    ALTER TABLE resolution_threads
      ADD CONSTRAINT resolution_threads_tenant_fk
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
      DEFERRABLE INITIALLY IMMEDIATE;
  END IF;
END $$;

ALTER TABLE resolution_threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE resolution_threads FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON resolution_threads;
CREATE POLICY tenant_isolation ON resolution_threads
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE resolution_thread_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE resolution_thread_steps FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON resolution_thread_steps;
CREATE POLICY tenant_isolation ON resolution_thread_steps
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE resolution_thread_watched_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE resolution_thread_watched_signals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON resolution_thread_watched_signals;
CREATE POLICY tenant_isolation ON resolution_thread_watched_signals
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE resolution_thread_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE resolution_thread_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON resolution_thread_events;
CREATE POLICY tenant_isolation ON resolution_thread_events
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

CREATE OR REPLACE FUNCTION resolution_threads_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS resolution_threads_updated_at_trg ON resolution_threads;
CREATE TRIGGER resolution_threads_updated_at_trg
  BEFORE UPDATE ON resolution_threads
  FOR EACH ROW
  EXECUTE FUNCTION resolution_threads_touch_updated_at();

DROP TRIGGER IF EXISTS resolution_thread_steps_updated_at_trg ON resolution_thread_steps;
CREATE TRIGGER resolution_thread_steps_updated_at_trg
  BEFORE UPDATE ON resolution_thread_steps
  FOR EACH ROW
  EXECUTE FUNCTION resolution_threads_touch_updated_at();

DROP TRIGGER IF EXISTS resolution_thread_signals_updated_at_trg ON resolution_thread_watched_signals;
CREATE TRIGGER resolution_thread_signals_updated_at_trg
  BEFORE UPDATE ON resolution_thread_watched_signals
  FOR EACH ROW
  EXECUTE FUNCTION resolution_threads_touch_updated_at();

COMMIT;
