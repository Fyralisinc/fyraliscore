-- =====================================================================
-- 0060_ask_fyralis.sql — Ask Fyralis overlay persistence
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS ask_sessions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  viewer_id UUID NOT NULL,
  initial_scope JSONB NOT NULL,
  current_scope JSONB NOT NULL,
  source_route TEXT,
  source_object_id UUID,
  source_object_type TEXT,
  mode TEXT NOT NULL CHECK (mode IN (
    'direct_synthesis_read', 'quick_inquiry', 'deep_inquiry', 'background_review'
  )),
  status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_sessions_tenant_viewer_idx
  ON ask_sessions (tenant_id, viewer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ask_sessions_source_object_idx
  ON ask_sessions (tenant_id, source_object_id)
  WHERE source_object_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ask_messages (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES ask_sessions(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  content TEXT NOT NULL,
  structured_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_messages_session_idx
  ON ask_messages (session_id, created_at);

CREATE TABLE IF NOT EXISTS ask_scopes (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES ask_sessions(id) ON DELETE CASCADE,
  scope_type TEXT NOT NULL,
  label TEXT NOT NULL,
  root_nodes UUID[] NOT NULL DEFAULT '{}',
  related_entities UUID[] NOT NULL DEFAULT '{}',
  filters JSONB NOT NULL DEFAULT '{}'::jsonb,
  access_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_scopes_session_idx
  ON ask_scopes (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ask_retrieval_runs (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES ask_sessions(id) ON DELETE CASCADE,
  message_id UUID REFERENCES ask_messages(id) ON DELETE SET NULL,
  intent TEXT NOT NULL,
  retrieval_plan JSONB NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN (
    'direct_synthesis_read', 'quick_inquiry', 'deep_inquiry', 'background_review'
  )),
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  latency_ms INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_retrieval_runs_session_idx
  ON ask_retrieval_runs (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ask_evidence_items (
  id UUID PRIMARY KEY,
  retrieval_run_id UUID NOT NULL REFERENCES ask_retrieval_runs(id) ON DELETE CASCADE,
  source_ref UUID,
  source_kind TEXT NOT NULL,
  summary TEXT NOT NULL,
  strength TEXT CHECK (
    strength IS NULL OR strength IN (
      'decisive', 'supporting', 'contextual', 'weak', 'counterevidence', 'unknown'
    )
  ),
  supports_answer BOOLEAN NOT NULL DEFAULT FALSE,
  is_counterevidence BOOLEAN NOT NULL DEFAULT FALSE,
  token_estimate INTEGER,
  access_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  omitted_reason TEXT,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_evidence_items_run_idx
  ON ask_evidence_items (retrieval_run_id, created_at);

CREATE INDEX IF NOT EXISTS ask_evidence_items_source_idx
  ON ask_evidence_items (source_kind, source_ref)
  WHERE source_ref IS NOT NULL;

CREATE TABLE IF NOT EXISTS ask_answers (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES ask_sessions(id) ON DELETE CASCADE,
  message_id UUID REFERENCES ask_messages(id) ON DELETE SET NULL,
  retrieval_run_id UUID REFERENCES ask_retrieval_runs(id) ON DELETE SET NULL,
  answer_payload JSONB NOT NULL,
  confidence DOUBLE PRECISION,
  mode TEXT NOT NULL CHECK (mode IN (
    'direct_synthesis_read', 'quick_inquiry', 'deep_inquiry', 'background_review'
  )),
  scope JSONB NOT NULL,
  token_estimate INTEGER,
  latency_ms INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_answers_session_idx
  ON ask_answers (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ask_proposed_state_changes (
  id UUID PRIMARY KEY,
  answer_id UUID NOT NULL REFERENCES ask_answers(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  proposed_op JSONB NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN (
      'proposed', 'accepted', 'rejected', 'delegated',
      'applied', 'failed_validation'
    )
  ),
  linked_trigger_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_proposed_state_changes_tenant_status_idx
  ON ask_proposed_state_changes (tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS ask_feedback (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES ask_sessions(id) ON DELETE CASCADE,
  answer_id UUID REFERENCES ask_answers(id) ON DELETE SET NULL,
  viewer_id UUID NOT NULL,
  feedback_type TEXT NOT NULL CHECK (
    feedback_type IN (
      'helpful', 'wrong', 'missing_context', 'too_verbose', 'unsafe', 'irrelevant'
    )
  ),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ask_feedback_session_idx
  ON ask_feedback (session_id, created_at DESC);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'tenants'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint WHERE conname = 'ask_sessions_tenant_fk'
    ) THEN
      ALTER TABLE ask_sessions
        ADD CONSTRAINT ask_sessions_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        DEFERRABLE INITIALLY IMMEDIATE;
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint WHERE conname = 'ask_state_changes_tenant_fk'
    ) THEN
      ALTER TABLE ask_proposed_state_changes
        ADD CONSTRAINT ask_state_changes_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants(id)
        DEFERRABLE INITIALLY IMMEDIATE;
    END IF;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION ask_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ask_sessions_updated_at_trg ON ask_sessions;
CREATE TRIGGER ask_sessions_updated_at_trg
  BEFORE UPDATE ON ask_sessions
  FOR EACH ROW EXECUTE FUNCTION ask_touch_updated_at();

DROP TRIGGER IF EXISTS ask_state_changes_updated_at_trg ON ask_proposed_state_changes;
CREATE TRIGGER ask_state_changes_updated_at_trg
  BEFORE UPDATE ON ask_proposed_state_changes
  FOR EACH ROW EXECUTE FUNCTION ask_touch_updated_at();

ALTER TABLE ask_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_sessions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_sessions;
CREATE POLICY tenant_isolation ON ask_sessions
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE ask_proposed_state_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_proposed_state_changes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_proposed_state_changes;
CREATE POLICY tenant_isolation ON ask_proposed_state_changes
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE ask_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_messages;
CREATE POLICY tenant_isolation ON ask_messages
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_messages.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_messages.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

ALTER TABLE ask_scopes ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_scopes FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_scopes;
CREATE POLICY tenant_isolation ON ask_scopes
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_scopes.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_scopes.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

ALTER TABLE ask_retrieval_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_retrieval_runs FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_retrieval_runs;
CREATE POLICY tenant_isolation ON ask_retrieval_runs
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_retrieval_runs.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_retrieval_runs.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

ALTER TABLE ask_evidence_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_evidence_items FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_evidence_items;
CREATE POLICY tenant_isolation ON ask_evidence_items
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1
      FROM ask_retrieval_runs r
      JOIN ask_sessions s ON s.id = r.session_id
      WHERE r.id = ask_evidence_items.retrieval_run_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1
      FROM ask_retrieval_runs r
      JOIN ask_sessions s ON s.id = r.session_id
      WHERE r.id = ask_evidence_items.retrieval_run_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

ALTER TABLE ask_answers ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_answers FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_answers;
CREATE POLICY tenant_isolation ON ask_answers
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_answers.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_answers.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

ALTER TABLE ask_feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE ask_feedback FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON ask_feedback;
CREATE POLICY tenant_isolation ON ask_feedback
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_feedback.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM ask_sessions s
      WHERE s.id = ask_feedback.session_id
        AND s.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

COMMIT;
