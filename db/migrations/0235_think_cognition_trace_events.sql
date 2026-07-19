BEGIN;

CREATE TABLE IF NOT EXISTS think_cognition_trace_events (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  trace_id TEXT NOT NULL CHECK (length(btrim(trace_id)) > 0),
  logical_call_id TEXT NOT NULL,
  trigger_id UUID,
  think_run_id UUID,
  batch_id TEXT,
  schema_version TEXT NOT NULL CHECK (schema_version = 'think-cognition-trace-v1'),
  stage TEXT NOT NULL CHECK (stage IN (
    'prompt','raw_provider_response','compiler','validated_command','applied_result'
  )),
  cognitive_purpose TEXT NOT NULL CHECK (cognitive_purpose IN (
    'mention_discovery','entity_resolution','question_planning',
    'main_reconciliation','main_synthesis'
  )),
  payload JSONB NOT NULL,
  content_digest TEXT NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  occurred_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, trace_id, stage),
  FOREIGN KEY (tenant_id, logical_call_id)
    REFERENCES llm_logical_call_receipts (tenant_id, logical_call_id)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS think_cognition_trace_events_run_idx
  ON think_cognition_trace_events (tenant_id, think_run_id, occurred_at);
ALTER TABLE think_cognition_trace_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE think_cognition_trace_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON think_cognition_trace_events
  USING (NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid)
  WITH CHECK (NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);

COMMIT;
