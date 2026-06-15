-- 0136_reflective_rule_feedback_loop.sql
--
-- Durable attribution and replay records for reflective retrieval rules.
-- This closes the loop around 0135: active rules can now receive credit,
-- candidate rules can be replayed before promotion, and both surfaces remain
-- optimization telemetry rather than canonical truth.

BEGIN;

CREATE TABLE IF NOT EXISTS reflective_rule_attributions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  rule_id UUID NOT NULL REFERENCES reflective_retrieval_rules(id) ON DELETE CASCADE,
  effect_type TEXT NOT NULL CHECK (
    effect_type IN ('question_plan', 'action_plan')
  ),
  question_id TEXT NOT NULL,
  question_primitive TEXT NOT NULL,
  action_path TEXT,
  action_target TEXT,
  action_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_count INTEGER NOT NULL DEFAULT 0 CHECK (evidence_count >= 0),
  selected_evidence_count INTEGER NOT NULL DEFAULT 0
    CHECK (selected_evidence_count >= 0),
  credit DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  cost DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  outcome_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (
    inquiry_session_id, rule_id, effect_type, question_id,
    action_path, action_target
  )
);

COMMENT ON TABLE reflective_rule_attributions IS
  'Per-inquiry reflective-rule credit ledger; mutable optimization telemetry, not canonical truth.';

CREATE INDEX IF NOT EXISTS reflective_rule_attributions_rule_idx
  ON reflective_rule_attributions (
    tenant_id, rule_id, created_at DESC
  );

CREATE INDEX IF NOT EXISTS reflective_rule_attributions_session_idx
  ON reflective_rule_attributions (
    tenant_id, inquiry_session_id
  );

CREATE TABLE IF NOT EXISTS reflective_rule_replay_runs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID REFERENCES inquiry_sessions(id) ON DELETE SET NULL,
  rule_id UUID REFERENCES reflective_retrieval_rules(id) ON DELETE SET NULL,
  signature_hash TEXT NOT NULL,
  rule_pack_hash TEXT NOT NULL,
  baseline_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  candidate_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  utility_delta DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  decision TEXT NOT NULL CHECK (
    decision IN ('promoted', 'candidate', 'rejected', 'quarantined')
  ),
  diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE reflective_rule_replay_runs IS
  'Offline replay results for reflective retrieval rules before or after promotion.';

CREATE INDEX IF NOT EXISTS reflective_rule_replay_runs_candidate_idx
  ON reflective_rule_replay_runs (
    tenant_id, signature_hash, rule_pack_hash, created_at DESC
  );

ALTER TABLE reflective_rule_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE reflective_rule_attributions FORCE ROW LEVEL SECURITY;
ALTER TABLE reflective_rule_replay_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE reflective_rule_replay_runs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON reflective_rule_attributions;
CREATE POLICY tenant_isolation ON reflective_rule_attributions
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS tenant_isolation ON reflective_rule_replay_runs;
CREATE POLICY tenant_isolation ON reflective_rule_replay_runs
  USING (true)
  WITH CHECK (true);

COMMIT;
