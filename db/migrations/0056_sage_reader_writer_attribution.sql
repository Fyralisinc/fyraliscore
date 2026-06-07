-- =====================================================================
-- 0056_sage_reader_writer_attribution.sql
--
-- Fine-grained credit assignment for the SAGE reader-writer feedback
-- loop. These tables live in the Discovery Utility Layer: they record
-- retrieval-policy bookkeeping, not canonical truth about Models.
-- =====================================================================

BEGIN;

ALTER TABLE inquiry_outcome_events
  DROP CONSTRAINT IF EXISTS inquiry_outcome_events_event_type_check;

ALTER TABLE inquiry_outcome_events
  ADD CONSTRAINT inquiry_outcome_events_event_type_check CHECK (
    event_type IN (
      'retrieved_evidence_used_in_packet',
      'retrieved_evidence_omitted',
      'omitted_evidence_later_requested',
      'node_used_in_valid_diff',
      'path_used_in_valid_diff',
      'reader_decision_used_in_valid_diff',
      'reader_decision_low_value',
      'outcome_quality_assessed',
      'validation_failed_due_to_missing_evidence',
      'validation_failed_due_to_bad_reference',
      'user_accepted_node',
      'user_contested_node',
      'model_later_confirmed',
      'model_later_falsified',
      'recommendation_acted_on',
      'recommendation_ignored'
    )
  );

CREATE TABLE IF NOT EXISTS sage_reader_decision_attributions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL
    REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  question_id TEXT NOT NULL,
  question_primitive TEXT NOT NULL,
  question TEXT,
  question_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  expected_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  expected_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  signal_type TEXT NOT NULL,
  entities JSONB NOT NULL DEFAULT '[]'::jsonb,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  selected BOOLEAN NOT NULL DEFAULT FALSE,
  selection_rank INTEGER CHECK (
    selection_rank IS NULL OR selection_rank >= 0
  ),
  activation_score DOUBLE PRECISION NOT NULL CHECK (
    activation_score >= 0 AND activation_score <= 1
  ),
  activation_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
  retrieval_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  projected_evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_in_packet_count INTEGER NOT NULL DEFAULT 0 CHECK (
    evidence_in_packet_count >= 0
  ),
  writer_used BOOLEAN NOT NULL DEFAULT FALSE,
  writer_credit_score DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (
    writer_credit_score >= 0
  ),
  credited_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (inquiry_session_id, question_id, model_id)
);

COMMENT ON TABLE sage_reader_decision_attributions IS
  'SAGE per-reader-decision attribution ledger — retrieval-policy credit assignment, not canonical truth.';

CREATE INDEX IF NOT EXISTS sage_reader_decision_attr_session_idx
  ON sage_reader_decision_attributions (
    inquiry_session_id, question_id, activation_score DESC
  );

CREATE INDEX IF NOT EXISTS sage_reader_decision_attr_model_idx
  ON sage_reader_decision_attributions (tenant_id, model_id, created_at DESC);

CREATE INDEX IF NOT EXISTS sage_reader_decision_attr_policy_idx
  ON sage_reader_decision_attributions (
    tenant_id, signal_type, question_primitive, created_at DESC
  );

CREATE INDEX IF NOT EXISTS sage_reader_decision_attr_credit_idx
  ON sage_reader_decision_attributions (
    tenant_id, writer_used, writer_credit_score DESC, credited_at DESC
  );

CREATE TABLE IF NOT EXISTS sage_question_policy_stats (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signal_type TEXT NOT NULL,
  question_primitive TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  successes INTEGER NOT NULL DEFAULT 0 CHECK (successes >= 0),
  total_credit DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  total_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  utility_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  last_credit_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, signal_type, question_primitive)
);

COMMENT ON TABLE sage_question_policy_stats IS
  'SAGE learned utility of question primitives by signal shape — mutable retrieval policy, not canonical truth.';

CREATE INDEX IF NOT EXISTS sage_question_policy_stats_lookup_idx
  ON sage_question_policy_stats (
    tenant_id, signal_type, utility_score DESC, question_primitive
  );

ALTER TABLE sage_reader_decision_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_reader_decision_attributions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON sage_reader_decision_attributions;
CREATE POLICY tenant_isolation ON sage_reader_decision_attributions
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE sage_question_policy_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_question_policy_stats FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON sage_question_policy_stats;
CREATE POLICY tenant_isolation ON sage_question_policy_stats
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
