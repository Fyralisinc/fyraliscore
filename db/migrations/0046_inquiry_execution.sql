-- =====================================================================
-- 0046_inquiry_execution.sql
--
-- First execution-layer migration for routed retrieval. This migration
-- adds the durable route-decision ledger used by shadow routing and
-- extends think_run_artifacts so later inquiry stages can be captured
-- in the same debug timeline.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS signal_routing_decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signal_ref_type TEXT NOT NULL CHECK (
    signal_ref_type IN (
      'observation',
      'query',
      'scheduled_job',
      'anomaly',
      'internal'
    )
  ),
  signal_ref_id UUID,
  route_decision_id UUID,
  think_run_id UUID,
  route TEXT NOT NULL CHECK (
    route IN (
      'IGNORE_OR_ARCHIVE',
      'DETERMINISTIC_UPDATE',
      'FAST_PATH',
      'DEEP_INQUIRY_PATH',
      'BACKGROUND_PATH',
      'HUMAN_VALIDATION_PATH'
    )
  ),
  decision_status TEXT NOT NULL CHECK (
    decision_status IN ('shadow', 'enforced', 'skipped', 'failed')
  ),
  score DOUBLE PRECISION NOT NULL CHECK (score >= 0.0 AND score <= 1.0),
  score_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
  estimated_cost JSONB NOT NULL DEFAULT '{}'::jsonb,
  risk_level TEXT,
  sensitivity TEXT,
  reason TEXT NOT NULL,
  enqueued_trigger_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS signal_routing_decisions_tenant_time_idx
  ON signal_routing_decisions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS signal_routing_decisions_tenant_route_idx
  ON signal_routing_decisions (tenant_id, route, created_at DESC);

CREATE INDEX IF NOT EXISTS signal_routing_decisions_signal_ref_idx
  ON signal_routing_decisions (signal_ref_type, signal_ref_id);

CREATE TABLE IF NOT EXISTS inquiry_sessions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signal_ref_type TEXT NOT NULL CHECK (
    signal_ref_type IN (
      'observation',
      'query',
      'scheduled_job',
      'anomaly',
      'internal'
    )
  ),
  signal_ref_id UUID,
  route TEXT NOT NULL CHECK (
    route IN (
      'IGNORE_OR_ARCHIVE',
      'DETERMINISTIC_UPDATE',
      'FAST_PATH',
      'DEEP_INQUIRY_PATH',
      'BACKGROUND_PATH',
      'HUMAN_VALIDATION_PATH'
    )
  ),
  status TEXT NOT NULL CHECK (
    status IN ('running', 'completed', 'failed', 'deferred')
  ),
  stop_status TEXT NOT NULL CHECK (
    stop_status IN (
      'sufficient_for_reasoning',
      'insufficient_continue',
      'insufficient_defer',
      'human_validation_required',
      'no_update_needed',
      'budget_exhausted'
    )
  ),
  round_count INTEGER NOT NULL DEFAULT 0 CHECK (round_count >= 0),
  question_count INTEGER NOT NULL DEFAULT 0 CHECK (question_count >= 0),
  evidence_count INTEGER NOT NULL DEFAULT 0 CHECK (evidence_count >= 0),
  context_packet JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

ALTER TABLE inquiry_sessions
  ADD COLUMN IF NOT EXISTS route_decision_id UUID;

ALTER TABLE inquiry_sessions
  ADD COLUMN IF NOT EXISTS think_run_id UUID;

CREATE INDEX IF NOT EXISTS inquiry_sessions_tenant_time_idx
  ON inquiry_sessions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS inquiry_sessions_signal_ref_idx
  ON inquiry_sessions (signal_ref_type, signal_ref_id);

CREATE INDEX IF NOT EXISTS inquiry_sessions_tenant_stop_idx
  ON inquiry_sessions (tenant_id, stop_status, created_at DESC);

CREATE TABLE IF NOT EXISTS inquiry_evidence_items (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_type TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  source_ref_id UUID,
  summary TEXT NOT NULL,
  trust_tier TEXT,
  occurred_at TIMESTAMPTZ,
  retrieval_paths JSONB NOT NULL DEFAULT '[]'::jsonb,
  retrieved_for_questions JSONB NOT NULL DEFAULT '[]'::jsonb,
  supports_hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
  weakens_hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
  contradicts_hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_content_ref TEXT,
  token_estimate INTEGER NOT NULL DEFAULT 1 CHECK (token_estimate >= 1),
  access_scope TEXT NOT NULL DEFAULT 'tenant',
  sensitivity TEXT NOT NULL DEFAULT 'normal',
  score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (session_id, source_ref)
);

CREATE INDEX IF NOT EXISTS inquiry_evidence_items_session_idx
  ON inquiry_evidence_items (session_id);

CREATE INDEX IF NOT EXISTS inquiry_evidence_items_tenant_source_idx
  ON inquiry_evidence_items (tenant_id, source_type, source_ref_id);

CREATE TABLE IF NOT EXISTS inquiry_question_runs (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  question_id TEXT NOT NULL,
  round_index INTEGER NOT NULL DEFAULT 0 CHECK (round_index >= 0),
  primitive TEXT NOT NULL,
  question TEXT NOT NULL,
  score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  retrieval_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  answer JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (session_id, question_id)
);

CREATE INDEX IF NOT EXISTS inquiry_question_runs_session_idx
  ON inquiry_question_runs (session_id, round_index, question_id);

ALTER TABLE think_run_artifacts
  DROP CONSTRAINT IF EXISTS think_run_artifacts_stage_check;

ALTER TABLE think_run_artifacts
  ADD CONSTRAINT think_run_artifacts_stage_check
  CHECK (stage IN (
    'trigger',
    'routing',
    'retrieval',
    'inquiry',
    'context_packet',
    'sufficiency',
    'prompt',
    'response',
    'validation',
    'apply',
    'post_commit',
    'cascade',
    'error'
  ));

COMMIT;
