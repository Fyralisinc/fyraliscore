-- =====================================================================
-- 0049_sage_inquiry_trace_gap_fillers.sql
--
-- Phase 1 (SAGE-inspired self-evolution): gap-filler tables for the
-- inquiry trace. Migration 0046 already created `inquiry_sessions`,
-- `inquiry_evidence_items`, and `inquiry_question_runs`. This migration
-- adds the three remaining tables called out in the Phase 1 build list
-- (fyralis-sage-synthesis-self-evolution.md §Phase 1):
--
--   * retrieval_plans         — one row per (session, question, plan_revision).
--                               Captures planned retrieval intents / paths /
--                               budgets / success_conditions before the plan
--                               is executed. Spec §7.3.
--
--   * omitted_evidence        — evidence retrieved by some pathway but NOT
--                               included in the final context packet, with a
--                               structured `omission_reason` so the topology
--                               optimizer can learn which omissions matter.
--                               Spec §15.1 / Phase 1.
--
--   * inquiry_outcome_events  — typed event log per spec §15.1. Powers the
--                               downstream "convert usage into training data"
--                               loop; the CHECK constraint pins the 12
--                               event_type strings enumerated in §15.1.
--
-- Conventions mirror 0046 (idempotent CREATE … IF NOT EXISTS, tenant FK
-- with ON DELETE CASCADE, CHECK on enum text columns, indexes on
-- (tenant_id, created_at DESC) + FK columns) and the 0036 / 0041 RLS
-- shape (tenant_isolation policy keyed on current_setting('app.current_tenant')).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- retrieval_plans
-- ---------------------------------------------------------------------
-- One row per (inquiry_session, question, plan_revision). `intents` /
-- `paths` / `budgets` / `success_conditions` are JSONB so the planner
-- can evolve their shapes without a migration. `plan_revision` lets a
-- question be re-planned mid-session (e.g. after the first round of
-- evidence) without losing the prior plan.
CREATE TABLE IF NOT EXISTS retrieval_plans (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL
    REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  question_id TEXT NOT NULL,
  plan_revision INTEGER NOT NULL DEFAULT 0 CHECK (plan_revision >= 0),
  intents JSONB NOT NULL DEFAULT '[]'::jsonb,
  paths JSONB NOT NULL DEFAULT '[]'::jsonb,
  budgets JSONB NOT NULL DEFAULT '{}'::jsonb,
  success_conditions JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (inquiry_session_id, question_id, plan_revision)
);

CREATE INDEX IF NOT EXISTS retrieval_plans_tenant_time_idx
  ON retrieval_plans (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS retrieval_plans_session_idx
  ON retrieval_plans (inquiry_session_id, question_id, plan_revision);

-- ---------------------------------------------------------------------
-- omitted_evidence
-- ---------------------------------------------------------------------
-- Evidence the retrieval layer surfaced but the packet builder chose to
-- drop. `omission_reason` is a closed enum so the topology optimizer
-- can learn (e.g. "generic_hub" → boost hub-penalty, "redundant" →
-- merge candidate, "budget_exhausted" → grow the budget). `source_ref`
-- mirrors the shape used in inquiry_evidence_items so the two tables
-- can be joined when needed.
CREATE TABLE IF NOT EXISTS omitted_evidence (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL
    REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  question_id TEXT,
  source_type TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  source_ref_id UUID,
  retrieval_paths JSONB NOT NULL DEFAULT '[]'::jsonb,
  omission_reason TEXT NOT NULL CHECK (
    omission_reason IN (
      'generic_hub',
      'redundant',
      'out_of_scope',
      'low_confidence',
      'budget_exhausted',
      'access_denied',
      'stale',
      'other'
    )
  ),
  reason_detail TEXT,
  score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS omitted_evidence_tenant_time_idx
  ON omitted_evidence (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS omitted_evidence_session_idx
  ON omitted_evidence (inquiry_session_id);

CREATE INDEX IF NOT EXISTS omitted_evidence_reason_idx
  ON omitted_evidence (tenant_id, omission_reason, created_at DESC);

CREATE INDEX IF NOT EXISTS omitted_evidence_source_idx
  ON omitted_evidence (tenant_id, source_type, source_ref_id);

-- ---------------------------------------------------------------------
-- inquiry_outcome_events
-- ---------------------------------------------------------------------
-- Typed event log per spec §15.1. The CHECK enumerates the 12 event
-- types from the doc exactly; adding a new type requires both a doc
-- update and a follow-up migration. `payload` is JSONB so each event
-- type can carry whatever shape it needs.
CREATE TABLE IF NOT EXISTS inquiry_outcome_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  inquiry_session_id UUID NOT NULL
    REFERENCES inquiry_sessions(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL CHECK (
    event_type IN (
      'retrieved_evidence_used_in_packet',
      'retrieved_evidence_omitted',
      'omitted_evidence_later_requested',
      'node_used_in_valid_diff',
      'path_used_in_valid_diff',
      'reader_decision_low_value',
      'validation_failed_due_to_missing_evidence',
      'validation_failed_due_to_bad_reference',
      'user_accepted_node',
      'user_contested_node',
      'model_later_confirmed',
      'model_later_falsified',
      'recommendation_acted_on',
      'recommendation_ignored'
    )
  ),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS inquiry_outcome_events_tenant_time_idx
  ON inquiry_outcome_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS inquiry_outcome_events_session_idx
  ON inquiry_outcome_events (inquiry_session_id, created_at);

CREATE INDEX IF NOT EXISTS inquiry_outcome_events_type_idx
  ON inquiry_outcome_events (tenant_id, event_type, created_at DESC);

-- ---------------------------------------------------------------------
-- RLS — mirror 0036 / 0041. Each table is tenant-scoped via its own
-- tenant_id column; the policy is the permissive-default shape so
-- pre-TenantContext call sites still work but TenantContext callers
-- get defense-in-depth.
-- ---------------------------------------------------------------------

ALTER TABLE retrieval_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE retrieval_plans FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON retrieval_plans;
CREATE POLICY tenant_isolation ON retrieval_plans
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE omitted_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE omitted_evidence FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON omitted_evidence;
CREATE POLICY tenant_isolation ON omitted_evidence
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE inquiry_outcome_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE inquiry_outcome_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON inquiry_outcome_events;
CREATE POLICY tenant_isolation ON inquiry_outcome_events
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
