-- =====================================================================
-- 0051_sage_retrieval_affordance_profiles.sql
--
-- SAGE Phase 9: Retrieval Affordance Profiles.
--
-- A retrieval_affordance_profile is *derived* metadata about a Model
-- (= SAGE Node): "what kinds of questions does this Model help answer,
-- what hypotheses does it support or weaken, what abstractions does it
-- typically participate in, what signals should re-activate it, and
-- what evidence should be projected if it becomes relevant".
--
-- The profile is keyed by `model_id` and ON DELETE CASCADE-tied to
-- the parent Model — affordances have no independent identity. Updates
-- happen via a sage-affordances policy module (heuristic v1) and later
-- via reinforcement signals from inquiry / valid-diff outcomes; canonical
-- Model state (proposition, confidence, falsifier) is never touched by
-- this surface.
--
-- Indexes:
--   * (tenant_id, utility_score DESC) — primary "best-utility-first"
--     scan for retrieval planners.
--   * GIN on answers_question_primitives — question-primitive lookups
--     ("which Models help answer CONSTRAINT?").
--   * GIN on supports_hypothesis_types — hypothesis-driven retrieval
--     ("which Models support hypothesis 'delivery_slippage'?").
--
-- RLS follows the 0036 / 0041 pattern: tenant_isolation with the
-- permissive-default branch so pre-TenantContext callers keep working.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS retrieval_affordance_profiles (
  model_id UUID PRIMARY KEY REFERENCES models(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  answers_question_primitives TEXT[] NOT NULL DEFAULT '{}',
  supports_hypothesis_types TEXT[] NOT NULL DEFAULT '{}',
  weakens_hypothesis_types TEXT[] NOT NULL DEFAULT '{}',
  common_composition_types TEXT[] NOT NULL DEFAULT '{}',
  action_affordances TEXT[] NOT NULL DEFAULT '{}',
  activation_signatures JSONB NOT NULL DEFAULT '{}'::jsonb,
  projection_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  utility_score FLOAT NOT NULL DEFAULT 0,
  decay_after TIMESTAMPTZ,
  last_reinforced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: "best affordances for this tenant" — utility-ranked scan.
CREATE INDEX IF NOT EXISTS retrieval_affordance_profiles_tenant_utility_idx
  ON retrieval_affordance_profiles (tenant_id, utility_score DESC);

-- Question-primitive lookups via GIN; arrays of short tokens like
-- CONSTRAINT / CAUSE / DEPENDENCY.
CREATE INDEX IF NOT EXISTS retrieval_affordance_profiles_question_primitives_idx
  ON retrieval_affordance_profiles USING GIN (answers_question_primitives);

-- Hypothesis-driven retrieval — match Models that support a given
-- hypothesis type.
CREATE INDEX IF NOT EXISTS retrieval_affordance_profiles_supports_hypothesis_idx
  ON retrieval_affordance_profiles USING GIN (supports_hypothesis_types);

-- ---------------------------------------------------------------------
-- RLS — mirror the 0036 permissive-default + 0041 pattern. Tenant-
-- scoped via own tenant_id column.
-- ---------------------------------------------------------------------

ALTER TABLE retrieval_affordance_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE retrieval_affordance_profiles FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON retrieval_affordance_profiles;
CREATE POLICY tenant_isolation ON retrieval_affordance_profiles
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
