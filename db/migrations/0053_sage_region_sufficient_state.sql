-- =====================================================================
-- 0053_sage_region_sufficient_state.sql
--
-- SAGE Phase 11: Region Sufficient-State Summaries
-- (fyralis-sage-synthesis-self-evolution.md §12 + Phase 11).
--
-- A `region_sufficient_state` row is a compact retrieval starting point
-- for a logical cluster ("region") of related Models — what's happening
-- in this region, what's currently known, what's unresolved, where
-- next inquiry should go. Background jobs and the inquiry planner read
-- it before raw graph traversal so reasoning over active regions does
-- not re-derive context from scratch.
--
-- region_id has NO foreign key. The topology layer does not yet
-- materialize a first-class `regions` table; `region_id` is a tenant-
-- unique UUID minted by the topology / clustering pass and reused
-- across this table and `model_structural_features.region_ids[]`
-- (migration 0050). The same loose-reference treatment applies to
-- `affected_goals`, `affected_commitments`, and `member_model_ids`:
-- archiving a referenced row must not cascade-delete the summary, and
-- the summary refresher tolerates dangling ids by filtering them out
-- on read.
--
-- Refresh triggers (per Phase 11) are encoded as a CHECK on
-- `last_refreshed_reason`. Allowed values:
--   validated_model_update | high_impact_signal | prediction_error |
--   user_contestation     | scheduled          | region_anomaly.
--
-- Conventions: BEGIN/COMMIT, CREATE TABLE / INDEX IF NOT EXISTS. RLS
-- mirrors 0036_rls_permissive_default.sql / 0041_predictions.sql.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS region_sufficient_state (
  region_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  region_label TEXT,
  summary TEXT NOT NULL,
  active_hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
  active_constraints JSONB NOT NULL DEFAULT '[]'::jsonb,
  known_counterevidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  unresolved_unknowns JSONB NOT NULL DEFAULT '[]'::jsonb,
  -- Loose refs: no FK, see header. These survive archive of the target.
  affected_goals UUID[] NOT NULL DEFAULT '{}'::uuid[],
  affected_commitments UUID[] NOT NULL DEFAULT '{}'::uuid[],
  member_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  priority_score FLOAT NOT NULL DEFAULT 0,
  prediction_error_score FLOAT NOT NULL DEFAULT 0,
  next_best_frontiers JSONB NOT NULL DEFAULT '[]'::jsonb,
  falsification_watch JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_refreshed_reason TEXT CHECK (
    last_refreshed_reason IS NULL OR last_refreshed_reason IN (
      'validated_model_update',
      'high_impact_signal',
      'prediction_error',
      'user_contestation',
      'scheduled',
      'region_anomaly'
    )
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: "highest-priority regions for this tenant" — drives the
-- inquiry planner's region leaderboard.
CREATE INDEX IF NOT EXISTS region_sufficient_state_tenant_priority_idx
  ON region_sufficient_state (tenant_id, priority_score DESC);

-- Hot path: "regions with the largest active prediction error" —
-- drives the residuals / Phase 12 follow-up sweep.
CREATE INDEX IF NOT EXISTS region_sufficient_state_tenant_pred_err_idx
  ON region_sufficient_state (tenant_id, prediction_error_score DESC);

-- Recency scan: "what regions were refreshed most recently" — drives
-- background staleness checks and the scheduled-refresh job.
CREATE INDEX IF NOT EXISTS region_sufficient_state_tenant_updated_idx
  ON region_sufficient_state (tenant_id, updated_at DESC);

-- Reverse lookup: "which region summaries include this Model as a
-- member?" Used by the refresh trigger fired on validated_model_update.
-- Uses GIN so `member_model_ids @> ARRAY[$1]::uuid[]` is index-backed.
CREATE INDEX IF NOT EXISTS region_sufficient_state_member_models_idx
  ON region_sufficient_state USING GIN (member_model_ids);


-- ---------------------------------------------------------------------
-- RLS — mirror the 0036 / 0041 permissive-default pattern.
-- ---------------------------------------------------------------------

ALTER TABLE region_sufficient_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE region_sufficient_state FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON region_sufficient_state;
CREATE POLICY tenant_isolation ON region_sufficient_state
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
