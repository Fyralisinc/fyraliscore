-- =====================================================================
-- 0089_sage_model_predictions.sql
--
-- Phase 12 of the SAGE-inspired self-evolution work
-- (fyralis-sage-synthesis-self-evolution.md §13 / Phase 12). Introduces
-- the *internal* Model-substrate prediction tables that let the
-- Synthesis loop turn falsifiable Model assertions into explicit
-- expectations and capture residuals when reality violates them.
--
-- IMPORTANT — NOT the same as `predictions` (migration 0041).
--   * `predictions` (0041)            — CEO/Forecasts *author-facing*
--                                       surface artifacts. Live on the
--                                       Forecasts page with their own
--                                       lifecycle (active → resolved
--                                       with outcome={true,false,partial})
--                                       and inspector UI. Curated /
--                                       narrated.
--   * `model_predictions` (this file) — *Internal* Model-substrate
--                                       expectations emitted by Models
--                                       in the Synthesis graph. Not
--                                       directly user-facing. Their
--                                       purpose is to trigger residual
--                                       inquiry (`model_prediction_errors`)
--                                       when observations contradict
--                                       what the Model implied would
--                                       happen, so the self-evolution
--                                       loop can prioritize high-impact
--                                       surprises and rewrite the
--                                       graph where it failed.
--
-- The two namespaces intentionally coexist: a single user-visible
-- Forecast row in `predictions` may be backed by zero, one, or many
-- internal `model_predictions` rows from the Models that contributed
-- to it. Cross-references are loose (no FK) so archived authoring
-- surfaces don't cascade-drop substrate history.
--
-- Schema adaptation: the spec uses `nodes(id)`; Fyralis nodes are
-- `models` (0001). The FK is therefore on `models(id)`.
--
-- Conventions: CREATE TABLE / INDEX IF NOT EXISTS, BEGIN/COMMIT.
-- RLS policy mirrors 0036_rls_permissive_default.sql /
-- 0041_predictions.sql (permissive default until every reader is
-- tenant-bound).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- model_predictions — Model-emitted expectation rows (doc §13.1).
--
-- One row per expectation. `expected_observation` carries the
-- structured shape the residual detector compares against incoming
-- observations:
--   { "kind": "...",
--     "scope_entities": [...],
--     "scope_actors": [...],
--     "value_constraint": {"op": "...", "value": ...},
--     ...                       # extension fields allowed }
--
-- `check_after` is nullable: not every prediction is time-bound. The
-- sweeper that resolves due predictions filters on
-- `(status='active' AND check_after IS NOT NULL AND check_after <= $now)`.
--
-- `resolved_by_observation_id` is a LOOSE reference into observations
-- (no FK) so observation archival doesn't take down the historical
-- residual record. Mirrors the same trade-off `predictions` (0041)
-- makes with its `target_node_id`.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_predictions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  prediction TEXT NOT NULL,
  expected_observation JSONB NOT NULL,
  check_after TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'confirmed', 'falsified', 'expired', 'superseded')),
  confidence DOUBLE PRECISION
    CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  resolved_by_observation_id UUID  -- loose ref to observations; no FK
);

-- Hot path: due-prediction sweeper. Filters by status, orders by
-- check_after so the sweep picks earliest-due first.
CREATE INDEX IF NOT EXISTS model_predictions_due_idx
  ON model_predictions (tenant_id, status, check_after);

-- "All predictions emitted by Model X": used by the residual detector
-- when a new observation arrives in a Model's scope, and by the
-- archive cascade when a Model is superseded.
CREATE INDEX IF NOT EXISTS model_predictions_tenant_model_idx
  ON model_predictions (tenant_id, model_id);

-- Recent activity scan for the self-evolution dashboards.
CREATE INDEX IF NOT EXISTS model_predictions_tenant_status_recent_idx
  ON model_predictions (tenant_id, status, created_at DESC);


-- ---------------------------------------------------------------------
-- model_prediction_errors — residual events (doc §13.2).
--
-- One row per detected expectation violation. `severity` measures
-- how badly the observation contradicts the expectation; `impact_score`
-- measures how much the failing Model matters (centrality, dependent
-- count, recent retrieval activity). The triage worker ranks by
-- `impact_score DESC` so it spends inquiry budget on failures that
-- matter.
--
-- `prediction_id` has ON DELETE CASCADE: if the parent expectation
-- itself is removed (test cleanup, op compaction), drop the residual
-- record with it. `observed_signal_id` is a LOOSE ref (no FK) so
-- observation archival is non-destructive of residual history.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_prediction_errors (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  prediction_id UUID REFERENCES model_predictions(id) ON DELETE CASCADE,
  observed_signal_id UUID,  -- loose ref to observations; no FK
  error_summary TEXT NOT NULL,
  severity DOUBLE PRECISION NOT NULL
    CHECK (severity >= 0 AND severity <= 1),
  impact_score DOUBLE PRECISION NOT NULL
    CHECK (impact_score >= 0 AND impact_score <= 1),
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN (
      'open', 'triaged', 'inquiry_scheduled',
      'inquiry_complete', 'resolved', 'ignored'
    )),
  triage_notes JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: triage worker ranking — "open residuals by impact".
CREATE INDEX IF NOT EXISTS model_prediction_errors_open_impact_idx
  ON model_prediction_errors (tenant_id, status, impact_score DESC);

-- Per-Model failure timeline (debug surface + supersession heuristic).
CREATE INDEX IF NOT EXISTS model_prediction_errors_tenant_model_recent_idx
  ON model_prediction_errors (tenant_id, model_id, created_at DESC);


-- ---------------------------------------------------------------------
-- RLS — mirror the 0036 / 0041 permissive-default pattern.
-- ---------------------------------------------------------------------

ALTER TABLE model_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_predictions FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_predictions;
CREATE POLICY tenant_isolation ON model_predictions
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE model_prediction_errors ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_prediction_errors FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_prediction_errors;
CREATE POLICY tenant_isolation ON model_prediction_errors
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
