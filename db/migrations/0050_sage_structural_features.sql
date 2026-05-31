-- =====================================================================
-- 0050_sage_structural_features.sql
--
-- Phase 5 of the SAGE-inspired self-evolution work
-- (fyralis-sage-synthesis-self-evolution.md §10, Phase 5). Introduces
-- the durable structural-feature store that surfaces topological
-- properties (degree, clustering coefficient, hub/bridge scores,
-- community membership, Jaccard overlap, ...) for every active
-- Model and Model-edge in the Synthesis graph.
--
-- Schema adaptation: the spec uses placeholder names (`nodes`,
-- `relationships`); Fyralis nodes are `models` (0001) and edges are
-- `model_edges` (0031). Tables are renamed to match (`model_*`).
--
-- Conventions: CREATE TABLE / INDEX IF NOT EXISTS, BEGIN/COMMIT.
-- RLS policy mirrors 0036_rls_permissive_default.sql /
-- 0041_predictions.sql (permissive default until every reader is
-- tenant-bound).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- model_structural_features — per-Model topological summary.
--
-- One row per Model. Recomputed by services.sage.structural_features.job
-- on a periodic full sweep + incremental update on graph writes
-- (Phase 5 acceptance criteria). PK on model_id with ON DELETE CASCADE:
-- when a Model is physically deleted (tests, ops cleanup) we drop the
-- derived row too. Archived Models stay in `models` and KEEP their
-- features row — the recompute job decides whether to refresh or zero
-- them based on `status`.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_structural_features (
  model_id UUID PRIMARY KEY REFERENCES models(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  degree_total INT NOT NULL DEFAULT 0,
  degree_in INT NOT NULL DEFAULT 0,
  degree_out INT NOT NULL DEFAULT 0,
  clustering_coefficient DOUBLE PRECISION,
  core_number INT,
  avg_neighbor_degree DOUBLE PRECISION,
  bridge_score DOUBLE PRECISION,
  hub_score DOUBLE PRECISION,
  community_id UUID,
  region_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hub leaderboard: "top hubs in this tenant". Hot path for hub-
-- suppression heuristics (Phase 6 structural gates).
CREATE INDEX IF NOT EXISTS model_structural_features_hub_idx
  ON model_structural_features (tenant_id, hub_score DESC);

-- Bridge leaderboard: "top bridges in this tenant". Hot path for
-- bridge-preservation heuristics.
CREATE INDEX IF NOT EXISTS model_structural_features_bridge_idx
  ON model_structural_features (tenant_id, bridge_score DESC);


-- ---------------------------------------------------------------------
-- model_edge_structural_features — per-edge topological summary.
--
-- One row per edge in model_edges. The source/target columns and
-- tenant_id are denormalized from model_edges so the structural
-- queries (bridge_likelihood ranking, redundancy sweeps) can run
-- without an extra join.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_edge_structural_features (
  edge_id UUID PRIMARY KEY REFERENCES model_edges(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_model_id UUID NOT NULL,
  target_model_id UUID NOT NULL,
  degree_difference DOUBLE PRECISION,
  common_neighbors INT,
  jaccard_overlap DOUBLE PRECISION,
  edge_betweenness_approx DOUBLE PRECISION,
  bridge_likelihood DOUBLE PRECISION,
  redundancy_score DOUBLE PRECISION,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS model_edge_structural_features_tenant_bridge_idx
  ON model_edge_structural_features (tenant_id, bridge_likelihood DESC);

CREATE INDEX IF NOT EXISTS model_edge_structural_features_tenant_jaccard_idx
  ON model_edge_structural_features (tenant_id, jaccard_overlap);

CREATE INDEX IF NOT EXISTS model_edge_structural_features_source_idx
  ON model_edge_structural_features (tenant_id, source_model_id);

CREATE INDEX IF NOT EXISTS model_edge_structural_features_target_idx
  ON model_edge_structural_features (tenant_id, target_model_id);


-- ---------------------------------------------------------------------
-- RLS — mirror the 0036 / 0041 permissive-default pattern.
-- ---------------------------------------------------------------------

ALTER TABLE model_structural_features ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_structural_features FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_structural_features;
CREATE POLICY tenant_isolation ON model_structural_features
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE model_edge_structural_features ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_edge_structural_features FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_edge_structural_features;
CREATE POLICY tenant_isolation ON model_edge_structural_features
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
