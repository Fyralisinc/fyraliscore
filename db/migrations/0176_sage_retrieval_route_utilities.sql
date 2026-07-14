-- =====================================================================
-- 0176_sage_retrieval_route_utilities.sql
--
-- SAGE route utility memory compresses retrieval outcomes into small,
-- mutable policy hints. It is optimization memory, not canonical truth.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS sage_retrieval_route_utilities (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signal_type TEXT NOT NULL,
  subkind TEXT,
  question_primitive TEXT,
  signature_hash TEXT NOT NULL,
  path TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  wins INTEGER NOT NULL DEFAULT 0 CHECK (wins >= 0),
  skips INTEGER NOT NULL DEFAULT 0 CHECK (skips >= 0),
  returned_models INTEGER NOT NULL DEFAULT 0 CHECK (returned_models >= 0),
  returned_observations INTEGER NOT NULL DEFAULT 0 CHECK (returned_observations >= 0),
  selected_evidence INTEGER NOT NULL DEFAULT 0 CHECK (selected_evidence >= 0),
  elapsed_ms_total INTEGER NOT NULL DEFAULT 0 CHECK (elapsed_ms_total >= 0),
  latency_ms_p95 DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (latency_ms_p95 >= 0.0),
  budget_total INTEGER NOT NULL DEFAULT 0 CHECK (budget_total >= 0),
  total_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (total_cost >= 0.0),
  total_quality_credit DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  utility_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, signature_hash, path)
);

COMMENT ON TABLE sage_retrieval_route_utilities IS
  'SAGE learned route utility hints for retrieval admission and budgeting; mutable optimization memory, not canonical truth.';

CREATE INDEX IF NOT EXISTS sage_route_utility_lookup_idx
  ON sage_retrieval_route_utilities (
    tenant_id, signal_type, question_primitive, utility_score DESC, confidence DESC
  );

CREATE INDEX IF NOT EXISTS sage_route_utility_updated_idx
  ON sage_retrieval_route_utilities (tenant_id, updated_at DESC);

ALTER TABLE sage_retrieval_route_utilities ENABLE ROW LEVEL SECURITY;
ALTER TABLE sage_retrieval_route_utilities FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON sage_retrieval_route_utilities;
CREATE POLICY tenant_isolation ON sage_retrieval_route_utilities
  USING (true)
  WITH CHECK (true);

COMMIT;
