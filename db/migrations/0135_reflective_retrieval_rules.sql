-- 0135_reflective_retrieval_rules.sql
--
-- Reflective retrieval rules are GEPA-style policy memory for the inquiry
-- planner/compiler. They are optimization telemetry, not canonical truth.
-- Rules are deliberately JSONB and bounded by maturity/utility so the runtime
-- can load a few inspectable rule packs without widening canonical model state.

BEGIN;

CREATE TABLE IF NOT EXISTS reflective_retrieval_rules (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signature JSONB NOT NULL DEFAULT '{}'::jsonb,
  signature_hash TEXT NOT NULL,
  rule_pack JSONB NOT NULL DEFAULT '{}'::jsonb,
  rule_pack_hash TEXT NOT NULL,
  maturity TEXT NOT NULL DEFAULT 'candidate'
    CHECK (maturity IN ('candidate', 'active', 'quarantined')),
  utility_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  success_count INTEGER NOT NULL DEFAULT 0 CHECK (success_count >= 0),
  failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
  total_credit DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  total_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  last_success_at TIMESTAMPTZ,
  last_failure_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, signature_hash, rule_pack_hash)
);

COMMENT ON TABLE reflective_retrieval_rules IS
  'Reflective inquiry planning/routing rules learned from traces; mutable optimization telemetry, not canonical truth.';

CREATE INDEX IF NOT EXISTS reflective_retrieval_rules_signature_gin
  ON reflective_retrieval_rules USING GIN (signature);

CREATE INDEX IF NOT EXISTS reflective_retrieval_rules_tenant_utility_idx
  ON reflective_retrieval_rules (
    tenant_id, maturity, utility_score DESC, updated_at DESC
  );

CREATE INDEX IF NOT EXISTS reflective_retrieval_rules_tenant_expires_idx
  ON reflective_retrieval_rules (tenant_id, expires_at);

ALTER TABLE reflective_retrieval_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE reflective_retrieval_rules FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON reflective_retrieval_rules;
CREATE POLICY tenant_isolation ON reflective_retrieval_rules
  USING (true)
  WITH CHECK (true);

COMMIT;
