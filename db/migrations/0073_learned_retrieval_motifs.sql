-- =====================================================================
-- 0073_learned_retrieval_motifs.sql
--
-- Retrieval motifs are learned procedural memory for inquiry retrieval:
-- reusable recipes over existing safe RetrievalAction operators. They
-- are optimization telemetry, not canonical truth.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS retrieval_motifs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  signature JSONB NOT NULL DEFAULT '{}'::jsonb,
  signature_hash TEXT NOT NULL,
  question_primitive TEXT NOT NULL,
  plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  plan_hash TEXT NOT NULL,
  maturity TEXT NOT NULL DEFAULT 'active' CHECK (
    maturity IN ('candidate', 'active', 'quarantined')
  ),
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
  UNIQUE (tenant_id, question_primitive, signature_hash, plan_hash)
);

COMMENT ON TABLE retrieval_motifs IS
  'Learned retrieval recipes over existing safe retrieval operators; mutable utility memory, not canonical truth.';

CREATE INDEX IF NOT EXISTS retrieval_motifs_signature_gin
  ON retrieval_motifs USING GIN (signature);

CREATE INDEX IF NOT EXISTS retrieval_motifs_tenant_primitive_utility_idx
  ON retrieval_motifs (
    tenant_id, question_primitive, maturity, utility_score DESC
  );

CREATE INDEX IF NOT EXISTS retrieval_motifs_tenant_expires_idx
  ON retrieval_motifs (tenant_id, expires_at);

ALTER TABLE retrieval_motifs ENABLE ROW LEVEL SECURITY;
ALTER TABLE retrieval_motifs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON retrieval_motifs;
CREATE POLICY tenant_isolation ON retrieval_motifs
  USING (true)
  WITH CHECK (true);

COMMIT;
