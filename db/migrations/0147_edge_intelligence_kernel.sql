-- 0147_edge_intelligence_kernel.sql
--
-- Durable substrate for first-class edge intelligence. This layer sits
-- before relationship_candidates/model_edges and captures why two Models
-- may deserve a typed connection:
--
--   relation_evidence      explicit predicate evidence from signals/diffs
--   model_pair_evidence    accumulated cross-model behavioral proof
--
-- The tables are intentionally pre-truth. They do not mutate model_edges
-- directly; workers/compilers can promote high-proof aggregates into the
-- existing relationship_candidates lifecycle.

BEGIN;

CREATE TABLE IF NOT EXISTS relation_evidence (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  source_observation_id UUID,
  think_run_id UUID,
  source_model_id UUID,
  target_model_id UUID,
  subject_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  object_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  predicate TEXT NOT NULL,
  edge_kind_hint TEXT,
  direction TEXT NOT NULL DEFAULT 'unknown' CHECK (
    direction IN ('source_to_target', 'target_to_source', 'symmetric', 'unknown')
  ),
  scope_entities JSONB NOT NULL DEFAULT '[]'::jsonb,
  temporal_bounds JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_text TEXT,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  extraction_method TEXT NOT NULL DEFAULT 'unknown',
  status TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'consumed', 'rejected', 'superseded')
  ),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(predicate) <> ''),
  CHECK (btrim(extraction_method) <> ''),
  CHECK (
    source_model_id IS NULL
    OR target_model_id IS NULL
    OR source_model_id <> target_model_id
  )
);

CREATE INDEX IF NOT EXISTS relation_evidence_tenant_predicate_idx
  ON relation_evidence (tenant_id, predicate, created_at DESC);

CREATE INDEX IF NOT EXISTS relation_evidence_model_pair_idx
  ON relation_evidence (
    tenant_id, source_model_id, target_model_id, edge_kind_hint, created_at DESC
  )
  WHERE source_model_id IS NOT NULL AND target_model_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS relation_evidence_source_observation_idx
  ON relation_evidence (tenant_id, source_observation_id, created_at DESC)
  WHERE source_observation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS relation_evidence_think_run_idx
  ON relation_evidence (tenant_id, think_run_id, created_at DESC)
  WHERE think_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS relation_evidence_scope_gin_idx
  ON relation_evidence USING gin (scope_entities);

CREATE TABLE IF NOT EXISTS model_pair_evidence (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  model_a_id UUID NOT NULL,
  model_b_id UUID NOT NULL,
  primitive TEXT NOT NULL DEFAULT 'UNKNOWN',
  co_retrieved_count INTEGER NOT NULL DEFAULT 0 CHECK (co_retrieved_count >= 0),
  co_used_valid_diff_count INTEGER NOT NULL DEFAULT 0 CHECK (
    co_used_valid_diff_count >= 0
  ),
  explicit_relation_count INTEGER NOT NULL DEFAULT 0 CHECK (
    explicit_relation_count >= 0
  ),
  think_edge_op_count INTEGER NOT NULL DEFAULT 0 CHECK (
    think_edge_op_count >= 0
  ),
  t4_accept_count INTEGER NOT NULL DEFAULT 0 CHECK (t4_accept_count >= 0),
  t4_reject_count INTEGER NOT NULL DEFAULT 0 CHECK (t4_reject_count >= 0),
  no_edge_count INTEGER NOT NULL DEFAULT 0 CHECK (no_edge_count >= 0),
  positive_outcome_count INTEGER NOT NULL DEFAULT 0 CHECK (
    positive_outcome_count >= 0
  ),
  negative_outcome_count INTEGER NOT NULL DEFAULT 0 CHECK (
    negative_outcome_count >= 0
  ),
  direction_votes JSONB NOT NULL DEFAULT '{}'::jsonb,
  edge_kind_votes JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (
    confidence_score >= 0.0 AND confidence_score <= 1.0
  ),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (model_a_id <> model_b_id),
  CHECK (btrim(primitive) <> ''),
  CONSTRAINT model_pair_evidence_unique
    UNIQUE (tenant_id, model_a_id, model_b_id, primitive)
);

CREATE INDEX IF NOT EXISTS model_pair_evidence_confidence_idx
  ON model_pair_evidence (
    tenant_id, confidence_score DESC, last_seen_at DESC
  );

CREATE INDEX IF NOT EXISTS model_pair_evidence_model_a_idx
  ON model_pair_evidence (tenant_id, model_a_id, confidence_score DESC);

CREATE INDEX IF NOT EXISTS model_pair_evidence_model_b_idx
  ON model_pair_evidence (tenant_id, model_b_id, confidence_score DESC);

CREATE INDEX IF NOT EXISTS model_pair_evidence_edge_kind_votes_gin_idx
  ON model_pair_evidence USING gin (edge_kind_votes);

COMMIT;
