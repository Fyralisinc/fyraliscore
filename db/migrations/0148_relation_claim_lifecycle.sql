-- 0148_relation_claim_lifecycle.sql
--
-- First-class relation claims. Unlike relation_evidence/model_pair_evidence,
-- these rows are write-plan objects: a concrete relation-bearing fact with
-- endpoint binding state, adjudication state, and optional accepted edge ids.

BEGIN;

CREATE TABLE IF NOT EXISTS relation_claims (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  source_observation_id UUID,
  think_run_id UUID,
  source_model_id UUID,
  target_model_id UUID,
  subject_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  object_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  predicate TEXT NOT NULL,
  edge_kind TEXT NOT NULL,
  direction TEXT NOT NULL DEFAULT 'source_to_target' CHECK (
    direction IN ('source_to_target', 'target_to_source', 'symmetric', 'unknown')
  ),
  endpoint_binding_status TEXT NOT NULL DEFAULT 'unbound' CHECK (
    endpoint_binding_status IN ('bound', 'partially_bound', 'unbound', 'ambiguous')
  ),
  write_policy TEXT NOT NULL DEFAULT 'candidate' CHECK (
    write_policy IN ('accepted_edge', 'candidate', 'needs_review', 'no_edge')
  ),
  status TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'accepted', 'candidate', 'needs_review', 'rejected', 'retired')
  ),
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  weight DOUBLE PRECISION CHECK (
    weight IS NULL OR (weight >= 0.0 AND weight <= 1.0)
  ),
  binding_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0 CHECK (
    binding_confidence >= 0.0 AND binding_confidence <= 1.0
  ),
  evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_text TEXT,
  explanation TEXT,
  accepted_edge_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  temporal_bounds JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ,
  CHECK (btrim(predicate) <> ''),
  CHECK (btrim(edge_kind) <> ''),
  CHECK (
    source_model_id IS NULL
    OR target_model_id IS NULL
    OR source_model_id <> target_model_id
  )
);

CREATE INDEX IF NOT EXISTS relation_claims_tenant_status_idx
  ON relation_claims (tenant_id, status, edge_kind, created_at DESC);

CREATE INDEX IF NOT EXISTS relation_claims_model_pair_idx
  ON relation_claims (
    tenant_id, source_model_id, target_model_id, edge_kind, created_at DESC
  )
  WHERE source_model_id IS NOT NULL AND target_model_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS relation_claims_source_observation_idx
  ON relation_claims (tenant_id, source_observation_id, created_at DESC)
  WHERE source_observation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS relation_claims_evidence_events_idx
  ON relation_claims USING gin (evidence_event_ids);

CREATE INDEX IF NOT EXISTS relation_claims_evidence_models_idx
  ON relation_claims USING gin (evidence_model_ids);

COMMIT;
