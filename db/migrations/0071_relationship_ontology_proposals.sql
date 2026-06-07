-- 0071_relationship_ontology_proposals.sql
--
-- Durable lifecycle for edge-type ontology gaps.
-- `relationship_candidates(candidate_kind='edge_type')` records individual
-- examples. This table clusters those examples into one reviewable proposal
-- per tenant + proposed_edge_kind so the ontology can evolve from evidence
-- instead of raw candidate backlog.

BEGIN;

CREATE TABLE IF NOT EXISTS relationship_ontology_proposals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposed_edge_kind TEXT NOT NULL,
  status TEXT NOT NULL CHECK (
    status IN ('draft', 'review_ready', 'accepted', 'rejected', 'superseded')
  ),
  description TEXT NOT NULL DEFAULT '',
  relationship_summary TEXT NOT NULL DEFAULT '',
  parent_kind TEXT,
  nearest_existing_kind TEXT,
  retrieval_fallback_kind TEXT,
  directionality TEXT NOT NULL DEFAULT 'unknown' CHECK (
    directionality IN ('directed', 'symmetric', 'unknown')
  ),
  dropped_dimensions TEXT[] NOT NULL DEFAULT '{}'::text[],
  example_candidate_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  example_count INT NOT NULL DEFAULT 0 CHECK (example_count >= 0),
  evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  promotion_criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
  facets JSONB NOT NULL DEFAULT '{}'::jsonb,
  avg_judgment_leverage_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  max_judgment_leverage_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at TIMESTAMPTZ,
  promoted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, proposed_edge_kind)
);

CREATE INDEX IF NOT EXISTS relationship_ontology_proposals_review_idx
  ON relationship_ontology_proposals (
    tenant_id,
    status,
    max_judgment_leverage_score DESC,
    last_seen_at DESC
  );

CREATE INDEX IF NOT EXISTS relationship_ontology_proposals_fallback_idx
  ON relationship_ontology_proposals (
    tenant_id,
    retrieval_fallback_kind,
    status
  );

ALTER TABLE relationship_ontology_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE relationship_ontology_proposals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON relationship_ontology_proposals;
CREATE POLICY tenant_isolation ON relationship_ontology_proposals
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;

