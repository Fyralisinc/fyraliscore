-- 0069_relationship_edge_type_candidates.sql
--
-- Let the relationship candidate layer represent ontology gaps directly.
-- These rows are pre-truth proposals for missing edge kinds; they do not
-- create accepted model_edges until a later promotion/adjudication step.

BEGIN;

ALTER TABLE relationship_candidates
  DROP CONSTRAINT IF EXISTS relationship_candidates_candidate_kind_check;

ALTER TABLE relationship_candidates
  ADD CONSTRAINT relationship_candidates_candidate_kind_check
  CHECK (candidate_kind IN ('edge', 'situation', 'edge_type'));

ALTER TABLE relationship_candidates
  DROP CONSTRAINT IF EXISTS relationship_candidates_basis_check;

ALTER TABLE relationship_candidates
  ADD CONSTRAINT relationship_candidates_basis_check
  CHECK (basis IN (
    'observed',
    'inferred',
    'correlated',
    'topology_suggested',
    'causal_hypothesis',
    'causal_confirmed',
    'ontology_gap'
  ));

ALTER TABLE relationship_candidates
  DROP CONSTRAINT IF EXISTS relationship_candidates_check;

ALTER TABLE relationship_candidates
  DROP CONSTRAINT IF EXISTS relationship_candidates_shape_check;

ALTER TABLE relationship_candidates
  ADD CONSTRAINT relationship_candidates_shape_check
  CHECK (
    (candidate_kind = 'edge'
      AND source_model_id IS NOT NULL
      AND target_model_id IS NOT NULL
      AND edge_kind IS NOT NULL
      AND source_model_id != target_model_id)
    OR
    (candidate_kind = 'situation'
      AND cardinality(member_model_ids) >= 2
      AND proposed_proposition IS NOT NULL)
    OR
    (candidate_kind = 'edge_type'
      AND source_model_id IS NULL
      AND target_model_id IS NULL
      AND edge_kind IS NULL
      AND proposed_proposition IS NOT NULL
      AND proposed_proposition->>'kind' = 'ontology_gap'
      AND COALESCE(proposed_proposition->>'proposed_edge_kind', '') != '')
  );

CREATE INDEX IF NOT EXISTS relationship_candidates_edge_type_idx
  ON relationship_candidates (
    tenant_id,
    (proposed_proposition->>'proposed_edge_kind'),
    review_status,
    judgment_leverage_score DESC,
    created_at DESC
  )
  WHERE candidate_kind = 'edge_type';

COMMIT;
