-- 0044_relationship_intelligence.sql
--
-- Candidate layer for relationship intelligence. This table sits before
-- accepted `model_edges` or composite `situation` Models: deterministic
-- miners, topology events, anomaly passes, and LLM adjudication can place
-- relationship/situation hypotheses here without making them accepted
-- memory. Today can review them; Ledger can record their lifecycle.

BEGIN;

CREATE TABLE IF NOT EXISTS relationship_candidates (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  candidate_kind TEXT NOT NULL CHECK (candidate_kind IN ('edge', 'situation')),
  basis TEXT NOT NULL CHECK (basis IN (
    'observed',
    'inferred',
    'correlated',
    'topology_suggested',
    'causal_hypothesis',
    'causal_confirmed'
  )),
  source_model_id UUID,
  target_model_id UUID,
  edge_kind TEXT,
  member_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  counterevidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  proposed_proposition JSONB,
  explanation TEXT NOT NULL,
  novelty_score FLOAT NOT NULL DEFAULT 0.0,
  impact_score FLOAT NOT NULL DEFAULT 0.0,
  actionability_score FLOAT NOT NULL DEFAULT 0.0,
  urgency_score FLOAT NOT NULL DEFAULT 0.0,
  uncertainty_score FLOAT NOT NULL DEFAULT 0.0,
  authority_required_score FLOAT NOT NULL DEFAULT 0.0,
  reversibility_score FLOAT NOT NULL DEFAULT 0.0,
  confidence_score FLOAT NOT NULL DEFAULT 0.0,
  judgment_leverage_score FLOAT NOT NULL DEFAULT 0.0,
  source TEXT NOT NULL DEFAULT 'relationship_candidate_service',
  review_status TEXT NOT NULL DEFAULT 'candidate' CHECK (review_status IN (
    'candidate',
    'needs_review',
    'accepted',
    'rejected',
    'contested',
    'retired'
  )),
  accepted_model_id UUID,
  accepted_edge_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
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
  ),
  CHECK (novelty_score >= 0.0 AND novelty_score <= 1.0),
  CHECK (impact_score >= 0.0 AND impact_score <= 1.0),
  CHECK (actionability_score >= 0.0 AND actionability_score <= 1.0),
  CHECK (urgency_score >= 0.0 AND urgency_score <= 1.0),
  CHECK (uncertainty_score >= 0.0 AND uncertainty_score <= 1.0),
  CHECK (authority_required_score >= 0.0 AND authority_required_score <= 1.0),
  CHECK (reversibility_score >= 0.0 AND reversibility_score <= 1.0),
  CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
  CHECK (judgment_leverage_score >= 0.0 AND judgment_leverage_score <= 1.0)
);

CREATE INDEX IF NOT EXISTS relationship_candidates_review_idx
  ON relationship_candidates (
    tenant_id, review_status, judgment_leverage_score DESC, created_at DESC
  );

CREATE INDEX IF NOT EXISTS relationship_candidates_edge_pair_idx
  ON relationship_candidates (
    tenant_id, source_model_id, target_model_id, edge_kind, created_at DESC
  )
  WHERE candidate_kind = 'edge';

CREATE INDEX IF NOT EXISTS relationship_candidates_members_idx
  ON relationship_candidates USING gin (member_model_ids);

CREATE INDEX IF NOT EXISTS relationship_candidates_evidence_models_idx
  ON relationship_candidates USING gin (evidence_model_ids);

COMMIT;
