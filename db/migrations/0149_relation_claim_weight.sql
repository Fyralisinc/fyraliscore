-- 0149_relation_claim_weight.sql
--
-- Keep relation_claims aligned with model_edges ontology semantics. Accepted
-- relation claims can create model_edges, so they need the same optional
-- edge-kind weight payload.

BEGIN;

ALTER TABLE relation_claims
  ADD COLUMN IF NOT EXISTS weight DOUBLE PRECISION;

ALTER TABLE relation_claims
  DROP CONSTRAINT IF EXISTS relation_claims_weight_check;

ALTER TABLE relation_claims
  ADD CONSTRAINT relation_claims_weight_check
  CHECK (weight IS NULL OR (weight >= 0.0 AND weight <= 1.0));

COMMIT;
