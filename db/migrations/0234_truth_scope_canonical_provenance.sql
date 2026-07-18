-- Preserve unresolved stable extracted business coordinates even before a
-- scope is promoted into a resource table. This is provenance, not an entity
-- existence assertion; subject_id remains the durable UUID identity.
ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS canonical_ref TEXT;

ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS display_label TEXT;
