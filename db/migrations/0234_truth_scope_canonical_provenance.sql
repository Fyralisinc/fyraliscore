-- Preserve unresolved stable extracted business coordinates even before a
-- scope is promoted into a resource table. This is provenance, not an entity
-- existence assertion; subject_id remains the durable UUID identity.
ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS canonical_ref TEXT;

ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS display_label TEXT;

ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS canonical_ref_status TEXT;

ALTER TABLE model_truth_scope_bindings
  ADD COLUMN IF NOT EXISTS normalization_version INTEGER;

DO $$ BEGIN
  ALTER TABLE model_truth_scope_bindings
    ADD CONSTRAINT model_truth_scope_canonical_ref_shape_chk CHECK (
      canonical_ref IS NULL OR (
        canonical_ref LIKE '%:%' AND canonical_ref NOT LIKE 'batch:%'
      )
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE model_truth_scope_bindings
    ADD CONSTRAINT model_truth_scope_provenance_status_chk CHECK (
      (canonical_ref IS NULL AND canonical_ref_status IS NULL
       AND normalization_version IS NULL)
      OR
      (canonical_ref IS NOT NULL
       AND canonical_ref_status IN ('provisional', 'resolved')
       AND normalization_version >= 1)
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
