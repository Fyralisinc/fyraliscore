-- =====================================================================
-- 0220_source_identity_attachment_surfaces.sql
--
-- Scope source-identity authority to one exact source-provided mention
-- surface. Observation-level authority is too broad because one structured
-- object can contain many unrelated entity mentions.
-- =====================================================================

BEGIN;

ALTER TABLE observation_source_identity_bindings
  ADD COLUMN IF NOT EXISTS source_surface TEXT;
ALTER TABLE observation_source_identity_bindings
  ADD COLUMN IF NOT EXISTS normalized_source_surface TEXT;

-- Existing attachments become safe-by-default: they can resolve only when the
-- resolver phrase is the full native identifier, never every phrase in the
-- observation.
UPDATE observation_source_identity_bindings attachment
SET source_surface = binding.source_native_identifier,
    normalized_source_surface = regexp_replace(
      lower(binding.source_native_identifier), '\s+', ' ', 'g'
    )
FROM source_identity_bindings binding
WHERE attachment.tenant_id=binding.tenant_id
  AND attachment.binding_id=binding.id
  AND attachment.binding_version=binding.binding_version
  AND (
    attachment.source_surface IS NULL
    OR attachment.normalized_source_surface IS NULL
  );

ALTER TABLE observation_source_identity_bindings
  ALTER COLUMN source_surface SET NOT NULL;
ALTER TABLE observation_source_identity_bindings
  ALTER COLUMN normalized_source_surface SET NOT NULL;

ALTER TABLE observation_source_identity_bindings
  DROP CONSTRAINT IF EXISTS observation_source_identity_bindings_pkey;
ALTER TABLE observation_source_identity_bindings
  ADD CONSTRAINT observation_source_identity_bindings_pkey PRIMARY KEY (
    tenant_id, observation_id, observation_occurred_at,
    binding_id, binding_version, normalized_source_surface
  );

ALTER TABLE observation_source_identity_bindings
  DROP CONSTRAINT IF EXISTS source_identity_attachment_surface_nonempty;
ALTER TABLE observation_source_identity_bindings
  ADD CONSTRAINT source_identity_attachment_surface_nonempty CHECK (
    length(btrim(source_surface)) > 0
    AND length(btrim(normalized_source_surface)) > 0
  );

CREATE INDEX IF NOT EXISTS observation_source_identity_surface_lookup_idx
  ON observation_source_identity_bindings (
    tenant_id, observation_id, normalized_source_surface,
    observation_occurred_at, attached_at
  );

COMMENT ON COLUMN observation_source_identity_bindings.source_surface IS
  'Exact source-provided surface whose mention may consume this binding.';
COMMENT ON COLUMN observation_source_identity_bindings.normalized_source_surface IS
  'Application-normalized source surface used for exact resolver matching.';

COMMIT;
