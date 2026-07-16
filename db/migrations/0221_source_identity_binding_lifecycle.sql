-- =====================================================================
-- 0221_source_identity_binding_lifecycle.sql
--
-- Add append-only valid-time lifecycle to authenticated source identity
-- bindings. Row IDs remain immutable; lineage_id links corrected and
-- successor versions so existing Observation attachments never redirect.
-- =====================================================================

BEGIN;

ALTER TABLE source_identity_bindings
  ADD COLUMN IF NOT EXISTS lineage_id UUID;
ALTER TABLE source_identity_bindings
  ADD COLUMN IF NOT EXISTS predecessor_binding_id UUID;
ALTER TABLE source_identity_bindings
  ADD COLUMN IF NOT EXISTS predecessor_binding_version INTEGER;
ALTER TABLE source_identity_bindings
  ADD COLUMN IF NOT EXISTS lifecycle_operation_kind TEXT;
ALTER TABLE source_identity_bindings
  ADD COLUMN IF NOT EXISTS lifecycle_operation_ref TEXT;

UPDATE source_identity_bindings
SET lineage_id=id
WHERE lineage_id IS NULL;

UPDATE source_identity_bindings
SET lifecycle_operation_kind='bind'
WHERE lifecycle_operation_kind IS NULL;

UPDATE source_identity_bindings
SET lifecycle_operation_ref=
  'legacy-bind:' || id::text || ':' || binding_version::text
WHERE lifecycle_operation_ref IS NULL;

ALTER TABLE source_identity_bindings
  ALTER COLUMN lineage_id SET NOT NULL;
ALTER TABLE source_identity_bindings
  ALTER COLUMN lifecycle_operation_kind SET NOT NULL;
ALTER TABLE source_identity_bindings
  ALTER COLUMN lifecycle_operation_ref SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname='source_identity_binding_predecessor_complete'
      AND conrelid='source_identity_bindings'::regclass
  ) THEN
    ALTER TABLE source_identity_bindings
      ADD CONSTRAINT source_identity_binding_predecessor_complete CHECK (
        (predecessor_binding_id IS NULL)
        = (predecessor_binding_version IS NULL)
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname='source_identity_binding_lifecycle_kind'
      AND conrelid='source_identity_bindings'::regclass
  ) THEN
    ALTER TABLE source_identity_bindings
      ADD CONSTRAINT source_identity_binding_lifecycle_kind CHECK (
        lifecycle_operation_kind IN (
          'bind',
          'close',
          'revoke',
          'supersede_closure',
          'supersede_successor'
        )
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname='source_identity_binding_predecessor_fk'
      AND conrelid='source_identity_bindings'::regclass
  ) THEN
    ALTER TABLE source_identity_bindings
      ADD CONSTRAINT source_identity_binding_predecessor_fk
      FOREIGN KEY (
        tenant_id,
        predecessor_binding_id,
        predecessor_binding_version
      )
      REFERENCES source_identity_bindings (
        tenant_id,
        id,
        binding_version
      );
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS source_identity_binding_lineage_version_idx
  ON source_identity_bindings (
    tenant_id,
    lineage_id,
    binding_version
  );

CREATE INDEX IF NOT EXISTS source_identity_binding_lineage_lookup_idx
  ON source_identity_bindings (
    tenant_id,
    lineage_id,
    transaction_from,
    binding_version
  );

ALTER TABLE observation_source_identity_bindings
  ADD COLUMN IF NOT EXISTS binding_lineage_id UUID;

UPDATE observation_source_identity_bindings attachment
SET binding_lineage_id=binding.lineage_id
FROM source_identity_bindings binding
WHERE attachment.tenant_id=binding.tenant_id
  AND attachment.binding_id=binding.id
  AND attachment.binding_version=binding.binding_version
  AND attachment.binding_lineage_id IS NULL;

ALTER TABLE observation_source_identity_bindings
  ALTER COLUMN binding_lineage_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS
  observation_source_identity_lineage_surface_idx
  ON observation_source_identity_bindings (
    tenant_id,
    observation_id,
    observation_occurred_at,
    binding_lineage_id,
    normalized_source_surface
  );

CREATE TABLE IF NOT EXISTS source_identity_binding_operations (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  operation_ref TEXT NOT NULL CHECK (
    length(btrim(operation_ref)) > 0
  ),
  operation_kind TEXT NOT NULL CHECK (
    operation_kind IN ('close', 'revoke', 'supersede')
  ),
  binding_lineage_id UUID NOT NULL,
  expected_binding_version INTEGER NOT NULL CHECK (
    expected_binding_version > 0
  ),
  request_fingerprint TEXT NOT NULL CHECK (
    request_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  effective_at TIMESTAMPTZ NOT NULL,
  transaction_at TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
  evidence_refs TEXT[] NOT NULL CHECK (cardinality(evidence_refs) > 0),
  result_binding_refs JSONB NOT NULL CHECK (
    jsonb_typeof(result_binding_refs)='array'
    AND jsonb_array_length(result_binding_refs) > 0
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_ref)
);

CREATE INDEX IF NOT EXISTS source_identity_binding_operations_lineage_idx
  ON source_identity_binding_operations (
    tenant_id,
    binding_lineage_id,
    transaction_at
  );

ALTER TABLE source_identity_binding_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_identity_binding_operations FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON source_identity_binding_operations;
CREATE POLICY tenant_isolation ON source_identity_binding_operations
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON COLUMN source_identity_bindings.lineage_id IS
  'Stable identity across append-only binding versions; row id stays immutable.';
COMMENT ON TABLE source_identity_binding_operations IS
  'Idempotent close, revoke and supersede commands for binding lineages.';
COMMENT ON COLUMN observation_source_identity_bindings.binding_lineage_id IS
  'Denormalized lineage fence preventing attachment replay from changing versions.';

COMMIT;
