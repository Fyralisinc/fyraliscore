-- =====================================================================
-- 0218_source_identity_bindings.sql
--
-- Tenant-scoped, bitemporal mappings from authenticated source-native
-- object identifiers to canonical company referents.  Free text and generic
-- entities_mentioned sidecars are deliberately excluded from this registry:
-- lookup is permitted only from the durable Observation source envelope.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS source_identity_bindings (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  binding_version INTEGER NOT NULL DEFAULT 1 CHECK (binding_version > 0),
  source_system TEXT NOT NULL CHECK (length(btrim(source_system)) > 0),
  source_native_identifier TEXT NOT NULL CHECK (
    length(btrim(source_native_identifier)) > 0
  ),
  source_identity_authority_ref TEXT NOT NULL CHECK (
    length(btrim(source_identity_authority_ref)) > 0
  ),
  canonical_referent JSONB NOT NULL CHECK (
    jsonb_typeof(canonical_referent) = 'object'
    AND length(COALESCE(canonical_referent ->> 'type', '')) > 0
    AND length(COALESCE(canonical_referent ->> 'id', '')) > 0
    AND COALESCE((canonical_referent ->> 'version')::integer, 1) > 0
  ),
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  transaction_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  transaction_to TIMESTAMPTZ,
  evidence_refs TEXT[] NOT NULL CHECK (cardinality(evidence_refs) > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  CHECK (transaction_to IS NULL OR transaction_to > transaction_from),
  UNIQUE (tenant_id, id, binding_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS source_identity_bindings_current_idx
  ON source_identity_bindings (
    tenant_id, source_system, source_native_identifier
  )
  WHERE valid_to IS NULL AND transaction_to IS NULL;

CREATE INDEX IF NOT EXISTS source_identity_bindings_lookup_idx
  ON source_identity_bindings (
    tenant_id, source_system, source_native_identifier,
    valid_from, transaction_from
  );

CREATE UNIQUE INDEX IF NOT EXISTS observations_tenant_identity_revision_idx
  ON observations (tenant_id, id, occurred_at);

CREATE TABLE IF NOT EXISTS observation_source_identity_bindings (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  observation_id UUID NOT NULL,
  observation_occurred_at TIMESTAMPTZ NOT NULL,
  binding_id UUID NOT NULL,
  binding_version INTEGER NOT NULL CHECK (binding_version > 0),
  attachment_authority_ref TEXT NOT NULL CHECK (
    length(btrim(attachment_authority_ref)) > 0
  ),
  attached_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    tenant_id, observation_id, observation_occurred_at,
    binding_id, binding_version
  ),
  FOREIGN KEY (tenant_id, observation_id, observation_occurred_at)
    REFERENCES observations (tenant_id, id, occurred_at),
  FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES source_identity_bindings (
      tenant_id, id, binding_version
    )
);

CREATE INDEX IF NOT EXISTS observation_source_identity_lookup_idx
  ON observation_source_identity_bindings (
    tenant_id, observation_id, observation_occurred_at, attached_at
  );

ALTER TABLE source_identity_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_identity_bindings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_identity_bindings;
CREATE POLICY tenant_isolation ON source_identity_bindings
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

ALTER TABLE observation_source_identity_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE observation_source_identity_bindings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON observation_source_identity_bindings;
CREATE POLICY tenant_isolation ON observation_source_identity_bindings
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

COMMENT ON TABLE source_identity_bindings IS
  'Authenticated source-envelope identity mappings; free text is never binding authority.';
COMMENT ON TABLE observation_source_identity_bindings IS
  'Ingestion-authorized evidence that one Observation carries a specific source identity binding.';

COMMIT;
