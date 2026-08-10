-- Immutable source evidence and observation-to-evidence lineage.

BEGIN;

CREATE TABLE IF NOT EXISTS source_evidence (
  id                        UUID PRIMARY KEY,
  tenant_id                 UUID NOT NULL REFERENCES tenants(id),
  source                    TEXT NOT NULL CHECK (btrim(source) <> ''),
  connector_installation_id UUID REFERENCES source_connector_installations(id),
  installation_scope        TEXT NOT NULL CHECK (btrim(installation_scope) <> ''),
  source_channel            TEXT NOT NULL CHECK (btrim(source_channel) <> ''),
  source_object_type        TEXT NOT NULL CHECK (btrim(source_object_type) <> ''),
  source_object_id          TEXT NOT NULL CHECK (btrim(source_object_id) <> ''),
  source_revision_id        TEXT NOT NULL CHECK (btrim(source_revision_id) <> ''),
  operation                 TEXT NOT NULL CHECK (
    operation IN ('create', 'update', 'delete', 'retract', 'snapshot')
  ),
  source_recorded_at        TIMESTAMPTZ NOT NULL,
  valid_from                TIMESTAMPTZ,
  valid_to                  TIMESTAMPTZ,
  supersedes_evidence_id    UUID REFERENCES source_evidence(id),
  parent_ref                JSONB,
  container_ref             JSONB,
  thread_id                 TEXT,
  raw_object_key            TEXT,
  content_hash              TEXT NOT NULL CHECK (
    content_hash ~ '^[0-9a-f]{40,64}$'
  ),
  raw_ingested_at           TIMESTAMPTZ NOT NULL,
  normalized_at             TIMESTAMPTZ NOT NULL,
  ingress_kind              TEXT NOT NULL CHECK (btrim(ingress_kind) <> ''),
  ingress_metadata          JSONB NOT NULL DEFAULT '{}'::jsonb
                                CHECK (jsonb_typeof(ingress_metadata) = 'object'),
  idem_hints                JSONB NOT NULL DEFAULT '{}'::jsonb
                                CHECK (jsonb_typeof(idem_hints) = 'object'),
  contract_version          INTEGER NOT NULL CHECK (contract_version > 0),
  connector_version         TEXT NOT NULL CHECK (btrim(connector_version) <> ''),
  parser_version            TEXT NOT NULL CHECK (btrim(parser_version) <> ''),
  normalizer_version        TEXT NOT NULL CHECK (btrim(normalizer_version) <> ''),
  raw_retention_state       TEXT NOT NULL CHECK (
    raw_retention_state IN ('available', 'expired', 'not_stored')
  ),
  raw_expired_at            TIMESTAMPTZ,
  first_seen_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  CHECK (
    (raw_retention_state = 'available' AND raw_object_key IS NOT NULL)
    OR raw_retention_state IN ('expired', 'not_stored')
  ),
  CHECK (
    (raw_retention_state = 'expired' AND raw_expired_at IS NOT NULL)
    OR (raw_retention_state <> 'expired' AND raw_expired_at IS NULL)
  ),
  UNIQUE (
    tenant_id, source, installation_scope, source_object_type,
    source_object_id, source_revision_id, operation
  )
);

CREATE INDEX IF NOT EXISTS source_evidence_object_history_idx
  ON source_evidence (
    tenant_id, source, installation_scope, source_object_type,
    source_object_id, source_recorded_at DESC
  );

CREATE INDEX IF NOT EXISTS source_evidence_hash_idx
  ON source_evidence (tenant_id, content_hash);

ALTER TABLE observations
  ADD COLUMN IF NOT EXISTS evidence_id UUID REFERENCES source_evidence(id);

ALTER TABLE observations
  DROP CONSTRAINT IF EXISTS observations_tenant_source_external_occurred_key;

ALTER TABLE observations
  DROP CONSTRAINT IF EXISTS observations_source_channel_external_id_occurred_at_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'observations'::regclass
       AND conname = 'observations_tenant_evidence_occurred_key'
  ) THEN
    ALTER TABLE observations
      ADD CONSTRAINT observations_tenant_evidence_occurred_key
      UNIQUE (tenant_id, evidence_id, occurred_at);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS observations_evidence_idx
  ON observations (tenant_id, evidence_id)
  WHERE evidence_id IS NOT NULL;

ALTER TABLE source_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_evidence FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_evidence;
CREATE POLICY tenant_isolation ON source_evidence
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
