-- Source-grounded identity references, mentions, and reproducible resolver runs.

BEGIN;

CREATE TABLE IF NOT EXISTS identity_source_references (
  id                         UUID PRIMARY KEY,
  tenant_id                  UUID NOT NULL REFERENCES tenants(id),
  connector_installation_id  UUID REFERENCES source_connector_installations(id),
  installation_scope         TEXT NOT NULL CHECK (btrim(installation_scope) <> ''),
  source                     TEXT NOT NULL CHECK (btrim(source) <> ''),
  native_type                TEXT NOT NULL CHECK (btrim(native_type) <> ''),
  native_id                  TEXT NOT NULL CHECK (btrim(native_id) <> ''),
  stable_key                 TEXT NOT NULL CHECK (stable_key ~ '^[0-9a-f]{64}$'),
  reference_kind             TEXT NOT NULL CHECK (
    reference_kind IN (
      'principal', 'artifact', 'container', 'conversation', 'work_record',
      'scheduled_event', 'transcript', 'operational_event',
      'financial_record', 'employment_record', 'external_resource', 'url'
    )
  ),
  attributes                 JSONB NOT NULL DEFAULT '{}'::jsonb
                                  CHECK (jsonb_typeof(attributes) = 'object'),
  first_evidence_id          UUID NOT NULL REFERENCES source_evidence(id),
  latest_evidence_id         UUID NOT NULL REFERENCES source_evidence(id),
  valid_from                 TIMESTAMPTZ,
  valid_to                   TIMESTAMPTZ,
  status                     TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'deleted', 'superseded')
  ),
  version                    BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
  first_seen_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  UNIQUE (tenant_id, stable_key),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS identity_source_references_lookup_idx
  ON identity_source_references (
    tenant_id, source, installation_scope, native_type, native_id
  );
CREATE INDEX IF NOT EXISTS identity_source_references_kind_idx
  ON identity_source_references (tenant_id, reference_kind, status);

CREATE TABLE IF NOT EXISTS entity_mentions (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  observation_id           UUID,
  observation_occurred_at  TIMESTAMPTZ,
  evidence_id              UUID REFERENCES source_evidence(id),
  source_reference_id      UUID,
  mention_key              TEXT NOT NULL CHECK (mention_key ~ '^[0-9a-f]{64}$'),
  mention_kind             TEXT NOT NULL CHECK (
    mention_kind IN (
      'structured_reference', 'source_actor', 'text', 'coreference', 'query'
    )
  ),
  text                     TEXT NOT NULL CHECK (btrim(text) <> ''),
  span_start               INTEGER CHECK (span_start IS NULL OR span_start >= 0),
  span_end                 INTEGER CHECK (span_end IS NULL OR span_end >= 0),
  expected_types           JSONB NOT NULL DEFAULT '[]'::jsonb
                                  CHECK (jsonb_typeof(expected_types) = 'array'),
  context                  JSONB NOT NULL DEFAULT '{}'::jsonb
                                  CHECK (jsonb_typeof(context) = 'object'),
  status                   TEXT NOT NULL DEFAULT 'registered' CHECK (
    status IN ('registered', 'superseded')
  ),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (span_start IS NULL AND span_end IS NULL)
    OR (span_start IS NOT NULL AND span_end IS NOT NULL AND span_end > span_start)
  ),
  CHECK (
    mention_kind = 'query'
    OR (observation_id IS NOT NULL AND observation_occurred_at IS NOT NULL
        AND evidence_id IS NOT NULL)
  ),
  UNIQUE (tenant_id, mention_key),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS entity_mentions_observation_idx
  ON entity_mentions (tenant_id, observation_id, created_at);
CREATE INDEX IF NOT EXISTS entity_mentions_source_ref_idx
  ON entity_mentions (tenant_id, source_reference_id)
  WHERE source_reference_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS entity_mentions_expected_types_idx
  ON entity_mentions USING gin (expected_types);

CREATE TABLE IF NOT EXISTS identity_resolution_runs (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  input_kind               TEXT NOT NULL CHECK (
    input_kind IN ('observation', 'query', 'reprocess')
  ),
  observation_id           UUID,
  observation_occurred_at  TIMESTAMPTZ,
  requester_actor_id       UUID REFERENCES actors(id),
  input_hash               TEXT NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
  resolver_name            TEXT NOT NULL CHECK (btrim(resolver_name) <> ''),
  resolver_version         TEXT NOT NULL CHECK (btrim(resolver_version) <> ''),
  policy_version           TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
  capability_snapshot      JSONB NOT NULL CHECK (jsonb_typeof(capability_snapshot) = 'object'),
  status                   TEXT NOT NULL DEFAULT 'running' CHECK (
    status IN ('running', 'completed', 'failed')
  ),
  result_hash              TEXT CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'),
  failure                  TEXT,
  started_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at             TIMESTAMPTZ,
  CHECK (
    input_kind = 'query'
    OR (observation_id IS NOT NULL AND observation_occurred_at IS NOT NULL)
  ),
  UNIQUE (tenant_id, input_kind, input_hash, resolver_name, resolver_version),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS identity_resolution_runs_observation_idx
  ON identity_resolution_runs (tenant_id, observation_id, started_at DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_source_references'::regclass
       AND conname = 'identity_source_references_tenant_first_evidence_fkey'
  ) THEN
    ALTER TABLE identity_source_references
      ADD CONSTRAINT identity_source_references_tenant_first_evidence_fkey
      FOREIGN KEY (tenant_id, first_evidence_id)
      REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_source_references'::regclass
       AND conname = 'identity_source_references_tenant_latest_evidence_fkey'
  ) THEN
    ALTER TABLE identity_source_references
      ADD CONSTRAINT identity_source_references_tenant_latest_evidence_fkey
      FOREIGN KEY (tenant_id, latest_evidence_id)
      REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_source_references'::regclass
       AND conname = 'identity_source_references_tenant_installation_fkey'
  ) THEN
    ALTER TABLE identity_source_references
      ADD CONSTRAINT identity_source_references_tenant_installation_fkey
      FOREIGN KEY (tenant_id, connector_installation_id)
      REFERENCES source_connector_installations(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'entity_mentions'::regclass
       AND conname = 'entity_mentions_observation_fkey'
  ) THEN
    ALTER TABLE entity_mentions
      ADD CONSTRAINT entity_mentions_observation_fkey
      FOREIGN KEY (observation_id, observation_occurred_at)
      REFERENCES observations(id, occurred_at);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'entity_mentions'::regclass
       AND conname = 'entity_mentions_tenant_evidence_fkey'
  ) THEN
    ALTER TABLE entity_mentions
      ADD CONSTRAINT entity_mentions_tenant_evidence_fkey
      FOREIGN KEY (tenant_id, evidence_id)
      REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'entity_mentions'::regclass
       AND conname = 'entity_mentions_tenant_source_ref_fkey'
  ) THEN
    ALTER TABLE entity_mentions
      ADD CONSTRAINT entity_mentions_tenant_source_ref_fkey
      FOREIGN KEY (tenant_id, source_reference_id)
      REFERENCES identity_source_references(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_runs'::regclass
       AND conname = 'identity_resolution_runs_observation_fkey'
  ) THEN
    ALTER TABLE identity_resolution_runs
      ADD CONSTRAINT identity_resolution_runs_observation_fkey
      FOREIGN KEY (observation_id, observation_occurred_at)
      REFERENCES observations(id, occurred_at);
  END IF;
END $$;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'identity_source_references', 'entity_mentions', 'identity_resolution_runs'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', table_name);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', table_name);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      ' NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      ' OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') WITH CHECK ('
      ' NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      ' OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      table_name
    );
  END LOOP;
END $$;

COMMIT;
