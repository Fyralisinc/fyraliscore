-- Tenant-safe identity assertions, perception claims, and evidence ACL lineage.

BEGIN;

-- Current actor mapping projection: scope every native identity to one tenant
-- and connector installation. Historical/proposed decisions live in the
-- identity_assertions ledger below.
ALTER TABLE actor_identity_mappings
  ADD COLUMN IF NOT EXISTS tenant_id UUID,
  ADD COLUMN IF NOT EXISTS connector_installation_id UUID,
  ADD COLUMN IF NOT EXISTS installation_scope TEXT;

UPDATE actor_identity_mappings AS mapping
   SET tenant_id = actor.tenant_id
  FROM actors AS actor
 WHERE actor.id = mapping.actor_id
   AND mapping.tenant_id IS NULL;

UPDATE actor_identity_mappings
   SET installation_scope = 'legacy:' || source_channel
 WHERE installation_scope IS NULL;

ALTER TABLE actor_identity_mappings
  ALTER COLUMN tenant_id SET NOT NULL,
  ALTER COLUMN installation_scope SET NOT NULL;

ALTER TABLE actor_identity_mappings
  DROP CONSTRAINT IF EXISTS actor_identity_mappings_pkey;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'actor_identity_mappings'::regclass
       AND conname = 'actor_identity_mappings_pkey'
  ) THEN
    ALTER TABLE actor_identity_mappings
      ADD CONSTRAINT actor_identity_mappings_pkey PRIMARY KEY (
        tenant_id, installation_scope, source_channel, source_actor_ref
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'actor_identity_mappings'::regclass
       AND conname = 'actor_identity_mappings_tenant_fkey'
  ) THEN
    ALTER TABLE actor_identity_mappings
      ADD CONSTRAINT actor_identity_mappings_tenant_fkey
      FOREIGN KEY (tenant_id) REFERENCES tenants(id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'actor_identity_mappings'::regclass
       AND conname = 'actor_identity_mappings_installation_fkey'
  ) THEN
    ALTER TABLE actor_identity_mappings
      ADD CONSTRAINT actor_identity_mappings_installation_fkey
      FOREIGN KEY (connector_installation_id)
      REFERENCES source_connector_installations(id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS actor_identity_mappings_tenant_actor_idx
  ON actor_identity_mappings (tenant_id, actor_id);

CREATE TABLE IF NOT EXISTS identity_assertions (
  id                    UUID PRIMARY KEY,
  tenant_id             UUID NOT NULL REFERENCES tenants(id),
  source_identity_key   TEXT NOT NULL CHECK (btrim(source_identity_key) <> ''),
  source_identity_ref   JSONB NOT NULL CHECK (jsonb_typeof(source_identity_ref) = 'object'),
  candidate_entity_ref  JSONB NOT NULL CHECK (jsonb_typeof(candidate_entity_ref) = 'object'),
  assertion_kind        TEXT NOT NULL CHECK (assertion_kind IN ('same_as', 'not_same_as')),
  status                TEXT NOT NULL CHECK (
    status IN ('proposed', 'accepted', 'rejected', 'superseded')
  ),
  confidence            DOUBLE PRECISION NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  evidence_id           UUID REFERENCES source_evidence(id),
  decision_provenance   JSONB NOT NULL DEFAULT '{}'::jsonb
                              CHECK (jsonb_typeof(decision_provenance) = 'object'),
  valid_from            TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to              TIMESTAMPTZ,
  version               BIGINT NOT NULL CHECK (version > 0),
  supersedes_assertion_id UUID REFERENCES identity_assertions(id),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at            TIMESTAMPTZ,
  CHECK (valid_to IS NULL OR valid_to >= valid_from),
  UNIQUE (tenant_id, source_identity_key, version)
);

CREATE INDEX IF NOT EXISTS identity_assertions_current_source_idx
  ON identity_assertions (tenant_id, source_identity_key, status, version DESC)
  WHERE status IN ('proposed', 'accepted');

CREATE INDEX IF NOT EXISTS identity_assertions_candidate_idx
  ON identity_assertions USING gin (candidate_entity_ref);

CREATE INDEX IF NOT EXISTS identity_assertions_evidence_idx
  ON identity_assertions (tenant_id, evidence_id)
  WHERE evidence_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS identity_cluster_events (
  id             UUID PRIMARY KEY,
  tenant_id      UUID NOT NULL REFERENCES tenants(id),
  event_kind     TEXT NOT NULL CHECK (event_kind IN ('merge', 'split', 'relabel')),
  before_refs    JSONB NOT NULL CHECK (jsonb_typeof(before_refs) = 'array'),
  after_refs     JSONB NOT NULL CHECK (jsonb_typeof(after_refs) = 'array'),
  evidence_ids   UUID[] NOT NULL DEFAULT '{}'::uuid[],
  provenance     JSONB NOT NULL DEFAULT '{}'::jsonb
                         CHECK (jsonb_typeof(provenance) = 'object'),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS identity_dependents (
  tenant_id           UUID NOT NULL REFERENCES tenants(id),
  identity_assertion_id UUID NOT NULL REFERENCES identity_assertions(id),
  dependent_kind      TEXT NOT NULL CHECK (
    dependent_kind IN ('observation', 'claim', 'topic', 'episode_membership')
  ),
  dependent_id        UUID NOT NULL,
  registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, identity_assertion_id, dependent_kind, dependent_id)
);

-- A general perception-layer claim is evidence-derived and precedes Think
-- models. Claimant perspective and exact evidence span are first-class.
CREATE TABLE IF NOT EXISTS perception_claims (
  id                    UUID PRIMARY KEY,
  tenant_id             UUID NOT NULL REFERENCES tenants(id),
  evidence_id           UUID NOT NULL REFERENCES source_evidence(id),
  observation_id        UUID NOT NULL,
  claimant_ref          JSONB CHECK (claimant_ref IS NULL OR jsonb_typeof(claimant_ref) = 'object'),
  subject_ref           JSONB NOT NULL CHECK (jsonb_typeof(subject_ref) = 'object'),
  predicate             TEXT NOT NULL CHECK (btrim(predicate) <> ''),
  object_value          JSONB NOT NULL,
  modality              TEXT NOT NULL CHECK (
    modality IN ('asserted', 'asked', 'proposed', 'planned', 'reported', 'denied', 'unknown')
  ),
  polarity              TEXT NOT NULL CHECK (polarity IN ('positive', 'negative', 'unknown')),
  confidence            DOUBLE PRECISION NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  valid_from            TIMESTAMPTZ,
  valid_to              TIMESTAMPTZ,
  evidence_span         JSONB NOT NULL CHECK (jsonb_typeof(evidence_span) = 'object'),
  extractor_kind        TEXT NOT NULL CHECK (extractor_kind IN ('deterministic', 'model', 'human')),
  extractor_name        TEXT NOT NULL CHECK (btrim(extractor_name) <> ''),
  extractor_version     TEXT NOT NULL CHECK (btrim(extractor_version) <> ''),
  extraction_run_id     UUID,
  claim_key             TEXT NOT NULL CHECK (claim_key ~ '^[0-9a-f]{64}$'),
  status                TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'superseded', 'rejected')
  ),
  supersedes_claim_id   UUID REFERENCES perception_claims(id),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
  UNIQUE (tenant_id, evidence_id, extractor_name, extractor_version, claim_key)
);

CREATE INDEX IF NOT EXISTS perception_claims_subject_predicate_idx
  ON perception_claims (tenant_id, predicate, status, created_at DESC);

CREATE INDEX IF NOT EXISTS perception_claims_subject_gin_idx
  ON perception_claims USING gin (subject_ref);

CREATE INDEX IF NOT EXISTS perception_claims_evidence_idx
  ON perception_claims (tenant_id, evidence_id, created_at);

-- Source-object authorization is evidence, not a channel-name heuristic.
ALTER TABLE source_evidence
  ADD COLUMN IF NOT EXISTS access_policy JSONB NOT NULL DEFAULT
    '{"visibility":"unknown","audience":[],"source_acl_version":"unknown"}'::jsonb,
  ADD COLUMN IF NOT EXISTS access_policy_hash TEXT,
  ADD COLUMN IF NOT EXISTS access_captured_at TIMESTAMPTZ;

ALTER TABLE source_evidence
  DROP CONSTRAINT IF EXISTS source_evidence_access_policy_object_check;
ALTER TABLE source_evidence
  ADD CONSTRAINT source_evidence_access_policy_object_check
  CHECK (jsonb_typeof(access_policy) = 'object');

CREATE INDEX IF NOT EXISTS source_evidence_access_policy_gin_idx
  ON source_evidence USING gin (access_policy);

-- Backfill current identity projections into the assertion ledger. UUIDs are
-- random only for migration history; all future application IDs use UUIDv7.
INSERT INTO identity_assertions (
  id, tenant_id, source_identity_key, source_identity_ref,
  candidate_entity_ref, assertion_kind, status, confidence,
  decision_provenance, valid_from, version, decided_at
)
SELECT gen_random_uuid(), mapping.tenant_id,
       'actor:' || mapping.installation_scope || ':' ||
         mapping.source_channel || ':' || mapping.source_actor_ref,
       jsonb_build_object(
         'kind', 'source_actor',
         'installation_scope', mapping.installation_scope,
         'source_channel', mapping.source_channel,
         'source_actor_ref', mapping.source_actor_ref
       ),
       jsonb_build_object('type', 'actor', 'id', mapping.actor_id),
       'same_as', 'accepted', mapping.confidence,
       jsonb_build_object('producer', '0192_actor_mapping_backfill'),
       mapping.created_at, 1, mapping.created_at
  FROM actor_identity_mappings AS mapping
ON CONFLICT (tenant_id, source_identity_key, version) DO NOTHING;

INSERT INTO identity_assertions (
  id, tenant_id, source_identity_key, source_identity_ref,
  candidate_entity_ref, assertion_kind, status, confidence,
  decision_provenance, valid_from, version, decided_at
)
SELECT gen_random_uuid(), alias.tenant_id,
       'alias:' || lower(regexp_replace(alias.alias_text, '\s+', ' ', 'g')) || ':' ||
         COALESCE(alias.actor_id::text, 'global'),
       jsonb_build_object(
         'kind', 'text_alias', 'alias_text', alias.alias_text,
         'actor_scope', alias.actor_id
       ),
       alias.resolved_entity_ref,
       'same_as', 'accepted', alias.confidence,
       jsonb_build_object(
         'producer', '0192_entity_alias_backfill',
         'source_event_id', alias.source_event_id
       ),
       alias.first_seen_at,
       row_number() OVER (
         PARTITION BY alias.tenant_id,
           lower(regexp_replace(alias.alias_text, '\s+', ' ', 'g')),
           alias.actor_id
         ORDER BY alias.first_seen_at, alias.id
       ),
       alias.first_seen_at
  FROM entity_aliases AS alias
ON CONFLICT (tenant_id, source_identity_key, version) DO NOTHING;

CREATE OR REPLACE FUNCTION record_actor_identity_assertion()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  identity_key TEXT;
  previous_id UUID;
  next_version BIGINT;
BEGIN
  identity_key := 'actor:' || NEW.installation_scope || ':' ||
    NEW.source_channel || ':' || NEW.source_actor_ref;
  SELECT id INTO previous_id
    FROM identity_assertions
   WHERE tenant_id = NEW.tenant_id
     AND source_identity_key = identity_key
     AND status = 'accepted'
     AND candidate_entity_ref = jsonb_build_object('type', 'actor', 'id', NEW.actor_id)
     AND confidence = NEW.confidence
   ORDER BY version DESC LIMIT 1;
  IF previous_id IS NOT NULL THEN
    RETURN NEW;
  END IF;
  UPDATE identity_assertions
     SET status = 'superseded', valid_to = now()
   WHERE tenant_id = NEW.tenant_id
     AND source_identity_key = identity_key
     AND status = 'accepted';
  SELECT COALESCE(max(version), 0) + 1 INTO next_version
    FROM identity_assertions
   WHERE tenant_id = NEW.tenant_id AND source_identity_key = identity_key;
  INSERT INTO identity_assertions (
    id, tenant_id, source_identity_key, source_identity_ref,
    candidate_entity_ref, assertion_kind, status, confidence,
    decision_provenance, version, supersedes_assertion_id, decided_at
  ) VALUES (
    gen_random_uuid(), NEW.tenant_id, identity_key,
    jsonb_build_object(
      'kind', 'source_actor',
      'installation_scope', NEW.installation_scope,
      'source_channel', NEW.source_channel,
      'source_actor_ref', NEW.source_actor_ref
    ),
    jsonb_build_object('type', 'actor', 'id', NEW.actor_id),
    'same_as', 'accepted', NEW.confidence,
    jsonb_build_object('producer', 'actor_identity_mapping_projection'),
    next_version,
    (
      SELECT id FROM identity_assertions
       WHERE tenant_id = NEW.tenant_id
         AND source_identity_key = identity_key
         AND status = 'superseded'
       ORDER BY version DESC LIMIT 1
    ),
    now()
  );
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS actor_identity_mapping_assertion_trg
  ON actor_identity_mappings;
CREATE TRIGGER actor_identity_mapping_assertion_trg
AFTER INSERT OR UPDATE OF actor_id, confidence
ON actor_identity_mappings
FOR EACH ROW EXECUTE FUNCTION record_actor_identity_assertion();

CREATE OR REPLACE FUNCTION record_entity_alias_identity_assertion()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  identity_key TEXT;
  next_version BIGINT;
BEGIN
  identity_key := 'alias:' ||
    lower(regexp_replace(NEW.alias_text, '\s+', ' ', 'g')) || ':' ||
    COALESCE(NEW.actor_id::text, 'global');
  IF EXISTS (
    SELECT 1 FROM identity_assertions
     WHERE tenant_id = NEW.tenant_id
       AND source_identity_key = identity_key
       AND candidate_entity_ref = NEW.resolved_entity_ref
       AND assertion_kind = 'same_as'
       AND status = 'accepted'
       AND confidence = NEW.confidence
  ) THEN
    RETURN NEW;
  END IF;
  SELECT COALESCE(max(version), 0) + 1 INTO next_version
    FROM identity_assertions
   WHERE tenant_id = NEW.tenant_id AND source_identity_key = identity_key;
  INSERT INTO identity_assertions (
    id, tenant_id, source_identity_key, source_identity_ref,
    candidate_entity_ref, assertion_kind, status, confidence,
    decision_provenance, valid_from, version, decided_at
  ) VALUES (
    gen_random_uuid(), NEW.tenant_id, identity_key,
    jsonb_build_object(
      'kind', 'text_alias', 'alias_text', NEW.alias_text,
      'actor_scope', NEW.actor_id
    ),
    NEW.resolved_entity_ref, 'same_as', 'accepted', NEW.confidence,
    jsonb_build_object(
      'producer', 'entity_alias_projection',
      'source_event_id', NEW.source_event_id
    ),
    NEW.first_seen_at, next_version, now()
  );
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS entity_alias_identity_assertion_trg ON entity_aliases;
CREATE TRIGGER entity_alias_identity_assertion_trg
AFTER INSERT OR UPDATE OF resolved_entity_ref, confidence
ON entity_aliases
FOR EACH ROW EXECUTE FUNCTION record_entity_alias_identity_assertion();

-- RLS for every new tenant-scoped ledger and the now tenant-scoped mapping.
DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'actor_identity_mappings', 'identity_assertions',
    'identity_cluster_events', 'identity_dependents', 'perception_claims'
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
