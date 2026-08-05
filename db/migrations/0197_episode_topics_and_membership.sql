-- Durable topic intents, episode identities, router runs, and memberships.

BEGIN;

CREATE TABLE IF NOT EXISTS episode_topics (
  id                   UUID PRIMARY KEY,
  tenant_id            UUID NOT NULL REFERENCES tenants(id),
  topic_key            TEXT NOT NULL CHECK (topic_key ~ '^[0-9a-f]{64}$'),
  origin               TEXT NOT NULL CHECK (
    origin IN ('automatic', 'query_seeded', 'human_pinned')
  ),
  label                TEXT NOT NULL CHECK (btrim(label) <> ''),
  query_text           TEXT,
  requester_actor_id   UUID REFERENCES actors(id),
  primary_anchor       JSONB NOT NULL CHECK (jsonb_typeof(primary_anchor) = 'object'),
  anchor_refs          JSONB NOT NULL DEFAULT '[]'::jsonb
                              CHECK (jsonb_typeof(anchor_refs) = 'array'),
  claim_predicates     JSONB NOT NULL DEFAULT '[]'::jsonb
                              CHECK (jsonb_typeof(claim_predicates) = 'array'),
  lexical_terms        JSONB NOT NULL DEFAULT '[]'::jsonb
                              CHECK (jsonb_typeof(lexical_terms) = 'array'),
  valid_time_start     TIMESTAMPTZ,
  valid_time_end       TIMESTAMPTZ,
  router_name          TEXT NOT NULL CHECK (btrim(router_name) <> ''),
  router_version       TEXT NOT NULL CHECK (btrim(router_version) <> ''),
  head_version         INTEGER NOT NULL DEFAULT 1 CHECK (head_version > 0),
  status               TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'superseded', 'archived')
  ),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_time_end IS NULL OR valid_time_start IS NULL
         OR valid_time_end >= valid_time_start),
  CHECK (origin <> 'query_seeded'
         OR (query_text IS NOT NULL AND requester_actor_id IS NOT NULL)),
  UNIQUE (tenant_id, topic_key),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episode_topics_anchor_idx
  ON episode_topics USING gin (anchor_refs);
CREATE INDEX IF NOT EXISTS episode_topics_active_idx
  ON episode_topics (tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS episode_topic_versions (
  id                 UUID PRIMARY KEY,
  tenant_id          UUID NOT NULL REFERENCES tenants(id),
  topic_id           UUID NOT NULL,
  version            INTEGER NOT NULL CHECK (version > 0),
  primary_anchor     JSONB NOT NULL CHECK (jsonb_typeof(primary_anchor) = 'object'),
  anchor_refs        JSONB NOT NULL CHECK (jsonb_typeof(anchor_refs) = 'array'),
  claim_predicates   JSONB NOT NULL CHECK (jsonb_typeof(claim_predicates) = 'array'),
  lexical_terms      JSONB NOT NULL CHECK (jsonb_typeof(lexical_terms) = 'array'),
  caused_by_membership_id UUID,
  manifest_hash      TEXT NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, topic_id, version),
  UNIQUE (tenant_id, topic_id, manifest_hash),
  FOREIGN KEY (tenant_id, topic_id) REFERENCES episode_topics(tenant_id, id)
);

CREATE TABLE IF NOT EXISTS episodes (
  id                 UUID PRIMARY KEY,
  tenant_id          UUID NOT NULL REFERENCES tenants(id),
  topic_id           UUID NOT NULL,
  lifecycle_state    TEXT NOT NULL DEFAULT 'open' CHECK (
    lifecycle_state IN ('open', 'dormant', 'settled', 'reopened', 'superseded')
  ),
  head_version       INTEGER NOT NULL DEFAULT 0 CHECK (head_version >= 0),
  opened_at          TIMESTAMPTZ NOT NULL,
  last_event_at      TIMESTAMPTZ NOT NULL,
  last_ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, topic_id),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, topic_id) REFERENCES episode_topics(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episodes_routing_idx
  ON episodes (tenant_id, lifecycle_state, last_event_at DESC);

CREATE TABLE IF NOT EXISTS episode_topic_equivalences (
  id                 UUID PRIMARY KEY,
  tenant_id          UUID NOT NULL REFERENCES tenants(id),
  left_topic_id      UUID NOT NULL,
  right_topic_id     UUID NOT NULL,
  decision           TEXT NOT NULL CHECK (decision IN ('equivalent', 'distinct')),
  authority          TEXT NOT NULL CHECK (authority IN ('system', 'human')),
  evidence_ids       UUID[] NOT NULL DEFAULT '{}'::uuid[],
  provenance         JSONB NOT NULL DEFAULT '{}'::jsonb
                            CHECK (jsonb_typeof(provenance) = 'object'),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (left_topic_id <> right_topic_id),
  UNIQUE (tenant_id, left_topic_id, right_topic_id),
  FOREIGN KEY (tenant_id, left_topic_id) REFERENCES episode_topics(tenant_id, id),
  FOREIGN KEY (tenant_id, right_topic_id) REFERENCES episode_topics(tenant_id, id)
);

CREATE TABLE IF NOT EXISTS episode_router_runs (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  perception_outbox_id     UUID NOT NULL REFERENCES perception_outbox(id),
  observation_id           UUID NOT NULL,
  observation_occurred_at  TIMESTAMPTZ NOT NULL,
  evidence_id              UUID NOT NULL,
  identity_snapshot_id     UUID NOT NULL,
  input_hash               TEXT NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
  router_name              TEXT NOT NULL CHECK (btrim(router_name) <> ''),
  router_version           TEXT NOT NULL CHECK (btrim(router_version) <> ''),
  feature_schema_version   INTEGER NOT NULL CHECK (feature_schema_version > 0),
  status                   TEXT NOT NULL DEFAULT 'running' CHECK (
    status IN ('running', 'completed', 'failed')
  ),
  result_hash              TEXT CHECK (result_hash IS NULL OR result_hash ~ '^[0-9a-f]{64}$'),
  failure                  TEXT,
  started_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at             TIMESTAMPTZ,
  UNIQUE (tenant_id, input_hash, router_name, router_version),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (observation_id, observation_occurred_at)
    REFERENCES observations(id, occurred_at),
  FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id),
  FOREIGN KEY (tenant_id, identity_snapshot_id)
    REFERENCES identity_resolution_snapshots(tenant_id, id)
);

CREATE TABLE IF NOT EXISTS episode_membership_assertions (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  topic_id                 UUID NOT NULL,
  episode_id               UUID NOT NULL,
  router_run_id            UUID NOT NULL,
  observation_id           UUID NOT NULL,
  observation_occurred_at  TIMESTAMPTZ NOT NULL,
  evidence_id              UUID NOT NULL,
  identity_snapshot_id     UUID NOT NULL,
  claim_ids                UUID[] NOT NULL DEFAULT '{}'::uuid[],
  identity_assertion_ids   UUID[] NOT NULL DEFAULT '{}'::uuid[],
  decision                 TEXT NOT NULL CHECK (decision IN ('include', 'exclude', 'hold')),
  score                    DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
  reasons                  JSONB NOT NULL CHECK (
    jsonb_typeof(reasons) = 'array' AND jsonb_array_length(reasons) > 0
  ),
  feature_snapshot         JSONB NOT NULL CHECK (jsonb_typeof(feature_snapshot) = 'object'),
  router_name              TEXT NOT NULL CHECK (btrim(router_name) <> ''),
  router_version           TEXT NOT NULL CHECK (btrim(router_version) <> ''),
  feature_schema_version   INTEGER NOT NULL CHECK (feature_schema_version > 0),
  decision_key             TEXT NOT NULL CHECK (decision_key ~ '^[0-9a-f]{64}$'),
  status                   TEXT NOT NULL DEFAULT 'accepted' CHECK (
    status IN ('proposed', 'accepted', 'rejected', 'superseded')
  ),
  supersedes_assertion_id  UUID,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, decision_key),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, topic_id) REFERENCES episode_topics(tenant_id, id),
  FOREIGN KEY (tenant_id, episode_id) REFERENCES episodes(tenant_id, id),
  FOREIGN KEY (tenant_id, router_run_id) REFERENCES episode_router_runs(tenant_id, id),
  FOREIGN KEY (observation_id, observation_occurred_at)
    REFERENCES observations(id, occurred_at),
  FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id),
  FOREIGN KEY (tenant_id, identity_snapshot_id)
    REFERENCES identity_resolution_snapshots(tenant_id, id),
  FOREIGN KEY (tenant_id, supersedes_assertion_id)
    REFERENCES episode_membership_assertions(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episode_memberships_episode_idx
  ON episode_membership_assertions (tenant_id, episode_id, decision, created_at);
CREATE INDEX IF NOT EXISTS episode_memberships_observation_idx
  ON episode_membership_assertions (tenant_id, observation_id, created_at);

CREATE OR REPLACE FUNCTION reject_episode_membership_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'episode membership assertions are immutable';
END $$;

DROP TRIGGER IF EXISTS episode_memberships_immutable_trg
  ON episode_membership_assertions;
CREATE TRIGGER episode_memberships_immutable_trg
BEFORE UPDATE OR DELETE ON episode_membership_assertions
FOR EACH ROW EXECUTE FUNCTION reject_episode_membership_mutation();

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'episode_topics', 'episode_topic_versions', 'episodes', 'episode_topic_equivalences',
    'episode_router_runs', 'episode_membership_assertions'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', table_name);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', table_name);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING ('
      'NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL OR '
      'tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') WITH CHECK ('
      'NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL OR '
      'tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid)',
      table_name
    );
  END LOOP;
END $$;

COMMIT;
