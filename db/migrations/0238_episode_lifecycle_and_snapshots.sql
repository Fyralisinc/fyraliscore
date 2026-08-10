-- Episode lifecycle history, contradictions, and immutable content-addressed snapshots.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS perception_claims_tenant_id_id_uidx
  ON perception_claims (tenant_id, id);

CREATE TABLE IF NOT EXISTS episode_lifecycle_events (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  episode_id               UUID NOT NULL,
  event_kind               TEXT NOT NULL CHECK (
    event_kind IN ('opened','dormant','settled','reopened','superseded','split','merged')
  ),
  from_state               TEXT,
  to_state                 TEXT NOT NULL CHECK (
    to_state IN ('open','dormant','settled','reopened','superseded')
  ),
  event_time_watermark     TIMESTAMPTZ NOT NULL,
  ingestion_time_watermark TIMESTAMPTZ NOT NULL,
  rule_name                TEXT NOT NULL CHECK (btrim(rule_name) <> ''),
  rule_version             TEXT NOT NULL CHECK (btrim(rule_version) <> ''),
  cause_ref                JSONB NOT NULL DEFAULT '{}'::jsonb
                                CHECK (jsonb_typeof(cause_ref) = 'object'),
  transition_key           TEXT NOT NULL CHECK (transition_key ~ '^[0-9a-f]{64}$'),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, transition_key),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, episode_id) REFERENCES episodes(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episode_lifecycle_history_idx
  ON episode_lifecycle_events (tenant_id, episode_id, created_at, id);

CREATE TABLE IF NOT EXISTS episode_contradictions (
  id                 UUID PRIMARY KEY,
  tenant_id          UUID NOT NULL REFERENCES tenants(id),
  episode_id         UUID NOT NULL,
  left_claim_id      UUID NOT NULL,
  right_claim_id     UUID NOT NULL,
  contradiction_kind TEXT NOT NULL CHECK (
    contradiction_kind IN (
      'opposite_polarity','incompatible_values','competing_temporal_state',
      'identity_ambiguity'
    )
  ),
  status             TEXT NOT NULL DEFAULT 'unresolved' CHECK (
    status IN ('unresolved','contextualized','resolved')
  ),
  explanation        TEXT,
  detector_name      TEXT NOT NULL CHECK (btrim(detector_name) <> ''),
  detector_version   TEXT NOT NULL CHECK (btrim(detector_version) <> ''),
  detection_key      TEXT NOT NULL CHECK (detection_key ~ '^[0-9a-f]{64}$'),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (left_claim_id <> right_claim_id),
  UNIQUE (tenant_id, detection_key),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, episode_id) REFERENCES episodes(tenant_id, id),
  FOREIGN KEY (tenant_id, left_claim_id) REFERENCES perception_claims(tenant_id, id),
  FOREIGN KEY (tenant_id, right_claim_id) REFERENCES perception_claims(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episode_contradictions_episode_idx
  ON episode_contradictions (tenant_id, episode_id, status, created_at);

CREATE TABLE IF NOT EXISTS episode_snapshots (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  topic_id                 UUID NOT NULL,
  episode_id               UUID NOT NULL,
  version                  INTEGER NOT NULL CHECK (version > 0),
  lifecycle_state          TEXT NOT NULL CHECK (
    lifecycle_state IN ('open','dormant','settled','reopened','superseded')
  ),
  prior_snapshot_id        UUID,
  input_hash               TEXT NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
  manifest                 JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
  snapshot_hash            TEXT NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
  access_manifest          JSONB NOT NULL CHECK (jsonb_typeof(access_manifest) = 'object'),
  observation_count        INTEGER NOT NULL CHECK (observation_count > 0),
  evidence_count           INTEGER NOT NULL CHECK (evidence_count > 0),
  claim_count              INTEGER NOT NULL CHECK (claim_count >= 0),
  contradiction_count      INTEGER NOT NULL CHECK (contradiction_count >= 0),
  event_time_watermark     TIMESTAMPTZ NOT NULL,
  ingestion_time_watermark TIMESTAMPTZ NOT NULL,
  settlement               JSONB CHECK (settlement IS NULL OR jsonb_typeof(settlement)='object'),
  created_at               TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, episode_id, version),
  UNIQUE (tenant_id, episode_id, input_hash),
  UNIQUE (tenant_id, snapshot_hash),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, topic_id) REFERENCES episode_topics(tenant_id, id),
  FOREIGN KEY (tenant_id, episode_id) REFERENCES episodes(tenant_id, id),
  FOREIGN KEY (tenant_id, prior_snapshot_id) REFERENCES episode_snapshots(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS episode_snapshots_history_idx
  ON episode_snapshots (tenant_id, episode_id, version DESC);

CREATE TABLE IF NOT EXISTS episode_snapshot_memberships (
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  snapshot_id              UUID NOT NULL,
  membership_assertion_id  UUID NOT NULL,
  observation_id           UUID NOT NULL,
  evidence_id              UUID NOT NULL,
  PRIMARY KEY (tenant_id, snapshot_id, membership_assertion_id),
  FOREIGN KEY (tenant_id, snapshot_id) REFERENCES episode_snapshots(tenant_id, id),
  FOREIGN KEY (tenant_id, membership_assertion_id)
    REFERENCES episode_membership_assertions(tenant_id, id),
  FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id)
);

CREATE OR REPLACE FUNCTION reject_episode_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'episode history is immutable';
END $$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'episode_lifecycle_events','episode_contradictions','episode_snapshots',
    'episode_snapshot_memberships'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS episode_history_immutable_trg ON %I', table_name);
    EXECUTE format(
      'CREATE TRIGGER episode_history_immutable_trg BEFORE UPDATE OR DELETE ON %I '
      'FOR EACH ROW EXECUTE FUNCTION reject_episode_history_mutation()', table_name
    );
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
