-- Durable settled-snapshot handoff to reasoning.

BEGIN;

CREATE TABLE IF NOT EXISTS episode_snapshot_outbox (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  event_kind               TEXT NOT NULL DEFAULT 'episode.snapshot_settled'
                                  CHECK (event_kind='episode.snapshot_settled'),
  topic_id                 UUID NOT NULL,
  episode_id               UUID NOT NULL,
  episode_snapshot_id      UUID NOT NULL,
  episode_snapshot_hash    TEXT NOT NULL CHECK (episode_snapshot_hash ~ '^[0-9a-f]{64}$'),
  mode                     TEXT NOT NULL CHECK (mode IN ('automatic_update','query_answer')),
  requester_actor_id       UUID REFERENCES actors(id),
  query_text               TEXT,
  contract_version         INTEGER NOT NULL DEFAULT 1 CHECK (contract_version > 0),
  dedupe_key               TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
  payload                  JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
  status                   TEXT NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending','leased','completed','dead_letter')
  ),
  available_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt_count            INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  lease_owner              TEXT,
  lease_expires_at         TIMESTAMPTZ,
  last_error               TEXT,
  completed_at             TIMESTAMPTZ,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (status='leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status<>'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (mode<>'query_answer' OR (requester_actor_id IS NOT NULL AND query_text IS NOT NULL)),
  UNIQUE (tenant_id,dedupe_key),
  UNIQUE (tenant_id,episode_snapshot_id,contract_version),
  FOREIGN KEY (tenant_id,topic_id) REFERENCES episode_topics(tenant_id,id),
  FOREIGN KEY (tenant_id,episode_id) REFERENCES episodes(tenant_id,id),
  FOREIGN KEY (tenant_id,episode_snapshot_id)
    REFERENCES episode_snapshots(tenant_id,id)
);

CREATE INDEX IF NOT EXISTS episode_snapshot_outbox_claim_idx
  ON episode_snapshot_outbox (status,available_at,created_at)
  WHERE status IN ('pending','leased');

ALTER TABLE episode_snapshot_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE episode_snapshot_outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON episode_snapshot_outbox;
CREATE POLICY tenant_isolation ON episode_snapshot_outbox
USING (
  NULLIF(current_setting('app.current_tenant',true),'') IS NULL
  OR tenant_id=NULLIF(current_setting('app.current_tenant',true),'')::uuid
) WITH CHECK (
  NULLIF(current_setting('app.current_tenant',true),'') IS NULL
  OR tenant_id=NULLIF(current_setting('app.current_tenant',true),'')::uuid
);

COMMIT;
