-- Durable observation-to-identity orchestration and identity-aware episode handoff.

BEGIN;

CREATE TABLE IF NOT EXISTS identity_resolution_outbox (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  event_kind               TEXT NOT NULL CHECK (
    event_kind IN ('observation.ready_for_identity', 'identity.reresolution_requested')
  ),
  observation_id           UUID NOT NULL,
  observation_occurred_at  TIMESTAMPTZ NOT NULL,
  evidence_id              UUID NOT NULL REFERENCES source_evidence(id),
  contract_version         INTEGER NOT NULL DEFAULT 1 CHECK (contract_version > 0),
  dedupe_key               TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
  reason                   TEXT NOT NULL CHECK (btrim(reason) <> ''),
  payload                  JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  status                   TEXT NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'leased', 'completed', 'dead_letter')
  ),
  available_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt_count            INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  lease_owner              TEXT,
  lease_expires_at         TIMESTAMPTZ,
  last_error               TEXT,
  completed_at             TIMESTAMPTZ,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, dedupe_key),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS identity_resolution_outbox_claim_idx
  ON identity_resolution_outbox (status, available_at, created_at)
  WHERE status IN ('pending', 'leased');
CREATE INDEX IF NOT EXISTS identity_resolution_outbox_observation_idx
  ON identity_resolution_outbox (tenant_id, observation_id, created_at DESC);

CREATE TABLE IF NOT EXISTS identity_change_events (
  id              UUID PRIMARY KEY,
  tenant_id       UUID NOT NULL REFERENCES tenants(id),
  event_kind      TEXT NOT NULL CHECK (
    event_kind IN (
      'identity.snapshot_created', 'identity.reresolution_requested',
      'identity.decision_recorded'
    )
  ),
  aggregate_ref   JSONB NOT NULL CHECK (jsonb_typeof(aggregate_ref) = 'object'),
  evidence_id     UUID REFERENCES source_evidence(id),
  payload         JSONB NOT NULL DEFAULT '{}'::jsonb
                        CHECK (jsonb_typeof(payload) = 'object'),
  dedupe_key      TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS identity_change_events_aggregate_idx
  ON identity_change_events USING gin (aggregate_ref);

ALTER TABLE perception_outbox
  ADD COLUMN IF NOT EXISTS identity_snapshot_id UUID,
  ADD COLUMN IF NOT EXISTS identity_snapshot_hash TEXT,
  ADD COLUMN IF NOT EXISTS identity_resolution_status TEXT;

-- A corrected identity snapshot must produce a new episode-construction input.
-- The original v1 uniqueness rule admitted only one event per observation and
-- contract version; dedupe_key is now the idempotency boundary and includes the
-- immutable snapshot hash.
DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  SELECT con.conname INTO constraint_name
    FROM pg_constraint con
   WHERE con.conrelid = 'perception_outbox'::regclass
     AND con.contype = 'u'
     AND (
       SELECT array_agg(att.attname::text ORDER BY key.ordinality)
         FROM unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality)
         JOIN pg_attribute att
           ON att.attrelid = con.conrelid AND att.attnum = key.attnum
     ) = ARRAY['tenant_id', 'event_kind', 'aggregate_id', 'contract_version'];
  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE perception_outbox DROP CONSTRAINT %I', constraint_name);
  END IF;
END $$;

ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_identity_snapshot_hash_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_identity_snapshot_hash_check CHECK (
    identity_snapshot_hash IS NULL OR identity_snapshot_hash ~ '^[0-9a-f]{64}$'
  );
ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_identity_status_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_identity_status_check CHECK (
    identity_resolution_status IS NULL
    OR identity_resolution_status IN ('complete', 'partial')
  );
ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_v2_identity_required_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_v2_identity_required_check CHECK (
    contract_version < 2
    OR (
      identity_snapshot_id IS NOT NULL
      AND identity_snapshot_hash IS NOT NULL
      AND identity_resolution_status IS NOT NULL
    )
  );

CREATE INDEX IF NOT EXISTS perception_outbox_identity_snapshot_idx
  ON perception_outbox (tenant_id, identity_snapshot_id)
  WHERE identity_snapshot_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS perception_outbox_identity_delivery_uidx
  ON perception_outbox (tenant_id, observation_id, identity_snapshot_hash)
  WHERE identity_snapshot_hash IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_outbox'::regclass
       AND conname = 'identity_resolution_outbox_observation_fkey'
  ) THEN
    ALTER TABLE identity_resolution_outbox
      ADD CONSTRAINT identity_resolution_outbox_observation_fkey
      FOREIGN KEY (observation_id, observation_occurred_at)
      REFERENCES observations(id, occurred_at);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_outbox'::regclass
       AND conname = 'identity_resolution_outbox_tenant_evidence_fkey'
  ) THEN
    ALTER TABLE identity_resolution_outbox
      ADD CONSTRAINT identity_resolution_outbox_tenant_evidence_fkey
      FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_change_events'::regclass
       AND conname = 'identity_change_events_tenant_evidence_fkey'
  ) THEN
    ALTER TABLE identity_change_events
      ADD CONSTRAINT identity_change_events_tenant_evidence_fkey
      FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'perception_outbox'::regclass
       AND conname = 'perception_outbox_tenant_identity_snapshot_fkey'
  ) THEN
    ALTER TABLE perception_outbox
      ADD CONSTRAINT perception_outbox_tenant_identity_snapshot_fkey
      FOREIGN KEY (tenant_id, identity_snapshot_id)
      REFERENCES identity_resolution_snapshots(tenant_id, id);
  END IF;
END $$;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'identity_resolution_outbox', 'identity_change_events'
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
