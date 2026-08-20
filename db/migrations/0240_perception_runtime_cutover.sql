-- Runtime closure for identity -> knowledge -> episode -> reasoning handoff.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS identity_resolution_snapshots_observation_lineage_uidx
  ON identity_resolution_snapshots (
    tenant_id, id, observation_id, observation_occurred_at
  );

CREATE TABLE IF NOT EXISTS reasoning_ingress_policies (
  tenant_id    UUID PRIMARY KEY REFERENCES tenants(id),
  mode         TEXT NOT NULL CHECK (mode IN ('direct', 'episode')),
  reason       TEXT NOT NULL DEFAULT 'operator_override' CHECK (btrim(reason) <> ''),
  updated_by   TEXT NOT NULL DEFAULT 'system' CHECK (btrim(updated_by) <> ''),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS perception_knowledge_snapshots (
  id                         UUID PRIMARY KEY,
  tenant_id                  UUID NOT NULL REFERENCES tenants(id),
  observation_id             UUID NOT NULL,
  observation_occurred_at    TIMESTAMPTZ NOT NULL,
  evidence_id                UUID NOT NULL,
  identity_snapshot_id       UUID NOT NULL,
  identity_snapshot_hash     TEXT NOT NULL CHECK (identity_snapshot_hash ~ '^[0-9a-f]{64}$'),
  identity_resolution_status TEXT NOT NULL CHECK (identity_resolution_status IN ('complete', 'partial')),
  claim_ids                  UUID[] NOT NULL DEFAULT '{}'::uuid[],
  claim_set_hash             TEXT NOT NULL CHECK (claim_set_hash ~ '^[0-9a-f]{64}$'),
  extractor_name             TEXT NOT NULL CHECK (btrim(extractor_name) <> ''),
  extractor_version          TEXT NOT NULL CHECK (btrim(extractor_version) <> ''),
  manifest                   JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
  snapshot_hash              TEXT NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, snapshot_hash),
  UNIQUE (
    tenant_id, observation_id, identity_snapshot_hash, claim_set_hash,
    extractor_name, extractor_version
  ),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (observation_id, observation_occurred_at)
    REFERENCES observations(id, occurred_at),
  FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id),
  FOREIGN KEY (tenant_id, identity_snapshot_id)
    REFERENCES identity_resolution_snapshots(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS perception_knowledge_snapshots_observation_idx
  ON perception_knowledge_snapshots (tenant_id, observation_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS perception_knowledge_snapshots_lineage_uidx
  ON perception_knowledge_snapshots (
    tenant_id, id, observation_id, observation_occurred_at, evidence_id,
    identity_snapshot_id
  );

CREATE TABLE IF NOT EXISTS perception_knowledge_outbox (
  id                         UUID PRIMARY KEY,
  tenant_id                  UUID NOT NULL REFERENCES tenants(id),
  event_kind                 TEXT NOT NULL CHECK (
    event_kind IN ('identity.ready_for_knowledge', 'claim.changed')
  ),
  observation_id             UUID NOT NULL,
  observation_occurred_at    TIMESTAMPTZ NOT NULL,
  evidence_id                UUID NOT NULL,
  identity_snapshot_id       UUID NOT NULL,
  identity_snapshot_hash     TEXT NOT NULL CHECK (identity_snapshot_hash ~ '^[0-9a-f]{64}$'),
  identity_resolution_status TEXT NOT NULL CHECK (identity_resolution_status IN ('complete', 'partial')),
  trigger_claim_id           UUID,
  reason                     TEXT NOT NULL CHECK (btrim(reason) <> ''),
  contract_version           INTEGER NOT NULL DEFAULT 1 CHECK (contract_version > 0),
  dedupe_key                 TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
  payload                    JSONB NOT NULL DEFAULT '{}'::jsonb
                                  CHECK (jsonb_typeof(payload) = 'object'),
  status                     TEXT NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'leased', 'completed', 'dead_letter')
  ),
  available_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt_count              INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  lease_owner                TEXT,
  lease_expires_at           TIMESTAMPTZ,
  last_error                 TEXT,
  completed_at               TIMESTAMPTZ,
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, dedupe_key),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  FOREIGN KEY (observation_id, observation_occurred_at)
    REFERENCES observations(id, occurred_at),
  FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id),
  FOREIGN KEY (tenant_id, identity_snapshot_id)
    REFERENCES identity_resolution_snapshots(tenant_id, id),
  FOREIGN KEY (
    tenant_id, identity_snapshot_id, observation_id, observation_occurred_at
  ) REFERENCES identity_resolution_snapshots(
    tenant_id, id, observation_id, observation_occurred_at
  ),
  FOREIGN KEY (tenant_id, trigger_claim_id) REFERENCES perception_claims(tenant_id, id)
);

CREATE INDEX IF NOT EXISTS perception_knowledge_outbox_claim_idx
  ON perception_knowledge_outbox (status, available_at, created_at)
  WHERE status IN ('pending', 'leased');
CREATE INDEX IF NOT EXISTS perception_knowledge_outbox_observation_idx
  ON perception_knowledge_outbox (tenant_id, observation_id, created_at DESC);

CREATE OR REPLACE FUNCTION reject_perception_knowledge_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'perception knowledge snapshots are immutable';
END $$;

DROP TRIGGER IF EXISTS perception_knowledge_snapshots_immutable_trg
  ON perception_knowledge_snapshots;
CREATE TRIGGER perception_knowledge_snapshots_immutable_trg
BEFORE UPDATE OR DELETE ON perception_knowledge_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_perception_knowledge_snapshot_mutation();

ALTER TABLE perception_outbox
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_id UUID,
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_hash TEXT,
  ADD COLUMN IF NOT EXISTS claim_set_hash TEXT;

ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_knowledge_snapshot_hash_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_knowledge_snapshot_hash_check CHECK (
    knowledge_snapshot_hash IS NULL OR knowledge_snapshot_hash ~ '^[0-9a-f]{64}$'
  );
ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_claim_set_hash_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_claim_set_hash_check CHECK (
    claim_set_hash IS NULL OR claim_set_hash ~ '^[0-9a-f]{64}$'
  );
ALTER TABLE perception_outbox
  DROP CONSTRAINT IF EXISTS perception_outbox_v3_knowledge_required_check;
ALTER TABLE perception_outbox
  ADD CONSTRAINT perception_outbox_v3_knowledge_required_check CHECK (
    contract_version < 3 OR (
      knowledge_snapshot_id IS NOT NULL
      AND knowledge_snapshot_hash IS NOT NULL
      AND claim_set_hash IS NOT NULL
    )
  );

DROP INDEX IF EXISTS perception_outbox_identity_delivery_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS perception_outbox_knowledge_delivery_uidx
  ON perception_outbox (
    tenant_id, observation_id, identity_snapshot_hash, knowledge_snapshot_hash
  )
  WHERE identity_snapshot_hash IS NOT NULL AND knowledge_snapshot_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS perception_outbox_knowledge_snapshot_idx
  ON perception_outbox (tenant_id, knowledge_snapshot_id)
  WHERE knowledge_snapshot_id IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'perception_outbox'::regclass
       AND conname = 'perception_outbox_tenant_knowledge_snapshot_fkey'
  ) THEN
    ALTER TABLE perception_outbox
      ADD CONSTRAINT perception_outbox_tenant_knowledge_snapshot_fkey
      FOREIGN KEY (tenant_id, knowledge_snapshot_id)
      REFERENCES perception_knowledge_snapshots(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'perception_outbox'::regclass
       AND conname = 'perception_outbox_knowledge_lineage_fkey'
  ) THEN
    ALTER TABLE perception_outbox
      ADD CONSTRAINT perception_outbox_knowledge_lineage_fkey
      FOREIGN KEY (
        tenant_id, knowledge_snapshot_id, observation_id,
        observation_occurred_at, evidence_id, identity_snapshot_id
      ) REFERENCES perception_knowledge_snapshots(
        tenant_id, id, observation_id, observation_occurred_at,
        evidence_id, identity_snapshot_id
      );
  END IF;
END $$;

ALTER TABLE episode_router_runs
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_id UUID,
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_hash TEXT,
  ADD COLUMN IF NOT EXISTS claim_set_hash TEXT;
ALTER TABLE episode_membership_assertions
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_id UUID,
  ADD COLUMN IF NOT EXISTS knowledge_snapshot_hash TEXT,
  ADD COLUMN IF NOT EXISTS claim_set_hash TEXT;

ALTER TABLE episode_router_runs
  DROP CONSTRAINT IF EXISTS episode_router_runs_knowledge_lineage_check;
ALTER TABLE episode_router_runs
  ADD CONSTRAINT episode_router_runs_knowledge_lineage_check CHECK (
    (knowledge_snapshot_id IS NULL AND knowledge_snapshot_hash IS NULL AND claim_set_hash IS NULL)
    OR (
      knowledge_snapshot_id IS NOT NULL
      AND knowledge_snapshot_hash ~ '^[0-9a-f]{64}$'
      AND claim_set_hash ~ '^[0-9a-f]{64}$'
    )
  );
ALTER TABLE episode_membership_assertions
  DROP CONSTRAINT IF EXISTS episode_memberships_knowledge_lineage_check;
ALTER TABLE episode_membership_assertions
  ADD CONSTRAINT episode_memberships_knowledge_lineage_check CHECK (
    (knowledge_snapshot_id IS NULL AND knowledge_snapshot_hash IS NULL AND claim_set_hash IS NULL)
    OR (
      knowledge_snapshot_id IS NOT NULL
      AND knowledge_snapshot_hash ~ '^[0-9a-f]{64}$'
      AND claim_set_hash ~ '^[0-9a-f]{64}$'
    )
  );

CREATE INDEX IF NOT EXISTS episode_router_runs_knowledge_snapshot_idx
  ON episode_router_runs (tenant_id, knowledge_snapshot_id)
  WHERE knowledge_snapshot_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS episode_memberships_knowledge_snapshot_idx
  ON episode_membership_assertions (tenant_id, knowledge_snapshot_id)
  WHERE knowledge_snapshot_id IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'episode_router_runs'::regclass
       AND conname = 'episode_router_runs_tenant_knowledge_snapshot_fkey'
  ) THEN
    ALTER TABLE episode_router_runs
      ADD CONSTRAINT episode_router_runs_tenant_knowledge_snapshot_fkey
      FOREIGN KEY (tenant_id, knowledge_snapshot_id)
      REFERENCES perception_knowledge_snapshots(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'episode_router_runs'::regclass
       AND conname = 'episode_router_runs_knowledge_lineage_fkey'
  ) THEN
    ALTER TABLE episode_router_runs
      ADD CONSTRAINT episode_router_runs_knowledge_lineage_fkey
      FOREIGN KEY (
        tenant_id, knowledge_snapshot_id, observation_id,
        observation_occurred_at, evidence_id, identity_snapshot_id
      ) REFERENCES perception_knowledge_snapshots(
        tenant_id, id, observation_id, observation_occurred_at,
        evidence_id, identity_snapshot_id
      );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'episode_membership_assertions'::regclass
       AND conname = 'episode_memberships_tenant_knowledge_snapshot_fkey'
  ) THEN
    ALTER TABLE episode_membership_assertions
      ADD CONSTRAINT episode_memberships_tenant_knowledge_snapshot_fkey
      FOREIGN KEY (tenant_id, knowledge_snapshot_id)
      REFERENCES perception_knowledge_snapshots(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'episode_membership_assertions'::regclass
       AND conname = 'episode_memberships_knowledge_lineage_fkey'
  ) THEN
    ALTER TABLE episode_membership_assertions
      ADD CONSTRAINT episode_memberships_knowledge_lineage_fkey
      FOREIGN KEY (
        tenant_id, knowledge_snapshot_id, observation_id,
        observation_occurred_at, evidence_id, identity_snapshot_id
      ) REFERENCES perception_knowledge_snapshots(
        tenant_id, id, observation_id, observation_occurred_at,
        evidence_id, identity_snapshot_id
      );
  END IF;
END $$;

CREATE OR REPLACE FUNCTION enqueue_knowledge_reprocessing_for_claim()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  snapshot identity_resolution_snapshots%ROWTYPE;
  observation observations%ROWTYPE;
  key TEXT;
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.status IS NOT DISTINCT FROM OLD.status THEN
    RETURN NEW;
  END IF;
  SELECT * INTO observation
    FROM observations
   WHERE tenant_id = NEW.tenant_id AND id = NEW.observation_id
   ORDER BY occurred_at DESC LIMIT 1;
  IF NOT FOUND THEN
    RETURN NEW;
  END IF;
  SELECT * INTO snapshot
    FROM identity_resolution_snapshots
   WHERE tenant_id = NEW.tenant_id AND observation_id = NEW.observation_id
   ORDER BY created_at DESC, id DESC LIMIT 1;
  IF NOT FOUND THEN
    RETURN NEW;
  END IF;
  key := NEW.tenant_id::text || ':claim:' || NEW.id::text || ':' ||
         NEW.status || ':identity:' || snapshot.snapshot_hash || ':knowledge-v1';
  INSERT INTO perception_knowledge_outbox (
    id, tenant_id, event_kind, observation_id, observation_occurred_at,
    evidence_id, identity_snapshot_id, identity_snapshot_hash,
    identity_resolution_status, trigger_claim_id, reason, dedupe_key, payload
  ) VALUES (
    gen_random_uuid(), NEW.tenant_id, 'claim.changed', NEW.observation_id,
    observation.occurred_at, NEW.evidence_id, snapshot.id,
    snapshot.snapshot_hash, snapshot.resolution_status, NEW.id,
    'claim_' || NEW.status, key,
    jsonb_build_object('claim_id', NEW.id, 'claim_status', NEW.status)
  ) ON CONFLICT (tenant_id, dedupe_key) DO NOTHING;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS perception_claims_knowledge_reprocessing_trg
  ON perception_claims;
CREATE TRIGGER perception_claims_knowledge_reprocessing_trg
AFTER INSERT OR UPDATE OF status ON perception_claims
FOR EACH ROW EXECUTE FUNCTION enqueue_knowledge_reprocessing_for_claim();

-- Convert already-resolved but unconsumed v2 work into knowledge-barrier work.
INSERT INTO perception_knowledge_outbox (
  id, tenant_id, event_kind, observation_id, observation_occurred_at,
  evidence_id, identity_snapshot_id, identity_snapshot_hash,
  identity_resolution_status, reason, dedupe_key, payload
)
SELECT gen_random_uuid(), item.tenant_id, 'identity.ready_for_knowledge',
       item.observation_id, item.observation_occurred_at, item.evidence_id,
       item.identity_snapshot_id, item.identity_snapshot_hash,
       item.identity_resolution_status, 'runtime_cutover_backfill',
       item.tenant_id::text || ':observation:' || item.observation_id::text ||
         ':identity:' || item.identity_snapshot_hash || ':knowledge-v1',
       jsonb_build_object('legacy_perception_outbox_id', item.id)
  FROM perception_outbox item
 WHERE item.contract_version = 2
   AND item.identity_snapshot_id IS NOT NULL
   AND item.status IN ('pending', 'leased')
ON CONFLICT (tenant_id, dedupe_key) DO NOTHING;

UPDATE perception_outbox
   SET status = 'completed', completed_at = now(), lease_owner = NULL,
       lease_expires_at = NULL, last_error = 'superseded_by_knowledge_barrier_v3',
       updated_at = now()
 WHERE contract_version < 3 AND status IN ('pending', 'leased');

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'reasoning_ingress_policies', 'perception_knowledge_snapshots',
    'perception_knowledge_outbox'
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
