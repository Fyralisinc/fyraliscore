-- Durable perception-to-episode boundary. This is constructor input only;
-- topic routing, membership, and episode persistence remain intentionally
-- outside this migration.

BEGIN;

CREATE TABLE IF NOT EXISTS perception_outbox (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  event_kind               TEXT NOT NULL CHECK (
    event_kind IN ('observation.ready_for_episode')
  ),
  aggregate_type           TEXT NOT NULL DEFAULT 'observation' CHECK (
    aggregate_type = 'observation'
  ),
  aggregate_id             UUID NOT NULL,
  observation_id           UUID NOT NULL,
  observation_occurred_at  TIMESTAMPTZ NOT NULL,
  evidence_id              UUID NOT NULL REFERENCES source_evidence(id),
  contract_version         INTEGER NOT NULL DEFAULT 1 CHECK (contract_version > 0),
  dedupe_key               TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
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
  UNIQUE (tenant_id, event_kind, aggregate_id, contract_version),
  UNIQUE (tenant_id, dedupe_key),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (
    (status = 'completed' AND completed_at IS NOT NULL)
    OR (status <> 'completed' AND completed_at IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS perception_outbox_claim_idx
  ON perception_outbox (status, available_at, created_at)
  WHERE status IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS perception_outbox_tenant_observation_idx
  ON perception_outbox (tenant_id, observation_id, created_at);

CREATE INDEX IF NOT EXISTS perception_outbox_evidence_idx
  ON perception_outbox (tenant_id, evidence_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'perception_outbox'::regclass
       AND conname = 'perception_outbox_tenant_evidence_fkey'
  ) THEN
    ALTER TABLE perception_outbox
      ADD CONSTRAINT perception_outbox_tenant_evidence_fkey
      FOREIGN KEY (tenant_id, evidence_id)
      REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'perception_outbox'::regclass
       AND conname = 'perception_outbox_observation_revision_fkey'
  ) THEN
    ALTER TABLE perception_outbox
      ADD CONSTRAINT perception_outbox_observation_revision_fkey
      FOREIGN KEY (observation_id, observation_occurred_at)
      REFERENCES observations(id, occurred_at);
  END IF;
END $$;

-- Existing evidence-linked observations become constructor input. A document
-- still waiting for summarization is deliberately withheld until its worker
-- atomically writes the summary and enqueues the event.
INSERT INTO perception_outbox (
  id, tenant_id, event_kind, aggregate_type, aggregate_id,
  observation_id, observation_occurred_at, evidence_id,
  contract_version, dedupe_key, payload
)
SELECT gen_random_uuid(), observation.tenant_id,
       'observation.ready_for_episode', 'observation', observation.id,
       observation.id, observation.occurred_at, observation.evidence_id,
       1,
       observation.tenant_id::text || ':observation:' || observation.id::text || ':v1',
       jsonb_build_object(
         'observation_id', observation.id,
         'evidence_id', observation.evidence_id,
         'source_channel', observation.source_channel,
         'kind', observation.kind,
         'trust_tier', observation.trust_tier,
         'occurred_at', observation.occurred_at,
         'actor_id', observation.actor_id
       )
  FROM observations AS observation
 WHERE observation.evidence_id IS NOT NULL
   AND (
     observation.content->'summarization' IS NULL
     OR observation.content->'summarization'->>'status' = 'complete'
   )
ON CONFLICT (tenant_id, dedupe_key) DO NOTHING;

ALTER TABLE perception_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE perception_outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON perception_outbox;
CREATE POLICY tenant_isolation ON perception_outbox
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMIT;
