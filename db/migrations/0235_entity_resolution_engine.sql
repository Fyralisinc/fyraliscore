-- Explainable candidates, constraints, typed assertions, and immutable snapshots.

BEGIN;

ALTER TABLE identity_assertions
  DROP CONSTRAINT IF EXISTS identity_assertions_assertion_kind_check;
ALTER TABLE identity_assertions
  ADD CONSTRAINT identity_assertions_assertion_kind_check CHECK (
    assertion_kind IN (
      'same_as', 'not_same_as', 'refers_to', 'represents', 'part_of', 'version_of'
    )
  );

ALTER TABLE identity_assertions
  ADD COLUMN IF NOT EXISTS mention_id UUID,
  ADD COLUMN IF NOT EXISTS resolver_run_id UUID,
  ADD COLUMN IF NOT EXISTS score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS access_policy_hash TEXT;

ALTER TABLE identity_assertions
  DROP CONSTRAINT IF EXISTS identity_assertions_score_components_object_check;
ALTER TABLE identity_assertions
  ADD CONSTRAINT identity_assertions_score_components_object_check
  CHECK (jsonb_typeof(score_components) = 'object');
ALTER TABLE identity_assertions
  DROP CONSTRAINT IF EXISTS identity_assertions_scope_object_check;
ALTER TABLE identity_assertions
  ADD CONSTRAINT identity_assertions_scope_object_check
  CHECK (jsonb_typeof(scope) = 'object');
ALTER TABLE identity_assertions
  DROP CONSTRAINT IF EXISTS identity_assertions_access_policy_hash_check;
ALTER TABLE identity_assertions
  ADD CONSTRAINT identity_assertions_access_policy_hash_check
  CHECK (
    access_policy_hash IS NULL OR access_policy_hash ~ '^[0-9a-f]{64}$'
  );

CREATE INDEX IF NOT EXISTS identity_assertions_mention_idx
  ON identity_assertions (tenant_id, mention_id, status, confidence DESC)
  WHERE mention_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS identity_assertions_run_idx
  ON identity_assertions (tenant_id, resolver_run_id, created_at)
  WHERE resolver_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS identity_constraints (
  id                UUID PRIMARY KEY,
  tenant_id         UUID NOT NULL REFERENCES tenants(id),
  constraint_kind   TEXT NOT NULL CHECK (
    constraint_kind IN ('must_link', 'cannot_link')
  ),
  left_ref          JSONB NOT NULL CHECK (jsonb_typeof(left_ref) = 'object'),
  right_ref         JSONB NOT NULL CHECK (jsonb_typeof(right_ref) = 'object'),
  authority         TEXT NOT NULL CHECK (
    authority IN ('source', 'system', 'human')
  ),
  evidence_id       UUID REFERENCES source_evidence(id),
  provenance        JSONB NOT NULL DEFAULT '{}'::jsonb
                           CHECK (jsonb_typeof(provenance) = 'object'),
  valid_from        TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to          TIMESTAMPTZ,
  status            TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'superseded', 'rejected')
  ),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_to >= valid_from),
  UNIQUE (tenant_id, constraint_kind, left_ref, right_ref, valid_from),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS identity_constraints_left_idx
  ON identity_constraints USING gin (left_ref);
CREATE INDEX IF NOT EXISTS identity_constraints_right_idx
  ON identity_constraints USING gin (right_ref);

CREATE TABLE IF NOT EXISTS identity_resolution_candidates (
  id                 UUID PRIMARY KEY,
  tenant_id          UUID NOT NULL REFERENCES tenants(id),
  resolver_run_id    UUID NOT NULL,
  mention_id         UUID NOT NULL,
  candidate_key      TEXT NOT NULL CHECK (candidate_key ~ '^[0-9a-f]{64}$'),
  candidate_ref      JSONB NOT NULL CHECK (jsonb_typeof(candidate_ref) = 'object'),
  retrieval_methods  JSONB NOT NULL CHECK (jsonb_typeof(retrieval_methods) = 'array'),
  features           JSONB NOT NULL CHECK (jsonb_typeof(features) = 'object'),
  score              DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
  rank               INTEGER NOT NULL CHECK (rank > 0),
  constraint_outcome TEXT NOT NULL CHECK (
    constraint_outcome IN ('allowed', 'must_link', 'cannot_link', 'type_rejected')
  ),
  reasons            JSONB NOT NULL DEFAULT '[]'::jsonb
                           CHECK (jsonb_typeof(reasons) = 'array'),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, resolver_run_id, mention_id, candidate_key),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS identity_resolution_candidates_mention_idx
  ON identity_resolution_candidates (
    tenant_id, resolver_run_id, mention_id, rank
  );
CREATE INDEX IF NOT EXISTS identity_resolution_candidates_ref_idx
  ON identity_resolution_candidates USING gin (candidate_ref);

CREATE TABLE IF NOT EXISTS identity_resolution_snapshots (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  resolver_run_id          UUID NOT NULL,
  input_kind               TEXT NOT NULL CHECK (
    input_kind IN ('observation', 'query', 'reprocess')
  ),
  observation_id           UUID,
  observation_occurred_at  TIMESTAMPTZ,
  requester_actor_id       UUID REFERENCES actors(id),
  resolution_status        TEXT NOT NULL CHECK (
    resolution_status IN ('complete', 'partial')
  ),
  mention_count            INTEGER NOT NULL CHECK (mention_count >= 0),
  resolved_count           INTEGER NOT NULL CHECK (resolved_count >= 0),
  probable_count           INTEGER NOT NULL CHECK (probable_count >= 0),
  ambiguous_count          INTEGER NOT NULL CHECK (ambiguous_count >= 0),
  unresolved_count         INTEGER NOT NULL CHECK (unresolved_count >= 0),
  assertion_ids            UUID[] NOT NULL DEFAULT '{}'::uuid[],
  manifest                 JSONB NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
  snapshot_hash            TEXT NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
  access_policy_hash       TEXT CHECK (
    access_policy_hash IS NULL OR access_policy_hash ~ '^[0-9a-f]{64}$'
  ),
  resolver_name            TEXT NOT NULL CHECK (btrim(resolver_name) <> ''),
  resolver_version         TEXT NOT NULL CHECK (btrim(resolver_version) <> ''),
  policy_version           TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    mention_count = resolved_count + probable_count + ambiguous_count + unresolved_count
  ),
  CHECK (
    input_kind = 'query'
    OR (observation_id IS NOT NULL AND observation_occurred_at IS NOT NULL)
  ),
  UNIQUE (tenant_id, resolver_run_id),
  UNIQUE (tenant_id, snapshot_hash),
  UNIQUE (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS identity_resolution_snapshots_observation_idx
  ON identity_resolution_snapshots (
    tenant_id, observation_id, created_at DESC
  );

CREATE TABLE IF NOT EXISTS identity_resolution_snapshot_items (
  tenant_id         UUID NOT NULL REFERENCES tenants(id),
  snapshot_id       UUID NOT NULL,
  mention_id        UUID NOT NULL,
  outcome           TEXT NOT NULL CHECK (
    outcome IN ('resolved', 'probable', 'ambiguous', 'unresolved')
  ),
  selected_ref      JSONB CHECK (
    selected_ref IS NULL OR jsonb_typeof(selected_ref) = 'object'
  ),
  confidence        DOUBLE PRECISION NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  assertion_id      UUID,
  alternatives      JSONB NOT NULL DEFAULT '[]'::jsonb
                           CHECK (jsonb_typeof(alternatives) = 'array'),
  reasons           JSONB NOT NULL DEFAULT '[]'::jsonb
                           CHECK (jsonb_typeof(reasons) = 'array'),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, snapshot_id, mention_id)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_assertions'::regclass
       AND conname = 'identity_assertions_tenant_mention_fkey'
  ) THEN
    ALTER TABLE identity_assertions
      ADD CONSTRAINT identity_assertions_tenant_mention_fkey
      FOREIGN KEY (tenant_id, mention_id) REFERENCES entity_mentions(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_assertions'::regclass
       AND conname = 'identity_assertions_tenant_run_fkey'
  ) THEN
    ALTER TABLE identity_assertions
      ADD CONSTRAINT identity_assertions_tenant_run_fkey
      FOREIGN KEY (tenant_id, resolver_run_id)
      REFERENCES identity_resolution_runs(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_constraints'::regclass
       AND conname = 'identity_constraints_tenant_evidence_fkey'
  ) THEN
    ALTER TABLE identity_constraints
      ADD CONSTRAINT identity_constraints_tenant_evidence_fkey
      FOREIGN KEY (tenant_id, evidence_id) REFERENCES source_evidence(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_candidates'::regclass
       AND conname = 'identity_resolution_candidates_tenant_run_fkey'
  ) THEN
    ALTER TABLE identity_resolution_candidates
      ADD CONSTRAINT identity_resolution_candidates_tenant_run_fkey
      FOREIGN KEY (tenant_id, resolver_run_id)
      REFERENCES identity_resolution_runs(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_candidates'::regclass
       AND conname = 'identity_resolution_candidates_tenant_mention_fkey'
  ) THEN
    ALTER TABLE identity_resolution_candidates
      ADD CONSTRAINT identity_resolution_candidates_tenant_mention_fkey
      FOREIGN KEY (tenant_id, mention_id) REFERENCES entity_mentions(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_snapshots'::regclass
       AND conname = 'identity_resolution_snapshots_tenant_run_fkey'
  ) THEN
    ALTER TABLE identity_resolution_snapshots
      ADD CONSTRAINT identity_resolution_snapshots_tenant_run_fkey
      FOREIGN KEY (tenant_id, resolver_run_id)
      REFERENCES identity_resolution_runs(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_snapshots'::regclass
       AND conname = 'identity_resolution_snapshots_observation_fkey'
  ) THEN
    ALTER TABLE identity_resolution_snapshots
      ADD CONSTRAINT identity_resolution_snapshots_observation_fkey
      FOREIGN KEY (observation_id, observation_occurred_at)
      REFERENCES observations(id, occurred_at);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_snapshot_items'::regclass
       AND conname = 'identity_resolution_snapshot_items_snapshot_fkey'
  ) THEN
    ALTER TABLE identity_resolution_snapshot_items
      ADD CONSTRAINT identity_resolution_snapshot_items_snapshot_fkey
      FOREIGN KEY (tenant_id, snapshot_id)
      REFERENCES identity_resolution_snapshots(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_snapshot_items'::regclass
       AND conname = 'identity_resolution_snapshot_items_mention_fkey'
  ) THEN
    ALTER TABLE identity_resolution_snapshot_items
      ADD CONSTRAINT identity_resolution_snapshot_items_mention_fkey
      FOREIGN KEY (tenant_id, mention_id) REFERENCES entity_mentions(tenant_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'identity_resolution_snapshot_items'::regclass
       AND conname = 'identity_resolution_snapshot_items_assertion_fkey'
  ) THEN
    ALTER TABLE identity_resolution_snapshot_items
      ADD CONSTRAINT identity_resolution_snapshot_items_assertion_fkey
      FOREIGN KEY (tenant_id, assertion_id)
      REFERENCES identity_assertions(tenant_id, id);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION reject_identity_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'identity resolution snapshots are immutable';
END $$;

DROP TRIGGER IF EXISTS identity_resolution_snapshots_immutable_trg
  ON identity_resolution_snapshots;
CREATE TRIGGER identity_resolution_snapshots_immutable_trg
BEFORE UPDATE OR DELETE ON identity_resolution_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_identity_snapshot_mutation();

DROP TRIGGER IF EXISTS identity_resolution_snapshot_items_immutable_trg
  ON identity_resolution_snapshot_items;
CREATE TRIGGER identity_resolution_snapshot_items_immutable_trg
BEFORE UPDATE OR DELETE ON identity_resolution_snapshot_items
FOR EACH ROW EXECUTE FUNCTION reject_identity_snapshot_mutation();

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'identity_constraints', 'identity_resolution_candidates',
    'identity_resolution_snapshots', 'identity_resolution_snapshot_items'
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
