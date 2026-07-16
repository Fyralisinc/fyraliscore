-- =====================================================================
-- 0222_canonical_referent_transitions.sql
--
-- Add an append-only, tenant-scoped lineage ledger for canonical
-- referents. Transitions relate immutable physical referent IDs; they do
-- not rewrite, merge, renumber, or retire the underlying entity rows.
--
-- The first supported command is replacement. The storage vocabulary
-- reserves merge, split, resurrection, and retirement operations for
-- later typed protocols without granting runtime writer authority.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS canonical_referent_transitions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  operation_ref TEXT NOT NULL CHECK (
    length(btrim(operation_ref)) > 0
  ),
  request_fingerprint TEXT NOT NULL CHECK (
    request_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  transition_kind TEXT NOT NULL CHECK (
    transition_kind IN (
      'replacement',
      'merge',
      'split',
      'resurrection',
      'retire'
    )
  ),
  effective_at TIMESTAMPTZ NOT NULL,
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expected_predecessor_version INTEGER NOT NULL CHECK (
    expected_predecessor_version > 0
  ),
  authority_ref TEXT NOT NULL CHECK (
    length(btrim(authority_ref)) > 0
  ),
  reason TEXT NOT NULL CHECK (
    length(btrim(reason)) > 0
  ),
  evidence_refs TEXT[] NOT NULL CHECK (
    cardinality(evidence_refs) > 0
  ),
  cause_event_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, operation_ref)
);

CREATE TABLE IF NOT EXISTS canonical_referent_transition_members (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  transition_id UUID NOT NULL,
  member_role TEXT NOT NULL CHECK (
    member_role IN ('predecessor', 'successor')
  ),
  member_ordinal INTEGER NOT NULL CHECK (
    member_ordinal >= 0
  ),
  canonical_ref JSONB NOT NULL CHECK (
    jsonb_typeof(canonical_ref) = 'object'
    AND canonical_ref ?& ARRAY['type', 'id', 'version']
    AND jsonb_typeof(canonical_ref -> 'type') = 'string'
    AND length(btrim(canonical_ref ->> 'type')) > 0
    AND jsonb_typeof(canonical_ref -> 'id') = 'string'
    AND length(btrim(canonical_ref ->> 'id')) > 0
    AND jsonb_typeof(canonical_ref -> 'version') = 'number'
    AND canonical_ref ->> 'version' ~ '^[1-9][0-9]*$'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    tenant_id,
    transition_id,
    member_role,
    member_ordinal
  ),
  FOREIGN KEY (tenant_id, transition_id)
    REFERENCES canonical_referent_transitions (tenant_id, id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS canonical_referent_transitions_effective_idx
  ON canonical_referent_transitions (
    tenant_id,
    effective_at,
    transaction_at
  );

CREATE INDEX IF NOT EXISTS canonical_referent_transitions_kind_idx
  ON canonical_referent_transitions (
    tenant_id,
    transition_kind,
    transaction_at
  );

CREATE INDEX IF NOT EXISTS canonical_referent_transitions_fingerprint_idx
  ON canonical_referent_transitions (
    tenant_id,
    request_fingerprint
  );

CREATE INDEX IF NOT EXISTS canonical_referent_transition_member_lookup_idx
  ON canonical_referent_transition_members (
    tenant_id,
    (canonical_ref ->> 'type'),
    (canonical_ref ->> 'id'),
    ((canonical_ref ->> 'version')::integer),
    member_role
  );

CREATE UNIQUE INDEX IF NOT EXISTS
  canonical_referent_transition_member_unique_ref_idx
  ON canonical_referent_transition_members (
    tenant_id,
    transition_id,
    member_role,
    (canonical_ref ->> 'type'),
    (canonical_ref ->> 'id'),
    ((canonical_ref ->> 'version')::integer)
  );

ALTER TABLE canonical_referent_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE canonical_referent_transitions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON canonical_referent_transitions;
CREATE POLICY tenant_isolation ON canonical_referent_transitions
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

ALTER TABLE canonical_referent_transition_members
  ENABLE ROW LEVEL SECURITY;
ALTER TABLE canonical_referent_transition_members
  FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation
  ON canonical_referent_transition_members;
CREATE POLICY tenant_isolation ON canonical_referent_transition_members
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id =
      NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

DO $$
DECLARE
  table_name TEXT;
  trigger_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'canonical_referent_transitions',
    'canonical_referent_transition_members'
  ]
  LOOP
    trigger_name := 'reject_' || table_name || '_mutation';
    IF NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgrelid = table_name::regclass
        AND tgname = trigger_name
        AND NOT tgisinternal
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION '
        'reject_consequential_immutable_mutation()',
        trigger_name,
        table_name
      );
    END IF;
  END LOOP;
END
$$;

COMMENT ON TABLE canonical_referent_transitions IS
  'Append-only lineage transitions over immutable, tenant-scoped referents.';
COMMENT ON TABLE canonical_referent_transition_members IS
  'Ordered predecessor and successor refs participating in a transition.';
COMMENT ON COLUMN canonical_referent_transitions.operation_ref IS
  'Tenant-scoped idempotency key; replays must use the original fingerprint.';
COMMENT ON COLUMN canonical_referent_transitions.cause_event_id IS
  'Optional evidence event reference; no FK because observations are partitioned.';

COMMIT;
