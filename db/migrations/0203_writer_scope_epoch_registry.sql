-- =====================================================================
-- 0203_writer_scope_epoch_registry.sql
--
-- Canonical single-writer ownership and cutover registry.  Exact partition
-- claims make overlap impossible; immutable versions and typed proofs retain
-- the full split/merge/transfer/retirement history.
-- =====================================================================

BEGIN;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'agency_command_results'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%writer_id%'
  LOOP
    EXECUTE format(
      'ALTER TABLE agency_command_results DROP CONSTRAINT %I', constraint_name
    );
  END LOOP;
  ALTER TABLE agency_command_results
    ADD CONSTRAINT agency_command_results_writer_id_check
    CHECK (writer_id IN (
      'ProposalAppender', 'EpisodeCoordinator', 'PredictionWriter',
      'AuthorizationApplier', 'OutcomeRecorder', 'SettlementApplier',
      'AttributionApplier', 'PolicyRegistryApplier', 'AgencyStateApplier',
      'WorkLedgerApplier', 'ExecutionLedgerApplier', 'RepairLedgerApplier',
      'WriterEpochApplier'
    ));
END $$;

CREATE TABLE IF NOT EXISTS writer_scope_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  scope_id UUID NOT NULL,
  semantic_responsibility TEXT NOT NULL,
  source_partitions TEXT[] NOT NULL CHECK (cardinality(source_partitions) > 0),
  writer_owner TEXT NOT NULL,
  pending_writer_owner TEXT,
  current_epoch INTEGER NOT NULL CHECK (current_epoch > 0),
  current_aggregate_version INTEGER NOT NULL CHECK (
    current_aggregate_version > 0
  ),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'legacy', 'adapter_enforced', 'backfilling', 'catch_up', 'verified',
    'writer_fenced', 'new_canonical', 'retired'
  )),
  current_version_digest TEXT NOT NULL CHECK (
    current_version_digest ~ '^[0-9a-f]{64}$'
  ),
  high_water JSONB CHECK (
    high_water IS NULL OR jsonb_typeof(high_water) = 'object'
  ),
  current_command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, scope_id),
  CHECK (
    (
      current_state = 'writer_fenced'
      AND pending_writer_owner IS NOT NULL
      AND pending_writer_owner <> writer_owner
    ) OR (
      current_state <> 'writer_fenced'
      AND pending_writer_owner IS NULL
    )
  ),
  CHECK (
    current_state NOT IN (
      'catch_up', 'verified', 'writer_fenced', 'new_canonical'
    ) OR high_water IS NOT NULL
  )
);

CREATE TABLE IF NOT EXISTS writer_scope_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  scope_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  epoch INTEGER NOT NULL CHECK (epoch > 0),
  state TEXT NOT NULL CHECK (state IN (
    'legacy', 'adapter_enforced', 'backfilling', 'catch_up', 'verified',
    'writer_fenced', 'new_canonical', 'retired'
  )),
  semantic_responsibility TEXT NOT NULL,
  source_partitions TEXT[] NOT NULL CHECK (cardinality(source_partitions) > 0),
  writer_owner TEXT NOT NULL,
  pending_writer_owner TEXT,
  parent_scope_ids UUID[] NOT NULL DEFAULT '{}',
  high_water JSONB CHECK (
    high_water IS NULL OR jsonb_typeof(high_water) = 'object'
  ),
  change_authority_ref TEXT NOT NULL,
  version_digest TEXT NOT NULL CHECK (version_digest ~ '^[0-9a-f]{64}$'),
  version JSONB NOT NULL CHECK (jsonb_typeof(version) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  recorded_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, scope_id)
    REFERENCES writer_scope_heads (tenant_id, scope_id),
  UNIQUE (tenant_id, scope_id, aggregate_version),
  UNIQUE (tenant_id, scope_id, version_digest),
  CHECK (
    (
      state = 'writer_fenced'
      AND pending_writer_owner IS NOT NULL
      AND pending_writer_owner <> writer_owner
    ) OR (
      state <> 'writer_fenced'
      AND pending_writer_owner IS NULL
    )
  )
);

CREATE TABLE IF NOT EXISTS writer_scope_partition_claims (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  semantic_responsibility TEXT NOT NULL,
  source_partition TEXT NOT NULL,
  scope_id UUID NOT NULL,
  scope_epoch INTEGER NOT NULL CHECK (scope_epoch > 0),
  scope_aggregate_version INTEGER NOT NULL CHECK (
    scope_aggregate_version > 0
  ),
  claimed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, semantic_responsibility, source_partition),
  FOREIGN KEY (tenant_id, scope_id)
    REFERENCES writer_scope_heads (tenant_id, scope_id)
);

CREATE TABLE IF NOT EXISTS writer_scope_transition_proofs (
  proof_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  subject_scope_id UUID NOT NULL,
  subject_aggregate_version INTEGER NOT NULL CHECK (
    subject_aggregate_version > 0
  ),
  proof_kind TEXT NOT NULL CHECK (proof_kind IN (
    'bootstrap_manifest', 'partition_coverage', 'adapter_compatibility',
    'backfill_manifest', 'catch_up_complete', 'semantic_equivalence',
    'authority_equivalence', 'representability', 'fence_acknowledged',
    'rollback', 'consumer_drain', 'repair_residue_closed'
  )),
  artifact_ref TEXT NOT NULL,
  artifact_digest TEXT NOT NULL CHECK (artifact_digest ~ '^[0-9a-f]{64}$'),
  proof JSONB NOT NULL CHECK (jsonb_typeof(proof) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, subject_scope_id, subject_aggregate_version)
    REFERENCES writer_scope_versions (
      tenant_id, scope_id, aggregate_version
    ) DEFERRABLE INITIALLY DEFERRED,
  UNIQUE (tenant_id, command_result_id, proof_kind, artifact_ref)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgrelid = 'writer_scope_versions'::regclass
      AND tgname = 'reject_writer_scope_versions_mutation'
      AND NOT tgisinternal
  ) THEN
    CREATE TRIGGER reject_writer_scope_versions_mutation
    BEFORE UPDATE OR DELETE ON writer_scope_versions
    FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgrelid = 'writer_scope_transition_proofs'::regclass
      AND tgname = 'reject_writer_scope_transition_proofs_mutation'
      AND NOT tgisinternal
  ) THEN
    CREATE TRIGGER reject_writer_scope_transition_proofs_mutation
    BEFORE UPDATE OR DELETE ON writer_scope_transition_proofs
    FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();
  END IF;
END $$;

ALTER TABLE writer_scope_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_heads FORCE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_partition_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_partition_claims FORCE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_transition_proofs ENABLE ROW LEVEL SECURITY;
ALTER TABLE writer_scope_transition_proofs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON writer_scope_heads;
CREATE POLICY tenant_isolation ON writer_scope_heads
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  );

DROP POLICY IF EXISTS tenant_isolation ON writer_scope_versions;
CREATE POLICY tenant_isolation ON writer_scope_versions
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  );

DROP POLICY IF EXISTS tenant_isolation ON writer_scope_partition_claims;
CREATE POLICY tenant_isolation ON writer_scope_partition_claims
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  );

DROP POLICY IF EXISTS tenant_isolation ON writer_scope_transition_proofs;
CREATE POLICY tenant_isolation ON writer_scope_transition_proofs
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(
      current_setting('app.current_tenant', true), ''
    )::uuid
  );

COMMENT ON TABLE writer_scope_heads IS
  'Canonical CAS head for one exact finite single-writer ownership scope.';
COMMENT ON TABLE writer_scope_versions IS
  'Append-only writer ownership, split, merge, fence and retirement history.';
COMMENT ON TABLE writer_scope_partition_claims IS
  'Unique active ownership claim for one semantic responsibility and partition.';

COMMIT;
