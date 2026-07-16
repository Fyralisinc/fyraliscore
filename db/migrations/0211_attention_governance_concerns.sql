-- =====================================================================
-- 0211_attention_governance_concerns.sql
--
-- Canonical attention-binding and plural-contributor Concern protocol.
-- ConcernApplier is the only writer.  Every mutation preserves the exact
-- command, authority fingerprints, CAS version, reducer result, event and
-- outbox.  Scoped-gap identity corrections are atomic predecessor/successor
-- transitions rather than in-place key edits.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS attention_governance_bindings (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  binding_id UUID NOT NULL,
  binding_version INTEGER NOT NULL CHECK (binding_version > 0),
  binding_ref TEXT NOT NULL,
  attention_source_ref TEXT NOT NULL,
  attention_source_kind TEXT NOT NULL CHECK (attention_source_kind IN (
    'goal', 'direction_bearing_decision', 'commitment',
    'standing_compliance_obligation', 'workflow_spec',
    'platform_obligation', 'discovery_duty'
  )),
  binding_digest TEXT NOT NULL CHECK (binding_digest ~ '^[0-9a-f]{64}$'),
  binding JSONB NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
  valid_from TIMESTAMPTZ NOT NULL,
  valid_until TIMESTAMPTZ NOT NULL,
  registered_by_ref TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, binding_id, binding_version),
  UNIQUE (tenant_id, binding_ref),
  CHECK (valid_until > valid_from)
);

CREATE TABLE IF NOT EXISTS concern_command_results (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_id UUID NOT NULL,
  semantic_idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  command_kind TEXT NOT NULL CHECK (command_kind IN (
    'evaluate', 'correct_identity'
  )),
  status TEXT NOT NULL CHECK (status IN (
    'applied', 'duplicate', 'rejected_terminal', 'rejected_retryable',
    'idempotency_conflict'
  )),
  command JSONB NOT NULL CHECK (jsonb_typeof(command) = 'object'),
  processing_authority_fingerprint TEXT NOT NULL CHECK (
    processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  consumption_authority_fingerprint TEXT NOT NULL CHECK (
    consumption_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  writer_scope_id TEXT NOT NULL,
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 0),
  aggregate_versions JSONB NOT NULL CHECK (
    jsonb_typeof(aggregate_versions) = 'array'
  ),
  result JSONB NOT NULL CHECK (jsonb_typeof(result) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, command_id),
  UNIQUE (tenant_id, semantic_idempotency_key)
);

CREATE TABLE IF NOT EXISTS concern_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  concern_id UUID NOT NULL,
  dedupe_key TEXT NOT NULL CHECK (dedupe_key ~ '^[0-9a-f]{64}$'),
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_state TEXT NOT NULL CHECK (current_state IN (
    'candidate', 'open', 'suspended', 'suppressed', 'accepted_risk',
    'dismissed', 'resolved', 'invalidated', 'expired'
  )),
  current_snapshot_digest TEXT NOT NULL CHECK (
    current_snapshot_digest ~ '^[0-9a-f]{64}$'
  ),
  effective_binding_digest TEXT NOT NULL CHECK (
    effective_binding_digest ~ '^[0-9a-f]{64}$'
  ),
  predecessor_concern_id UUID,
  successor_concern_id UUID,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, concern_id),
  UNIQUE (tenant_id, dedupe_key),
  CHECK (predecessor_concern_id IS NULL OR predecessor_concern_id <> concern_id),
  CHECK (successor_concern_id IS NULL OR successor_concern_id <> concern_id)
);

CREATE TABLE IF NOT EXISTS concern_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  concern_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  state TEXT NOT NULL CHECK (state IN (
    'candidate', 'open', 'suspended', 'suppressed', 'accepted_risk',
    'dismissed', 'resolved', 'invalidated', 'expired'
  )),
  snapshot_digest TEXT NOT NULL CHECK (snapshot_digest ~ '^[0-9a-f]{64}$'),
  snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
  effective_binding_digest TEXT NOT NULL CHECK (
    effective_binding_digest ~ '^[0-9a-f]{64}$'
  ),
  effective_binding_envelope JSONB NOT NULL CHECK (
    jsonb_typeof(effective_binding_envelope) = 'object'
  ),
  command_result_id UUID NOT NULL REFERENCES concern_command_results(id),
  evidence_cutoff TIMESTAMPTZ NOT NULL,
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, concern_id)
    REFERENCES concern_heads (tenant_id, concern_id),
  UNIQUE (tenant_id, concern_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS concern_transitions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  concern_id UUID NOT NULL,
  from_version INTEGER NOT NULL CHECK (from_version >= 0),
  to_version INTEGER NOT NULL CHECK (to_version > 0),
  from_state TEXT,
  to_state TEXT NOT NULL,
  cause TEXT NOT NULL,
  transition JSONB NOT NULL CHECK (jsonb_typeof(transition) = 'object'),
  transitioned_at TIMESTAMPTZ NOT NULL,
  command_result_id UUID NOT NULL REFERENCES concern_command_results(id),
  FOREIGN KEY (tenant_id, concern_id, to_version)
    REFERENCES concern_versions (tenant_id, concern_id, aggregate_version),
  UNIQUE (tenant_id, concern_id, to_version),
  CHECK (to_version = from_version + 1)
);

CREATE TABLE IF NOT EXISTS concern_identity_corrections (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  predecessor_concern_id UUID NOT NULL,
  predecessor_version INTEGER NOT NULL CHECK (predecessor_version > 0),
  successor_concern_id UUID NOT NULL,
  successor_version INTEGER NOT NULL CHECK (successor_version = 1),
  correction_epoch INTEGER NOT NULL CHECK (correction_epoch > 0),
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  correction_reason TEXT NOT NULL,
  command_result_id UUID NOT NULL REFERENCES concern_command_results(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, predecessor_concern_id)
    REFERENCES concern_heads (tenant_id, concern_id),
  FOREIGN KEY (tenant_id, successor_concern_id)
    REFERENCES concern_heads (tenant_id, concern_id),
  UNIQUE (tenant_id, predecessor_concern_id, correction_epoch),
  UNIQUE (tenant_id, predecessor_concern_id, successor_concern_id),
  CHECK (predecessor_concern_id <> successor_concern_id)
);

CREATE TABLE IF NOT EXISTS concern_canonical_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_result_id UUID NOT NULL REFERENCES concern_command_results(id),
  concern_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  semantic_transition TEXT NOT NULL,
  event_payload JSONB NOT NULL CHECK (jsonb_typeof(event_payload) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, concern_id, aggregate_version)
    REFERENCES concern_versions (tenant_id, concern_id, aggregate_version),
  UNIQUE (tenant_id, concern_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS concern_outbox_records (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  event_id UUID NOT NULL REFERENCES concern_canonical_events(id),
  destination_operation TEXT NOT NULL,
  payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN (
    'pending', 'delivering', 'delivered', 'retry_scheduled', 'failed_terminal'
  )),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deadline TIMESTAMPTZ NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  attempt_budget INTEGER NOT NULL CHECK (attempt_budget > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, event_id, destination_operation)
);

CREATE INDEX IF NOT EXISTS attention_governance_bindings_source_idx
  ON attention_governance_bindings (
    tenant_id, attention_source_ref, valid_from, valid_until
  );
CREATE INDEX IF NOT EXISTS concern_heads_state_idx
  ON concern_heads (tenant_id, current_state, updated_at);
CREATE INDEX IF NOT EXISTS concern_versions_current_idx
  ON concern_versions (tenant_id, concern_id, aggregate_version DESC);
CREATE INDEX IF NOT EXISTS concern_outbox_due_idx
  ON concern_outbox_records (tenant_id, available_at, created_at)
  WHERE state IN ('pending', 'retry_scheduled');

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'attention_governance_bindings',
    'concern_command_results',
    'concern_heads',
    'concern_versions',
    'concern_transitions',
    'concern_identity_corrections',
    'concern_canonical_events',
    'concern_outbox_records'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') '
      'WITH CHECK ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      t
    );
  END LOOP;
END $$;

COMMIT;
