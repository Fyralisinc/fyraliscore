-- =====================================================================
-- 0190_governed_intent_protocol.sql
--
-- Durable proposal -> exact acceptance -> constitutive command spine.
-- Interpreted or model-produced direction is never written directly into
-- goals, decisions, commitments, priorities, or workflow specifications.
-- The sidecar versions existing Act aggregates without rewriting history.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS intent_proposals (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposal_version INTEGER NOT NULL CHECK (proposal_version > 0),
  semantic_idempotency_key TEXT NOT NULL,
  object_kind TEXT NOT NULL CHECK (object_kind IN (
    'goal', 'priority', 'decision', 'commitment', 'workflow_spec',
    'standing_compliance_obligation'
  )),
  operation TEXT NOT NULL CHECK (operation IN (
    'create', 'update', 'transition', 'supersede', 'retire'
  )),
  target_aggregate_id UUID,
  normalized_payload_digest TEXT NOT NULL CHECK (
    normalized_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  source_assertion_refs TEXT[] NOT NULL CHECK (
    cardinality(source_assertion_refs) > 0
  ),
  grounding_dependency_refs TEXT[] NOT NULL DEFAULT '{}',
  processing_authority_fingerprint TEXT NOT NULL CHECK (
    processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  proposal JSONB NOT NULL CHECK (jsonb_typeof(proposal) = 'object'),
  fate TEXT NOT NULL CHECK (fate IN (
    'open', 'deferred', 'accepted_for_authorization', 'rejected',
    'expired', 'superseded'
  )),
  review_due_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id, proposal_version),
  UNIQUE (tenant_id, semantic_idempotency_key)
);

CREATE TABLE IF NOT EXISTS intent_proposal_fate_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposal_id UUID NOT NULL,
  proposal_version INTEGER NOT NULL,
  from_fate TEXT,
  to_fate TEXT NOT NULL CHECK (to_fate IN (
    'open', 'deferred', 'accepted_for_authorization', 'rejected',
    'expired', 'superseded'
  )),
  reason_class TEXT NOT NULL,
  reason TEXT NOT NULL,
  actor_or_service_ref TEXT NOT NULL,
  event_digest TEXT NOT NULL CHECK (event_digest ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, proposal_id, proposal_version)
    REFERENCES intent_proposals (tenant_id, id, proposal_version),
  UNIQUE (tenant_id, proposal_id, proposal_version, event_digest)
);

CREATE TABLE IF NOT EXISTS intent_exact_acceptances (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposal_id UUID NOT NULL,
  proposal_version INTEGER NOT NULL,
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  normalized_payload_digest TEXT NOT NULL CHECK (
    normalized_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  principal_id TEXT NOT NULL,
  capability_ref TEXT NOT NULL,
  authority_fingerprint TEXT NOT NULL CHECK (
    authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  acceptance JSONB NOT NULL CHECK (jsonb_typeof(acceptance) = 'object'),
  accepted_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, proposal_id, proposal_version)
    REFERENCES intent_proposals (tenant_id, id, proposal_version),
  UNIQUE (tenant_id, proposal_id, proposal_version)
);

CREATE TABLE IF NOT EXISTS intent_aggregate_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_kind TEXT NOT NULL CHECK (object_kind IN (
    'goal', 'priority', 'decision', 'commitment', 'workflow_spec',
    'standing_compliance_obligation'
  )),
  aggregate_id UUID NOT NULL,
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_payload_digest TEXT NOT NULL CHECK (
    current_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  current_fate TEXT NOT NULL CHECK (current_fate IN (
    'current', 'suspended_basis_ended', 'disputed_pending_review',
    'retrospectively_contaminated', 'superseded', 'retired'
  )),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, object_kind, aggregate_id)
);

CREATE TABLE IF NOT EXISTS intent_command_results (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_id UUID NOT NULL,
  semantic_idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  mutation_payload_digest TEXT NOT NULL CHECK (
    mutation_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  object_kind TEXT NOT NULL CHECK (object_kind IN (
    'goal', 'priority', 'decision', 'commitment', 'workflow_spec',
    'standing_compliance_obligation'
  )),
  operation TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'applied', 'duplicate', 'rejected_terminal', 'rejected_retryable',
    'idempotency_conflict'
  )),
  proposal_acceptance_id UUID REFERENCES intent_exact_acceptances(id),
  authority_basis JSONB NOT NULL CHECK (jsonb_typeof(authority_basis) = 'object'),
  survival_policy JSONB NOT NULL CHECK (jsonb_typeof(survival_policy) = 'object'),
  grounding_dependencies JSONB NOT NULL CHECK (
    jsonb_typeof(grounding_dependencies) = 'array'
  ),
  writer_scope_id TEXT NOT NULL,
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 0),
  aggregate_id UUID,
  aggregate_version INTEGER CHECK (aggregate_version > 0),
  result JSONB NOT NULL CHECK (jsonb_typeof(result) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, command_id),
  UNIQUE (tenant_id, semantic_idempotency_key)
);

CREATE TABLE IF NOT EXISTS intent_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_kind TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  operation TEXT NOT NULL,
  mutation_payload_digest TEXT NOT NULL CHECK (
    mutation_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  command_result_id UUID NOT NULL REFERENCES intent_command_results(id),
  proposal_acceptance_id UUID REFERENCES intent_exact_acceptances(id),
  authority_basis_snapshot JSONB NOT NULL CHECK (
    jsonb_typeof(authority_basis_snapshot) = 'object'
  ),
  survival_policy JSONB NOT NULL CHECK (jsonb_typeof(survival_policy) = 'object'),
  grounding_dependencies JSONB NOT NULL CHECK (
    jsonb_typeof(grounding_dependencies) = 'array'
  ),
  effective_at TIMESTAMPTZ NOT NULL,
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  fate TEXT NOT NULL CHECK (fate IN (
    'current', 'retained_basis_valid_at_commit', 'suspended_basis_ended',
    'retained_with_revalidated_grounding', 'disputed_pending_review',
    'superseded', 'cancelled_if_reversible', 'reauthorization_required',
    'retrospectively_contaminated', 'retired'
  )),
  UNIQUE (tenant_id, object_kind, aggregate_id, aggregate_version),
  UNIQUE (tenant_id, command_result_id)
);

CREATE TABLE IF NOT EXISTS intent_canonical_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_result_id UUID NOT NULL REFERENCES intent_command_results(id),
  object_kind TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  semantic_transition TEXT NOT NULL,
  event_payload JSONB NOT NULL CHECK (jsonb_typeof(event_payload) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, object_kind, aggregate_id, aggregate_version)
);

CREATE TABLE IF NOT EXISTS intent_outbox_records (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  event_id UUID NOT NULL REFERENCES intent_canonical_events(id),
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

CREATE TABLE IF NOT EXISTS intent_basis_change_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_kind TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  prior_aggregate_version INTEGER NOT NULL CHECK (prior_aggregate_version > 0),
  change_kind TEXT NOT NULL,
  resulting_fate TEXT NOT NULL,
  change_payload JSONB NOT NULL CHECK (jsonb_typeof(change_payload) = 'object'),
  command_result_id UUID REFERENCES intent_command_results(id),
  changed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, object_kind, aggregate_id, prior_aggregate_version, id)
);

CREATE INDEX IF NOT EXISTS intent_proposals_open_idx
  ON intent_proposals (tenant_id, review_due_at, created_at)
  WHERE fate IN ('open', 'deferred');
CREATE INDEX IF NOT EXISTS intent_versions_current_idx
  ON intent_versions (tenant_id, object_kind, aggregate_id, aggregate_version DESC);
CREATE INDEX IF NOT EXISTS intent_outbox_due_idx
  ON intent_outbox_records (tenant_id, available_at, created_at)
  WHERE state IN ('pending', 'retry_scheduled');

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'intent_proposals',
    'intent_proposal_fate_events',
    'intent_exact_acceptances',
    'intent_aggregate_heads',
    'intent_command_results',
    'intent_versions',
    'intent_canonical_events',
    'intent_outbox_records',
    'intent_basis_change_events'
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

COMMENT ON TABLE intent_proposals IS
  'Interpreted direction and model output; never itself company intent.';
COMMENT ON TABLE intent_exact_acceptances IS
  'Explicit capable-principal acceptance of one immutable proposal and payload digest.';
COMMENT ON TABLE intent_versions IS
  'Constitutive authority, grounding, and survival lineage for versioned intent aggregates.';
COMMENT ON TABLE intent_canonical_events IS
  'Neutral committed transition proof emitted atomically by IntentApplier.';

COMMIT;
