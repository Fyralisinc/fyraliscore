-- =====================================================================
-- 0194_consequential_agency_protocol.sql
--
-- Canonical proposal/spec -> prediction -> authorization -> independent
-- outcome -> settlement -> attribution protocol.  These records do not
-- replace the legacy product Forecasts table, Model-substrate expectations,
-- SAGE inquiry outcome events, or human correction facts; those narrower
-- artifacts may later become governed producers/consumers of this protocol.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS agency_command_results (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_id UUID NOT NULL,
  writer_id TEXT NOT NULL CHECK (writer_id IN (
    'ProposalAppender', 'EpisodeCoordinator', 'PredictionWriter',
    'AuthorizationApplier', 'OutcomeRecorder', 'SettlementApplier',
    'AttributionApplier'
  )),
  semantic_idempotency_key TEXT NOT NULL,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  command_kind TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'applied', 'duplicate', 'rejected_terminal', 'rejected_retryable',
    'idempotency_conflict'
  )),
  command JSONB NOT NULL CHECK (jsonb_typeof(command) = 'object'),
  processing_authority_fingerprint TEXT NOT NULL CHECK (
    processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  consumption_authority_fingerprint TEXT CHECK (
    consumption_authority_fingerprint IS NULL
    OR consumption_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  writer_scope_id TEXT NOT NULL,
  writer_epoch INTEGER NOT NULL CHECK (writer_epoch >= 0),
  object_type TEXT NOT NULL,
  object_id UUID NOT NULL,
  object_version INTEGER NOT NULL CHECK (object_version > 0),
  result JSONB NOT NULL CHECK (jsonb_typeof(result) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, command_id),
  UNIQUE (tenant_id, writer_id, semantic_idempotency_key)
);

CREATE TABLE IF NOT EXISTS intervention_episode_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  episode_kind TEXT NOT NULL,
  current_version INTEGER NOT NULL CHECK (current_version > 0),
  current_episode_digest TEXT NOT NULL CHECK (
    current_episode_digest ~ '^[0-9a-f]{64}$'
  ),
  intervention_spec_digest TEXT CHECK (
    intervention_spec_digest IS NULL
    OR intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, episode_id)
);

CREATE TABLE IF NOT EXISTS intervention_episode_versions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
  episode_digest TEXT NOT NULL CHECK (episode_digest ~ '^[0-9a-f]{64}$'),
  episode JSONB NOT NULL CHECK (jsonb_typeof(episode) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  transaction_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, episode_id, aggregate_version),
  UNIQUE (tenant_id, command_result_id)
);

CREATE TABLE IF NOT EXISTS consequential_intervention_specs (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  spec_id UUID NOT NULL,
  spec_digest TEXT NOT NULL CHECK (spec_digest ~ '^[0-9a-f]{64}$'),
  episode_id UUID NOT NULL,
  registered_by_proposal_id UUID NOT NULL,
  registered_by_proposal_version INTEGER NOT NULL CHECK (
    registered_by_proposal_version > 0
  ),
  spec JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, spec_id),
  UNIQUE (tenant_id, spec_digest),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id)
);

CREATE TABLE IF NOT EXISTS consequential_proposals (
  id UUID NOT NULL,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposal_version INTEGER NOT NULL CHECK (proposal_version > 0),
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  episode_id UUID NOT NULL,
  intervention_spec_id UUID NOT NULL,
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  proposal JSONB NOT NULL CHECK (jsonb_typeof(proposal) = 'object'),
  current_fate_version INTEGER NOT NULL DEFAULT 1 CHECK (current_fate_version > 0),
  current_fate TEXT NOT NULL CHECK (current_fate IN (
    'open', 'deferred', 'accepted_for_authorization', 'rejected',
    'expired', 'superseded'
  )),
  review_due_at TIMESTAMPTZ NOT NULL,
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, id, proposal_version),
  UNIQUE (tenant_id, proposal_digest),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  FOREIGN KEY (tenant_id, intervention_spec_id)
    REFERENCES consequential_intervention_specs (tenant_id, spec_id)
      DEFERRABLE INITIALLY DEFERRED
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'consequential_specs_registering_proposal_fk'
  ) THEN
    ALTER TABLE consequential_intervention_specs
      ADD CONSTRAINT consequential_specs_registering_proposal_fk
      FOREIGN KEY (
        tenant_id, registered_by_proposal_id, registered_by_proposal_version
      ) REFERENCES consequential_proposals (tenant_id, id, proposal_version)
        DEFERRABLE INITIALLY DEFERRED;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS consequential_proposal_reviews (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  proposal_id UUID NOT NULL,
  proposal_version INTEGER NOT NULL CHECK (proposal_version > 0),
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  from_fate_version INTEGER NOT NULL CHECK (from_fate_version > 0),
  to_fate_version INTEGER NOT NULL CHECK (to_fate_version > 1),
  from_fate TEXT NOT NULL,
  to_fate TEXT NOT NULL,
  principal_or_policy_ref TEXT NOT NULL,
  consumption_authority_fingerprint TEXT NOT NULL CHECK (
    consumption_authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  review JSONB NOT NULL CHECK (jsonb_typeof(review) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  decided_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, proposal_id, proposal_version)
    REFERENCES consequential_proposals (tenant_id, id, proposal_version),
  UNIQUE (tenant_id, proposal_id, to_fate_version),
  CHECK (to_fate_version = from_fate_version + 1)
);

CREATE TABLE IF NOT EXISTS consequential_predictions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  prediction_kind TEXT NOT NULL,
  prediction_digest TEXT NOT NULL CHECK (prediction_digest ~ '^[0-9a-f]{64}$'),
  intervention_spec_digest TEXT CHECK (
    intervention_spec_digest IS NULL
    OR intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  metric_definition TEXT NOT NULL,
  evidence_cutoff TIMESTAMPTZ NOT NULL,
  forecast_window_start TIMESTAMPTZ NOT NULL,
  forecast_window_end TIMESTAMPTZ NOT NULL,
  preregistered_at TIMESTAMPTZ NOT NULL,
  prediction JSONB NOT NULL CHECK (jsonb_typeof(prediction) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, prediction_digest),
  CHECK (evidence_cutoff <= preregistered_at),
  CHECK (preregistered_at <= forecast_window_start),
  CHECK (forecast_window_end > forecast_window_start)
);

CREATE TABLE IF NOT EXISTS consequential_authorization_decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  proposal_id UUID NOT NULL,
  proposal_version INTEGER NOT NULL CHECK (proposal_version > 0),
  proposal_digest TEXT NOT NULL CHECK (proposal_digest ~ '^[0-9a-f]{64}$'),
  intervention_spec_digest TEXT NOT NULL CHECK (
    intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  decision_digest TEXT NOT NULL CHECK (decision_digest ~ '^[0-9a-f]{64}$'),
  disposition TEXT NOT NULL CHECK (disposition IN ('authorized', 'rejected')),
  authority_fingerprint TEXT NOT NULL CHECK (
    authority_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  use_budget INTEGER NOT NULL CHECK (use_budget >= 0),
  attempt_budget INTEGER NOT NULL CHECK (attempt_budget >= 0),
  decision JSONB NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  decided_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, proposal_id, proposal_version)
    REFERENCES consequential_proposals (tenant_id, id, proposal_version),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, decision_digest)
);

CREATE TABLE IF NOT EXISTS consequential_outcomes (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  outcome_digest TEXT NOT NULL CHECK (outcome_digest ~ '^[0-9a-f]{64}$'),
  metric_definition TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  valid_time TIMESTAMPTZ NOT NULL,
  independent_of_execution_claim BOOLEAN NOT NULL,
  measurement_quality DOUBLE PRECISION NOT NULL CHECK (
    measurement_quality >= 0 AND measurement_quality <= 1
  ),
  outcome JSONB NOT NULL CHECK (jsonb_typeof(outcome) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, outcome_digest),
  CHECK (independent_of_execution_claim),
  CHECK (observed_at >= valid_time)
);

CREATE TABLE IF NOT EXISTS consequential_settlements (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  prediction_id UUID NOT NULL,
  outcome_id UUID,
  settlement_digest TEXT NOT NULL CHECK (settlement_digest ~ '^[0-9a-f]{64}$'),
  disposition TEXT NOT NULL CHECK (disposition IN (
    'settled', 'censored', 'incomparable', 'measurement_unavailable'
  )),
  settlement JSONB NOT NULL CHECK (jsonb_typeof(settlement) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  settled_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (prediction_id) REFERENCES consequential_predictions(id),
  FOREIGN KEY (outcome_id) REFERENCES consequential_outcomes(id),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, prediction_id),
  UNIQUE (tenant_id, settlement_digest)
);

CREATE TABLE IF NOT EXISTS consequential_attributions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  episode_id UUID NOT NULL,
  settlement_id UUID NOT NULL REFERENCES consequential_settlements(id),
  attribution_digest TEXT NOT NULL CHECK (
    attribution_digest ~ '^[0-9a-f]{64}$'
  ),
  subject_ref TEXT NOT NULL,
  causal_confidence DOUBLE PRECISION NOT NULL CHECK (
    causal_confidence >= 0 AND causal_confidence <= 1
  ),
  withheld_credit BOOLEAN NOT NULL,
  attribution JSONB NOT NULL CHECK (jsonb_typeof(attribution) = 'object'),
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, episode_id)
    REFERENCES intervention_episode_heads (tenant_id, episode_id),
  UNIQUE (tenant_id, settlement_id, subject_ref),
  UNIQUE (tenant_id, attribution_digest)
);

CREATE TABLE IF NOT EXISTS agency_canonical_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  command_result_id UUID NOT NULL REFERENCES agency_command_results(id),
  writer_id TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id UUID NOT NULL,
  object_version INTEGER NOT NULL CHECK (object_version > 0),
  semantic_transition TEXT NOT NULL,
  intervention_spec_digest TEXT CHECK (
    intervention_spec_digest IS NULL
    OR intervention_spec_digest ~ '^[0-9a-f]{64}$'
  ),
  event_payload JSONB NOT NULL CHECK (jsonb_typeof(event_payload) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, writer_id, object_type, object_id, object_version)
);

CREATE TABLE IF NOT EXISTS agency_outbox_records (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  event_id UUID NOT NULL REFERENCES agency_canonical_events(id),
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

CREATE INDEX IF NOT EXISTS consequential_proposals_review_idx
  ON consequential_proposals (tenant_id, current_fate, review_due_at);
CREATE INDEX IF NOT EXISTS consequential_predictions_due_idx
  ON consequential_predictions (tenant_id, forecast_window_end, created_at);
CREATE INDEX IF NOT EXISTS consequential_outcomes_episode_metric_idx
  ON consequential_outcomes (tenant_id, episode_id, metric_definition, observed_at);
CREATE INDEX IF NOT EXISTS consequential_settlements_episode_idx
  ON consequential_settlements (tenant_id, episode_id, settled_at);
CREATE INDEX IF NOT EXISTS agency_outbox_due_idx
  ON agency_outbox_records (tenant_id, available_at, created_at)
  WHERE state IN ('pending', 'retry_scheduled');

CREATE OR REPLACE FUNCTION reject_consequential_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is append-only; corrections require a new governed object',
    TG_TABLE_NAME USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

DO $$
DECLARE
  t TEXT;
  trigger_name TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'consequential_intervention_specs',
    'consequential_predictions',
    'consequential_authorization_decisions',
    'consequential_outcomes',
    'consequential_settlements',
    'consequential_attributions'
  ]
  LOOP
    trigger_name := 'reject_' || t || '_mutation';
    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger WHERE tgname = trigger_name AND NOT tgisinternal
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation()',
        trigger_name,
        t
      );
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'agency_command_results',
    'intervention_episode_heads',
    'intervention_episode_versions',
    'consequential_intervention_specs',
    'consequential_proposals',
    'consequential_proposal_reviews',
    'consequential_predictions',
    'consequential_authorization_decisions',
    'consequential_outcomes',
    'consequential_settlements',
    'consequential_attributions',
    'agency_canonical_events',
    'agency_outbox_records'
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

COMMENT ON TABLE consequential_predictions IS
  'Canonical immutable consequential predictions; distinct from product forecasts and Model expectations.';
COMMENT ON TABLE consequential_outcomes IS
  'Independent measurement records; task or effect completion cannot be inserted as Outcome.';
COMMENT ON TABLE consequential_settlements IS
  'Immutable comparison and residual classification against one preregistered Prediction.';

COMMIT;
