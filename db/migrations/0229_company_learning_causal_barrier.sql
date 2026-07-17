-- Versioned truth-critical barriers and exact decision-level context credit.

BEGIN;

CREATE TABLE IF NOT EXISTS company_learning_barriers (
  barrier_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL,
  barrier_version BIGINT NOT NULL CHECK (barrier_version > 0),
  prior_barrier_id UUID,
  expected_model_version_ids UUID[] NOT NULL DEFAULT '{}',
  expected_relation_version_ids UUID[] NOT NULL DEFAULT '{}',
  invalidated_model_version_ids UUID[] NOT NULL DEFAULT '{}',
  truth_critical_pending_count INTEGER NOT NULL CHECK (
    truth_critical_pending_count >= 0
  ),
  status TEXT NOT NULL CHECK (status IN ('complete', 'failed')),
  receipt_digest TEXT NOT NULL CHECK (receipt_digest ~ '^[0-9a-f]{64}$'),
  completed_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, barrier_id),
  UNIQUE (tenant_id, batch_id),
  UNIQUE (tenant_id, barrier_version),
  FOREIGN KEY (tenant_id, prior_barrier_id)
    REFERENCES company_learning_barriers(tenant_id, barrier_id)
);

CREATE INDEX IF NOT EXISTS company_learning_barriers_latest_idx
  ON company_learning_barriers (tenant_id, barrier_version DESC);

CREATE TABLE IF NOT EXISTS company_learning_context_decisions (
  decision_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL,
  route_id TEXT NOT NULL,
  context_item_kind TEXT NOT NULL CHECK (context_item_kind IN (
    'current_episode', 'historical_observation', 'accepted_model',
    'accepted_relation', 'residual', 'candidate'
  )),
  context_item_id TEXT NOT NULL,
  context_item_version TEXT NOT NULL,
  retrieved BOOLEAN NOT NULL,
  selected BOOLEAN NOT NULL,
  included BOOLEAN NOT NULL,
  referenced BOOLEAN NOT NULL,
  counterevidence_retained BOOLEAN NOT NULL,
  confidence_affecting BOOLEAN NOT NULL,
  necessary_background BOOLEAN NOT NULL,
  historical_reopen_reason TEXT CHECK (historical_reopen_reason IN (
    'cold_start', 'sparse_coverage', 'contradiction', 'provenance',
    'novelty', 'correction', 'unresolved_question'
  )),
  decision_fate TEXT NOT NULL CHECK (decision_fate IN (
    'mutation', 'justified_noop', 'validator_drop', 'unused'
  )),
  result_object_kind TEXT,
  result_object_id UUID,
  evidence_lineage JSONB NOT NULL DEFAULT '[]'::jsonb,
  decided_at TIMESTAMPTZ NOT NULL,
  CHECK (NOT referenced OR included),
  CHECK (NOT included OR selected),
  CHECK (NOT selected OR retrieved),
  CHECK (
    context_item_kind <> 'historical_observation'
    OR NOT selected
    OR historical_reopen_reason IS NOT NULL
  )
);

CREATE INDEX IF NOT EXISTS company_learning_context_decisions_batch_idx
  ON company_learning_context_decisions (tenant_id, batch_id, decision_id);

CREATE TABLE IF NOT EXISTS company_learning_outcome_links (
  outcome_link_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  decision_id UUID NOT NULL REFERENCES company_learning_context_decisions(decision_id),
  outcome_kind TEXT NOT NULL CHECK (outcome_kind IN (
    'confirmation', 'revision', 'falsification', 'correction',
    'human_adjudication'
  )),
  outcome_object_kind TEXT NOT NULL,
  outcome_object_id UUID NOT NULL,
  attribution_basis TEXT NOT NULL CHECK (
    attribution_basis IN ('direct', 'associative')
  ),
  evidence_lineage JSONB NOT NULL DEFAULT '[]'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, decision_id, outcome_kind, outcome_object_id)
);

ALTER TABLE company_learning_barriers ENABLE ROW LEVEL SECURITY;
ALTER TABLE company_learning_context_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE company_learning_outcome_links ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON company_learning_barriers;
CREATE POLICY tenant_isolation ON company_learning_barriers
  USING (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid)
  WITH CHECK (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);
DROP POLICY IF EXISTS tenant_isolation ON company_learning_context_decisions;
CREATE POLICY tenant_isolation ON company_learning_context_decisions
  USING (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid)
  WITH CHECK (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);
DROP POLICY IF EXISTS tenant_isolation ON company_learning_outcome_links;
CREATE POLICY tenant_isolation ON company_learning_outcome_links
  USING (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid)
  WITH CHECK (NULLIF(current_setting('app.current_tenant', true), '') IS NULL OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);

COMMIT;
