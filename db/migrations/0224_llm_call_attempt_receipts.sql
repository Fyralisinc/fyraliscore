-- =====================================================================
-- 0224_llm_call_attempt_receipts.sql
--
-- Immutable, tenant-scoped receipts for every logical LLM call and every
-- physical provider-wrapper attempt. Aggregate think_run_costs remain a
-- dashboard surface; these rows are the reconciliation source.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS llm_logical_call_receipts (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  logical_call_id TEXT NOT NULL CHECK (length(btrim(logical_call_id)) > 0),
  trigger_id UUID,
  think_run_id UUID,
  batch_id TEXT,
  provider TEXT NOT NULL CHECK (length(btrim(provider)) > 0),
  model TEXT NOT NULL CHECK (length(btrim(model)) > 0),
  purpose TEXT NOT NULL CHECK (length(btrim(purpose)) > 0),
  schema_name TEXT NOT NULL CHECK (length(btrim(schema_name)) > 0),
  prompt_digest TEXT CHECK (prompt_digest ~ '^[0-9a-f]{64}$'),
  context_digest TEXT CHECK (context_digest ~ '^[0-9a-f]{64}$'),
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN (
      'success', 'cache_hit', 'timeout', 'parse_failure',
      'provider_error', 'exhausted'
    )
  ),
  physical_attempt_count INTEGER NOT NULL CHECK (physical_attempt_count >= 0),
  validation_outcome TEXT,
  apply_outcome TEXT,
  error_class TEXT,
  error_message TEXT,
  receipt_created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, logical_call_id),
  CHECK (ended_at >= started_at),
  CHECK (
    (outcome = 'cache_hit' AND physical_attempt_count = 0)
    OR (outcome <> 'cache_hit' AND physical_attempt_count > 0)
  )
);

CREATE TABLE IF NOT EXISTS llm_provider_attempt_receipts (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  physical_attempt_id TEXT NOT NULL CHECK (
    length(btrim(physical_attempt_id)) > 0
  ),
  logical_call_id TEXT NOT NULL,
  parent_attempt_id TEXT,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  provider TEXT NOT NULL CHECK (length(btrim(provider)) > 0),
  model TEXT NOT NULL CHECK (length(btrim(model)) > 0),
  purpose TEXT NOT NULL CHECK (length(btrim(purpose)) > 0),
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ NOT NULL,
  outcome TEXT NOT NULL CHECK (
    outcome IN ('success', 'timeout', 'parse_failure', 'provider_error')
  ),
  error_class TEXT,
  error_message TEXT,
  retry_scheduled BOOLEAN NOT NULL DEFAULT false,
  input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
  output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
  cache_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_tokens >= 0),
  cost_usd NUMERIC(18, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
  usage_exactness TEXT NOT NULL CHECK (
    usage_exactness IN ('reported', 'estimated', 'unavailable')
  ),
  pricing_version TEXT NOT NULL CHECK (length(btrim(pricing_version)) > 0),
  receipt_created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, physical_attempt_id),
  UNIQUE (tenant_id, logical_call_id, ordinal),
  FOREIGN KEY (tenant_id, logical_call_id)
    REFERENCES llm_logical_call_receipts (tenant_id, logical_call_id)
    DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, parent_attempt_id)
    REFERENCES llm_provider_attempt_receipts (tenant_id, physical_attempt_id)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (ended_at >= started_at),
  CHECK (parent_attempt_id IS NULL OR parent_attempt_id <> physical_attempt_id)
);

CREATE INDEX IF NOT EXISTS llm_logical_call_receipts_run_idx
  ON llm_logical_call_receipts (tenant_id, think_run_id, started_at);
CREATE INDEX IF NOT EXISTS llm_logical_call_receipts_trigger_idx
  ON llm_logical_call_receipts (tenant_id, trigger_id, started_at);
CREATE INDEX IF NOT EXISTS llm_logical_call_receipts_batch_idx
  ON llm_logical_call_receipts (tenant_id, batch_id, started_at)
  WHERE batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS llm_provider_attempt_receipts_call_idx
  ON llm_provider_attempt_receipts (tenant_id, logical_call_id, ordinal);
CREATE INDEX IF NOT EXISTS llm_provider_attempt_receipts_failure_idx
  ON llm_provider_attempt_receipts (tenant_id, outcome, ended_at)
  WHERE outcome <> 'success';

ALTER TABLE llm_logical_call_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE llm_logical_call_receipts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON llm_logical_call_receipts;
CREATE POLICY tenant_isolation ON llm_logical_call_receipts
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

ALTER TABLE llm_provider_attempt_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE llm_provider_attempt_receipts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON llm_provider_attempt_receipts;
CREATE POLICY tenant_isolation ON llm_provider_attempt_receipts
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON TABLE llm_logical_call_receipts IS
  'Immutable terminal receipts for logical LLM calls; one row reconciles zero or more physical attempts.';
COMMENT ON TABLE llm_provider_attempt_receipts IS
  'Immutable wrapper-level provider attempts including timeout, failure, parse repair, tokens and cost basis.';

COMMIT;
