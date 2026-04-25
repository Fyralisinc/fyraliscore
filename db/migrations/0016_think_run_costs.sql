-- =====================================================================
-- 0016_think_run_costs.sql — Wave 5 OP-2
-- =====================================================================
-- THINK-DESIGN-AUDIT §9.3: no per-trigger cost tracking today. Retry-
-- heavy triggers silently consume LLM budget. Add durable cost rows
-- so per-tenant and per-trigger-kind cost queries are cheap.
--
-- One row per completed Think run (success OR failure — we record
-- cost regardless of outcome so failed-but-expensive triggers are
-- visible). Columns are idempotent on (trigger_id, computed_at) so a
-- retry-driven duplicate record just appends a new timestamped row.
--
-- Pricing is NOT encoded in the schema — we store the already-
-- computed llm_cost_usd so price-per-model can change without
-- rewriting history.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS think_run_costs (
  trigger_id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  trigger_kind TEXT NOT NULL,
  llm_calls_count INTEGER NOT NULL DEFAULT 0,
  llm_input_tokens_total INTEGER NOT NULL DEFAULT 0,
  llm_output_tokens_total INTEGER NOT NULL DEFAULT 0,
  llm_cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
  latency_total_ms INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  outcome TEXT NOT NULL CHECK (
    outcome IN (
      'success',
      'validation_failure',
      'reasoning_exhausted',
      'dead_letter',
      'skipped_idempotent',
      'failed'
    )
  ),
  model_name TEXT,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (trigger_id, computed_at)
);

-- Per-tenant dashboards (recent cost by tenant, by time).
CREATE INDEX IF NOT EXISTS think_costs_tenant_time
  ON think_run_costs (tenant_id, computed_at DESC);

-- Outcome filter (failed expensive triggers are the most important
-- to surface; partial index so success rows don't bloat it).
CREATE INDEX IF NOT EXISTS think_costs_outcome
  ON think_run_costs (outcome, computed_at DESC)
  WHERE outcome != 'success';

-- Trigger kind filter (cost per kind dashboards).
CREATE INDEX IF NOT EXISTS think_costs_trigger_kind
  ON think_run_costs (trigger_kind, computed_at DESC);

COMMIT;
