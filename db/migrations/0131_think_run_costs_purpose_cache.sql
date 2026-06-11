-- =====================================================================
-- 0131_think_run_costs_purpose_cache.sql — Think cost-plan §0.1
-- =====================================================================
-- Phase 0.1 of THINK-COST-PLAN: make spend measurable before any
-- cost-cutting change. Three additions to think_run_costs:
--
--   1. `purpose` — separates the run's main reasoning call from
--      question-planning and parse-repair calls, which today are
--      aggregated into one indistinguishable row. One row per purpose
--      per run; purpose is added to the primary key so the rows coexist.
--
--   2. `cache_read_input_tokens` / `cache_creation_input_tokens` — the
--      cached subset of input tokens, captured so the prompt-cache
--      layout work (§1.1) is verifiable from the ledger instead of an
--      estimate. (Note: the Codex CLI/app-server transports report only
--      estimated usage, so these are populated only on the API-key
--      `responses` path; they stay 0 otherwise.)
--
-- Existing cost queries that `SUM(llm_cost_usd)` keep working — they
-- now also include the planning/repair rows, which is more accurate.
-- =====================================================================

BEGIN;

ALTER TABLE think_run_costs
  ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'main_reasoning'
    CHECK (purpose IN ('main_reasoning', 'question_planning', 'parse_repair')),
  ADD COLUMN IF NOT EXISTS cache_read_input_tokens INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0;

-- Widen the primary key so per-purpose rows for the same run can coexist
-- (they share the transaction-time `computed_at`).
ALTER TABLE think_run_costs DROP CONSTRAINT IF EXISTS think_run_costs_pkey;
ALTER TABLE think_run_costs
  ADD CONSTRAINT think_run_costs_pkey
  PRIMARY KEY (trigger_id, computed_at, purpose);

-- Per-purpose cost dashboards (e.g. how much is question_planning?).
CREATE INDEX IF NOT EXISTS think_costs_purpose
  ON think_run_costs (purpose, computed_at DESC);

COMMIT;
