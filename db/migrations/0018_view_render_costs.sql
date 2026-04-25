-- =====================================================================
-- 0018_view_render_costs.sql — Agent-RND (Company OS CEO view, §2)
-- =====================================================================
-- Mirrors the pattern in 0016_think_run_costs.sql. One row per
-- rendering-service call (greeting / card / query-grid / conversation
-- turn / close-line). Used for cost observability by tenant, by
-- rendering type, and by outcome.
--
-- Primary key is (render_id, computed_at) so a retry-driven
-- re-record appends a new timestamped row rather than conflicting.
-- Pricing is NOT encoded: we persist the already-computed llm_cost_usd
-- so price revisions don't rewrite history.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS view_render_costs (
  render_id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  render_kind TEXT NOT NULL CHECK (
    render_kind IN (
      'greeting',
      'card_observation',
      'card_decision',
      'card_question',
      'query_grid',
      'conversation_turn',
      'close_line'
    )
  ),
  llm_calls_count INTEGER NOT NULL DEFAULT 0,
  llm_input_tokens_total INTEGER NOT NULL DEFAULT 0,
  llm_output_tokens_total INTEGER NOT NULL DEFAULT 0,
  llm_cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
  latency_total_ms INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  flagged BOOLEAN NOT NULL DEFAULT FALSE,
  outcome TEXT NOT NULL CHECK (
    outcome IN (
      'success',
      'success_with_flags',
      'rejected_after_retry',
      'failed'
    )
  ),
  model_name TEXT,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (render_id, computed_at)
);

-- Per-tenant dashboards (recent cost by tenant, by time).
CREATE INDEX IF NOT EXISTS render_costs_tenant_time
  ON view_render_costs (tenant_id, computed_at DESC);

-- Outcome filter (rejected / failed rows are the most interesting
-- for forensics; partial index so success rows don't bloat it).
CREATE INDEX IF NOT EXISTS render_costs_outcome
  ON view_render_costs (outcome, computed_at DESC)
  WHERE outcome != 'success';

-- Render-kind filter (cost per surface type).
CREATE INDEX IF NOT EXISTS render_costs_kind
  ON view_render_costs (render_kind, computed_at DESC);

COMMIT;
