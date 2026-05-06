-- 0029_reconciliation_events.sql
--
-- T5: reconciliation as a first-class pipeline step.
--
-- Every claim_op.insert that passes the validator is now run through
-- the reconciler before apply. The reconciler decides whether the
-- proposed Model is a near-duplicate of an existing one (auto-merge),
-- borderline (human review), or genuinely new (no match). This table
-- records every decision — including no-match — so we have the data
-- to retune the cosine + recency thresholds empirically.
--
-- Foreign-key behavior:
--   * `matched_model_id` → `models.id` ON DELETE SET NULL so the audit
--      trail survives if the matched Model is later archived or
--      manually removed.
--   * `trigger_id` is NOT a FK because triggers live in a partitioned
--      queue and individual rows are pruned after success. We keep
--      the value for joins-when-the-row-still-exists and accept that
--      audit lookups may dangle.
--
-- See services/think/RECONCILIATION_DESIGN.md for the full design.
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS reconciliation_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decision TEXT NOT NULL CHECK (
    decision IN ('auto_merge', 'human_review', 'no_match')
  ),
  -- The original ClaimOp.entry that was being inserted. Stored as
  -- JSONB so we can reconstruct exactly what the LLM proposed,
  -- regardless of which fields the diff schema currently exposes.
  original_claim_op JSONB NOT NULL,
  -- The matched existing Model, if any. NULL when decision='no_match'.
  matched_model_id UUID REFERENCES models(id) ON DELETE SET NULL,
  cosine_similarity FLOAT,
  proposition_kind TEXT,
  -- Trigger that produced the candidate. Used for joining to
  -- think_runs / applied_triggers for upstream context. Not a FK
  -- because triggers are pruned post-success.
  trigger_id UUID NOT NULL,
  think_run_id UUID,
  -- Resolution by a human reviewer (only meaningful for
  -- decision='human_review' rows). NULL until resolved.
  resolved_at TIMESTAMPTZ,
  resolved_decision TEXT CHECK (
    resolved_decision IN ('merge', 'keep_separate', 'reject')
  ),
  resolved_by_actor_id UUID
);

-- Most read patterns: "show me the unresolved review queue per tenant"
-- and "find the audit row for this matched Model id".
CREATE INDEX IF NOT EXISTS recon_events_tenant_unresolved_idx
  ON reconciliation_events (tenant_id, occurred_at DESC)
  WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS recon_events_matched_model_idx
  ON reconciliation_events (matched_model_id)
  WHERE matched_model_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS recon_events_trigger_idx
  ON reconciliation_events (trigger_id);
