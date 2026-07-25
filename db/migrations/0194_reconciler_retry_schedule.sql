-- ============================================================================
-- 0194_reconciler_retry_schedule.sql
--
-- Durable RetryLater scheduling for both reconciliation workflows.
--
-- A source reconciler can make provider requests after historical shards have
-- settled.  ProviderTransport represents a provider cooldown as RetryLater;
-- treating that exception as either a terminal failure or an empty/clean probe
-- loses data.  These columns let the at-completion and periodic reconcilers
-- commit the provider's not-before time, release their row lock, and resume in a
-- later worker tick.
-- ============================================================================

BEGIN;

ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS reconcile_next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reconcile_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reconcile_retry_reason TEXT,
    ADD COLUMN IF NOT EXISTS reconcile_retry_operation TEXT;

ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_reconcile_attempt_count_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_reconcile_attempt_count_check
    CHECK (reconcile_attempt_count >= 0) NOT VALID;
ALTER TABLE source_onboarding_runs
    VALIDATE CONSTRAINT source_onboarding_runs_reconcile_attempt_count_check;

-- Both reconciler lanes use this index.  `reconciled_at IS NULL` rows are
-- at-completion retries; non-NULL rows are periodic retries.  The status guard
-- excludes failed and mid-reshare runs.
CREATE INDEX IF NOT EXISTS source_onboarding_runs_reconcile_retry_due_idx
    ON source_onboarding_runs (reconcile_next_attempt_at, completed_at)
    WHERE status = 'completed' AND reconcile_next_attempt_at IS NOT NULL;

COMMIT;
