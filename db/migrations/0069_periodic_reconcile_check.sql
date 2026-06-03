-- =====================================================================
-- 0069_periodic_reconcile_check.sql
--   Periodic re-reconciliation watermark for source_onboarding_runs.
-- =====================================================================
-- The M6.2b Reconciler runs a per-source gap check exactly ONCE, at
-- end-of-backfill (driven by the `source_shards_completed` signal).
-- After that one pass stamps `reconciled_at`, nothing re-checks the
-- (run, source) for gaps again. For github/slack/discord there is no
-- durable live watermark (unlike Gmail's history_id), so a webhook /
-- gateway event missed AFTER onboarding is never recovered. The
-- PeriodicReconciler service closes that gap by re-running the same
-- per-source reconciler on a schedule for already-reconciled runs.
--
-- This adds the one column that service needs:
--
--   last_reconcile_check_at TIMESTAMPTZ NULLABLE — the wall-clock of
--     the most recent periodic gap check for this (run, source). The
--     PeriodicReconciler selects rows whose check is older than a
--     configurable min-age (default 6h) — oldest first — and stamps
--     this column every pass (clean or re-share). NULL means "never
--     periodically checked" and sorts first (NULLS FIRST), so freshly
--     reconciled runs enter the rotation immediately.
--
--     Distinct from `reconciled_at` (the one-shot at-completion clean
--     stamp, set once and never cleared) — this column advances on
--     every periodic pass and is the rate-control + round-robin key.
--
-- Constitution alignment:
--   §I — bounded to the periodic-reconcile concern.
--   §II — additive; idempotent ADD COLUMN / CREATE INDEX.
--   §III — no RLS changes; inherits the parent table's policies.
-- =====================================================================

BEGIN;

ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS last_reconcile_check_at TIMESTAMPTZ;

-- Eligibility scan: the PeriodicReconciler claims reconciled,
-- non-re-sharing runs (status='completed' AND reconciled_at IS NOT
-- NULL), oldest-checked first. The partial index keeps the scan cheap
-- as the run table grows — only steady-state reconciled rows are
-- indexed, and the index order matches the ORDER BY
-- (last_reconcile_check_at NULLS FIRST).
CREATE INDEX IF NOT EXISTS source_onboarding_runs_periodic_recheck_idx
    ON source_onboarding_runs (last_reconcile_check_at NULLS FIRST)
    WHERE status = 'completed' AND reconciled_at IS NOT NULL;

COMMIT;
