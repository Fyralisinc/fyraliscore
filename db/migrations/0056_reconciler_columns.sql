-- =====================================================================
-- 0056_reconciler_columns.sql
--   M6.2b — Reconciler service additions to source_onboarding_runs.
-- =====================================================================
-- Adds two columns to the M6.1-shipped source_onboarding_runs table
-- (migration 0055) for M6.2b's Reconciler service:
--
--   1. reconciled_at TIMESTAMPTZ NULLABLE — operator-visible audit
--      stamp set when the Reconciler completes a CLEAN pass for this
--      (run, source) pair. Distinct from completed_at (which
--      SourceOnboarding stamps when all shards reach a terminal
--      state, BEFORE the Reconciler runs). A row with
--      status='completed' but reconciled_at IS NULL is in the
--      transient state between SourceOnboarding's roll-up and the
--      Reconciler's processing.
--
--   2. reconciliation_pass_count INTEGER NOT NULL DEFAULT 0 — the
--      counter that distinguishes the Nth source_shards_completed
--      emit for this (run, source) pair across re-share cycles.
--      LOAD-BEARING for idempotency: without this, the second
--      source_shards_completed emit (after a re-share + new-shard
--      completion) would collide with the first emit's
--      idempotency_key, emit_signal would silently dedup, and the
--      Reconciler would never see the second event.
--
--      Lifecycle:
--        - Initial: 0 (set by DEFAULT).
--        - SourceOnboarding's first rollup emits with
--          idempotency_key = f"{run_id}:{source}:pass_0".
--        - Reconciler decides re-share: increments to 1 in the same
--          transaction that creates the new shards.
--        - SourceOnboarding's second rollup (after new shards
--          complete) emits with idempotency_key
--          = f"{run_id}:{source}:pass_1". Etc.
--
-- Schema audit decision (M6.2b Phase 1):
--   No other columns are needed. The existing M1-shipped 0045
--   onboarding_shards columns (parent_shard_id, state enum value
--   'reconciliation_resharded') are the re-share-linkage anchors —
--   per A15, these were designed into the LLD §1.2 schema in M1.
--   M6.2b is therefore the SECOND M6-era work-unit shipping minimal
--   schema work, this one being two additive columns. M6.2a was the
--   first (no migration at all per A15).
--
-- Constitution alignment:
--   §I — bounded to M6.2b's Reconciler concerns.
--   §II — additive; idempotent CREATE/ADD.
--   §III — no RLS changes; inherits the M1+M6.1 policies on the
--          parent table.
-- =====================================================================

BEGIN;

ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS reconciled_at TIMESTAMPTZ;

ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS reconciliation_pass_count INTEGER NOT NULL DEFAULT 0;

-- Ops index: "show me runs that completed but the Reconciler hasn't
-- yet stamped reconciled_at" — the transient state worth monitoring
-- under §6.C of the cutover runbook.
CREATE INDEX IF NOT EXISTS source_onboarding_runs_awaiting_reconcile_idx
    ON source_onboarding_runs (completed_at)
    WHERE status = 'completed' AND reconciled_at IS NULL;

COMMIT;
