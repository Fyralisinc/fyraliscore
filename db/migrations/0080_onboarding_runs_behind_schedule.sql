-- =====================================================================
-- 0080_onboarding_runs_behind_schedule.sql
--   Fire-once guard for the `tenant.onboarding.behind_schedule` progress
--   event (ingestion LLD §6 Bridge contract).
-- =====================================================================
-- The `behind_schedule` event is an ops-only signal that fires once per
-- run, `FEELS_MONITOR_BEHIND_SCHEDULE_SEC` (default 15 min) after a run
-- starts if no source has reached `feels_onboarded`. The FeelsOnboarded
-- monitor is the natural emitter: it already polls `onboarding_runs` for
-- the `feels_onboarded` milestone, so it observes `started_at` +
-- `feels_onboarded_at` on the same scan.
--
-- "Once per run" needs a durable claim slot, exactly like the existing
-- `feels_onboarded_at` column: the monitor does a guarded
-- `UPDATE ... SET behind_schedule_emitted_at = now()
--    WHERE id = $1 AND behind_schedule_emitted_at IS NULL`
-- and only publishes if the UPDATE affected a row. Concurrent monitor
-- instances racing on the same run therefore publish exactly once
-- (claim-via-UPDATE), mirroring `feels_onboarded_at`'s single-fire
-- contract (LLD §2.6).
--
-- Constitution alignment:
--   §I  — per-feature substrate for the backfill/onboarding capability;
--         bounded to the ingestion progress contract.
--   §II — additive + idempotent (ADD COLUMN IF NOT EXISTS); no backfill
--         needed (NULL = "behind_schedule not yet emitted", the correct
--         default for every existing run).
--   §III — column lives on the already tenant-scoped `onboarding_runs`
--         (RLS + FK from 0056); no new table, no new policy.
-- =====================================================================

BEGIN;

ALTER TABLE onboarding_runs
    ADD COLUMN IF NOT EXISTS behind_schedule_emitted_at TIMESTAMPTZ;

COMMIT;
