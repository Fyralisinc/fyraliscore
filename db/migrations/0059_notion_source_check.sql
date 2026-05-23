-- 0059_notion_source_check.sql
--   IN-14 — Notion as a fifth ingestion source.
--
-- The M6 ingestion substrate pins the allowed `source` values with an
-- inline CHECK on FOUR tables, each reading
-- `CHECK (source IN ('slack','github','discord','gmail'))`:
--   - onboarding_shards          (migration 0045)
--   - ingestion_failures         (migration 0046)
--   - onboarding_triggers        (migration 0047)
--   - source_onboarding_runs     (migration 0055)
-- Adding Notion requires widening all four to admit 'notion'. Missing any
-- one breaks a different stage: triggers (OAuth install emits one), runs +
-- shards (backfill planning), failures (DLQ writer).
--
-- This is an ADDITIVE constraint widening, NOT a destructive change
-- (Constitution §II.5): existing rows are a strict subset of the new
-- allowed set, so no row can violate the widened constraint and no
-- staged dual-write/backfill/cutover plan is required.
--
-- Idempotent (§II.2): each constraint is dropped IF EXISTS and re-added
-- with the same auto-generated name Postgres assigns to an inline column
-- CHECK (`<table>_source_check`). Re-running the directory drops the
-- widened constraint and re-creates it — a no-op against an already-
-- migrated DB.
--
-- provider_installations.provider is free TEXT (migration 0039) with no
-- CHECK, so Notion installs need no table change there.

ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion'));
