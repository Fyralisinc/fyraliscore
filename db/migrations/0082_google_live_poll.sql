-- =====================================================================
-- 0082_google_live_poll.sql
--   Near-real-time LIVE poller leasing marks for Google Calendar + Drive.
-- =====================================================================
-- Calendar/Drive were backfill+reconcile only — no continuous live
-- ingestion. This adds a `live_poller` worker per source (the analog of
-- `gmail_history_poller`): a short-cadence loop that leases active
-- calendars/targets whose incremental cursor (`sync_token` /
-- `start_page_token`) is already seeded and drains the delta via the
-- EXISTING fetcher + `ingest()` path.
--
-- Leasing mirrors gmail_mailbox_watches.last_poll_at: a dedicated
-- `last_live_poll_at` claim slot (NOT last_synced_at, which backfill +
-- reconcile also touch) lets concurrent poller replicas use
-- `FOR UPDATE SKIP LOCKED` without fighting the backfill/reconcile writers.
-- `consecutive_live_failures` + `live_last_error` mirror the gmail poller's
-- failure backoff (transition to a paused-equivalent after N failures).
--
-- Constitution alignment:
--   §I  — per-feature substrate for the live-ingestion capability.
--   §II — additive + idempotent (ADD COLUMN IF NOT EXISTS); NULL
--         last_live_poll_at = "never live-polled", the correct default.
--   §III — columns live on the already tenant-scoped + RLS'd per-resource
--         tables (0071 / 0072); no new table, no new policy.
-- =====================================================================

BEGIN;

ALTER TABLE google_calendar_calendars
    ADD COLUMN IF NOT EXISTS last_live_poll_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS consecutive_live_failures INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS live_last_error TEXT;

ALTER TABLE google_drive_targets
    ADD COLUMN IF NOT EXISTS last_live_poll_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS consecutive_live_failures INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS live_last_error TEXT;

-- Leasing scan support: "active, cursor-seeded, due for a live poll".
CREATE INDEX IF NOT EXISTS google_calendar_calendars_live_poll_idx
    ON google_calendar_calendars (last_live_poll_at NULLS FIRST)
    WHERE state = 'active' AND sync_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS google_drive_targets_live_poll_idx
    ON google_drive_targets (last_live_poll_at NULLS FIRST)
    WHERE state = 'active' AND start_page_token IS NOT NULL;

COMMIT;
