-- =====================================================================
-- 0083_google_push_channels.sql
--   Native Google push-channel (events.watch / changes.watch) state for
--   Calendar + Drive — the low-latency `gmail_watch` analog.
-- =====================================================================
-- Unlike Gmail (Pub/Sub), Calendar/Drive push directly to a `web_hook`
-- address. `*.watch` returns a channel `{id, resourceId, expiration}`; the
-- ping carries `X-Goog-Channel-ID` + `X-Goog-Channel-Token` and the receiver
-- drains the delta via the same cursor the poller uses. This adds the channel
-- bookkeeping per resource so the watch scheduler can (re)register before a
-- channel expires and the push ingress can resolve + verify an inbound ping.
--
--   watch_channel_id  — the id WE generate + pass to *.watch (lookup key on
--                       the inbound X-Goog-Channel-ID).
--   watch_resource_id — Google's resourceId from the watch response (needed
--                       for channels.stop).
--   watch_token       — the shared secret WE set; echoed in X-Goog-Channel-Token
--                       and constant-time-compared on every push.
--   watch_expiration  — channel TTL from Google; the scheduler renews before it.
--   last_push_at      — last verified push (observability).
--   watch_state       — inactive | active | errored.
--
-- Constitution alignment:
--   §I  — per-feature substrate for the live-push capability.
--   §II — additive + idempotent (ADD COLUMN IF NOT EXISTS); 'inactive' is the
--         correct default for every existing resource (no channel yet).
--   §III — columns live on the already tenant-scoped + RLS'd per-resource
--         tables (0071 / 0072); no new table.
-- =====================================================================

BEGIN;

ALTER TABLE google_calendar_calendars
    ADD COLUMN IF NOT EXISTS watch_channel_id TEXT,
    ADD COLUMN IF NOT EXISTS watch_resource_id TEXT,
    ADD COLUMN IF NOT EXISTS watch_token TEXT,
    ADD COLUMN IF NOT EXISTS watch_expiration TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_push_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS watch_state TEXT NOT NULL DEFAULT 'inactive'
        CHECK (watch_state IN ('inactive', 'active', 'errored'));

ALTER TABLE google_drive_targets
    ADD COLUMN IF NOT EXISTS watch_channel_id TEXT,
    ADD COLUMN IF NOT EXISTS watch_resource_id TEXT,
    ADD COLUMN IF NOT EXISTS watch_token TEXT,
    ADD COLUMN IF NOT EXISTS watch_expiration TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_push_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS watch_state TEXT NOT NULL DEFAULT 'inactive'
        CHECK (watch_state IN ('inactive', 'active', 'errored'));

-- Inbound-push resolution: X-Goog-Channel-ID -> the watched resource. The id
-- we generate is globally unique, so a UNIQUE partial index both enforces that
-- and makes the push lookup a single-row probe.
CREATE UNIQUE INDEX IF NOT EXISTS google_calendar_calendars_watch_channel_idx
    ON google_calendar_calendars (watch_channel_id)
    WHERE watch_channel_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS google_drive_targets_watch_channel_idx
    ON google_drive_targets (watch_channel_id)
    WHERE watch_channel_id IS NOT NULL;

-- Renewal scan support: "active channels nearing expiry" + "seeded resources
-- with no channel yet".
CREATE INDEX IF NOT EXISTS google_calendar_calendars_watch_expiry_idx
    ON google_calendar_calendars (watch_expiration NULLS FIRST)
    WHERE state = 'active' AND sync_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS google_drive_targets_watch_expiry_idx
    ON google_drive_targets (watch_expiration NULLS FIRST)
    WHERE state = 'active' AND start_page_token IS NOT NULL;

COMMIT;
