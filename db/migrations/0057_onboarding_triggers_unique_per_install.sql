-- =====================================================================
-- 0057_onboarding_triggers_unique_per_install.sql
--   F4 OAuth retrofit (X1): idempotency for callback-driven trigger writes.
-- =====================================================================
-- Per [05-lld-amendments.md A20]: each OAuth callback (Gmail / Slack /
-- GitHub / Discord) writes its onboarding_triggers row atomically with
-- the install row insert. The retry path (user re-completes the OAuth
-- flow, network retransmit, browser refresh) must produce at most one
-- trigger row per (tenant, source, install_row) tuple.
--
-- Schema reality from 0047: trigger rows reference EITHER
-- `installation_row_id` (Slack/GitHub/Discord, pointing at
-- provider_installations.id) OR `gmail_installation_id` (Gmail,
-- pointing at gmail_installations.id) — mutually exclusive per the
-- 0047 header. A single multi-column UNIQUE would have both NULLs in
-- one row (each NULL counts as distinct in Postgres), so we use two
-- partial unique indexes — one per id column — each guarded by a
-- "not-null" predicate so the index only fires on rows that actually
-- carry the corresponding install reference.
--
-- The combination is what the callback's ON CONFLICT DO NOTHING
-- references:
--   - Non-Gmail callbacks:
--       INSERT ... ON CONFLICT (tenant_id, source, installation_row_id)
--                  WHERE installation_row_id IS NOT NULL
--                  DO NOTHING
--   - Gmail callback:
--       INSERT ... ON CONFLICT (tenant_id, source, gmail_installation_id)
--                  WHERE gmail_installation_id IS NOT NULL
--                  DO NOTHING
--
-- Constitution alignment:
--   §I — substrate; F4 retrofit is load-bearing for first-customer cutover.
--   §II — additive; the indexes are CREATE INDEX IF NOT EXISTS and the
--         WHERE-guards mean they touch no existing rows on creation
--         (none currently carry trigger rows in production per audit).
-- =====================================================================

BEGIN;

-- Non-Gmail sources (slack, github, discord): trigger references
-- provider_installations.id via installation_row_id.
CREATE UNIQUE INDEX IF NOT EXISTS
    onboarding_triggers_unique_per_provider_install_idx
    ON onboarding_triggers (tenant_id, source, installation_row_id)
    WHERE installation_row_id IS NOT NULL;

-- Gmail: trigger references gmail_installations.id via gmail_installation_id.
CREATE UNIQUE INDEX IF NOT EXISTS
    onboarding_triggers_unique_per_gmail_install_idx
    ON onboarding_triggers (tenant_id, source, gmail_installation_id)
    WHERE gmail_installation_id IS NOT NULL;

COMMIT;
