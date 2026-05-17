-- =====================================================================
-- 0047_onboarding_triggers_outbox.sql
--   Ingestion LLD §1.4 — OAuth transactional outbox.
-- =====================================================================
-- Every OAuth callback writes its install row, audit row, and a row
-- here in a SINGLE transaction. The Temporal Schedule poller workflow
-- consumes unconsumed rows every 5s (LLD §2.2) using
-- `FOR UPDATE SKIP LOCKED` for multi-pod safety.
--
-- The pattern resolves Phase 2.1 Q E1: today's OAuth callbacks run
-- install UPSERT + audit + (no outbox) as three separate transactions,
-- so a crash between install and trigger-publish silently drops the
-- onboarding trigger. The outbox makes the install <-> trigger pair
-- atomic.
--
-- Deliberate departures from the per-feature triad:
--   - No RLS: the poller runs as a service and reads ALL tenants'
--     rows. RLS would force a service-role bypass on every read,
--     which is more friction than the policy buys.
--   - `installation_row_id` and `gmail_installation_id` are mutually
--     exclusive (Gmail uses its own table; Slack/GH/Discord use
--     `provider_installations`). Enforced as application invariant
--     rather than a CHECK to avoid coupling this migration to both
--     tables' presence ordering.
-- Constitution alignment:
--   §I — substrate for the cutover; bounded to OAuth-to-workflow handoff.
--   §II — additive; idempotent.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS onboarding_triggers (
    id                       UUID         PRIMARY KEY,        -- uuid7() app-side
    tenant_id                UUID         NOT NULL
                                          REFERENCES tenants(id),
    source                   TEXT         NOT NULL
                                          CHECK (source IN ('slack','github','discord','gmail')),
    trigger_kind             TEXT         NOT NULL
                                          CHECK (trigger_kind IN ('install','reinstall','manual_replay')),
    installation_row_id      UUID,                            -- nullable for Gmail
    gmail_installation_id    UUID,                            -- nullable for Slack/GH/Discord
    payload                  JSONB        NOT NULL DEFAULT '{}'::jsonb,
    consumed_at              TIMESTAMPTZ,                     -- set by poller when workflow started
    consumed_by_workflow_id  TEXT,                            -- workflow_id the poller created
    consume_attempts         INTEGER      NOT NULL DEFAULT 0,
    last_attempt_at          TIMESTAMPTZ,
    last_error               TEXT,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Workload-shaped index: the poller's hot query is
--   WHERE consumed_at IS NULL ORDER BY created_at LIMIT 100
-- The partial index restricts the b-tree to unconsumed rows only.
CREATE INDEX IF NOT EXISTS onboarding_triggers_unconsumed_idx
    ON onboarding_triggers (created_at)
    WHERE consumed_at IS NULL;

-- Lookups by tenant for ops + admin dashboards.
CREATE INDEX IF NOT EXISTS onboarding_triggers_tenant_source_idx
    ON onboarding_triggers (tenant_id, source, created_at DESC);

-- No RLS — service-level table. See header.

COMMIT;
