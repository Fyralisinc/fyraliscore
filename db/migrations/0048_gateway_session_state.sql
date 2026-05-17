-- =====================================================================
-- 0048_gateway_session_state.sql
--   Ingestion LLD §1.5 — Discord Gateway session state persisted to DB.
-- =====================================================================
-- Replaces in-memory `session_id` / `last_seq` storage (Phase 2.1
-- risk #3 fix). UPSERT on every dispatched frame so a pod restart can
-- RESUME instead of re-IDENTIFY-ing and losing the message gap.
--
-- The actual leader lease lives in Redis (LLD §13). The columns
-- `leader_lease_holder` and `leader_lease_expires_at` are
-- INFORMATIONAL — they exist for diagnostics ("which pod thinks it's
-- the leader right now") and for the operator who must debug a split
-- brain. The Redis lease is the authority.
--
-- Single-shard v1 deployment: `shard_id = 0`. When/if multi-shard
-- ships, one row per shard. The UNIQUE (application_id, shard_id)
-- carries the conflict target for ON CONFLICT UPSERT.
--
-- No RLS — Discord Gateway is app-level, not per-tenant.
--
-- Constitution alignment:
--   §I — per-feature substrate for Discord realtime durability.
--   §II — additive; idempotent.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS gateway_session_state (
    id                      UUID         PRIMARY KEY,         -- uuid7() app-side
    -- Single row per (application_id, shard_id). For v1 shard_id = 0.
    shard_id                INTEGER      NOT NULL DEFAULT 0,
    application_id          TEXT         NOT NULL,            -- Discord app id
    session_id              TEXT,                             -- last seen Discord session
    resume_gateway_url      TEXT,                             -- last seen RESUME URL
    last_seq                BIGINT,                           -- last seen sequence number
    heartbeat_interval_ms   INTEGER,
    last_heartbeat_ack_at   TIMESTAMPTZ,
    last_dispatched_at      TIMESTAMPTZ,
    leader_lease_holder     TEXT,                             -- pod identifier (informational)
    leader_lease_expires_at TIMESTAMPTZ,                      -- Redis-lock mirror (informational)
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (application_id, shard_id)
);

CREATE INDEX IF NOT EXISTS gateway_session_state_active_idx
    ON gateway_session_state (application_id, shard_id);

-- No RLS — Discord Gateway is app-level, not per-tenant.

COMMIT;
