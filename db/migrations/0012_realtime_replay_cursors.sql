-- =====================================================================
-- 0012_realtime_replay_cursors.sql — Realtime service replay bookmarks
-- =====================================================================
-- Wave 4-D owns the Realtime WebSocket service (BUILD-PLAN §5 Prompt
-- 4.D). Clients may send `{"action": "replay", "since_sequence_num": X}`
-- to resume a torn stream; the dispatcher replays observations with
-- `sequence_num > X` and can optionally persist the last delivered
-- cursor here for durable resumption.
--
-- Design choices (documented in BUILD-LOG Wave 4-D entry):
--   - Composite PK `(tenant_id, actor_id, subscription_id)`. `subscription_id`
--     is client-chosen; tenant+actor are derived from the bearer token.
--   - Cursors are best-effort. Wave 4's client state is primarily in-memory
--     in the dispatcher; this table exists so cold-restart can reconstruct
--     a known-good cursor per (tenant, actor, subscription). Not load-bearing
--     for correctness — `replay` works with or without a persisted cursor.
--   - `last_ack_at` is set whenever the server stamps a new delivered cursor;
--     a daily maintenance job prunes rows whose `last_ack_at` is older than
--     30 days so abandoned subscriptions don't accumulate.
--
-- Partially addresses BUILD-PLAN §0.6 "schema drift prevention" — no new
-- columns on foundation tables; this is an operational sidecar.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS realtime_replay_cursors (
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL,
  subscription_id UUID NOT NULL,
  last_delivered_sequence_num BIGINT NOT NULL,
  last_ack_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, actor_id, subscription_id)
);

-- Secondary index: let the daily pruner scan by last_ack_at without
-- a full table scan once this grows. Tenant + time gives us a cheap
-- per-tenant stale lookup too.
CREATE INDEX IF NOT EXISTS realtime_replay_cursors_stale_idx
  ON realtime_replay_cursors (tenant_id, last_ack_at);

COMMIT;
