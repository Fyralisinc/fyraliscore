-- =====================================================================
-- 0003_actor_sessions.sql — Gateway bearer-token session registry
-- =====================================================================
-- BUILD-PLAN §3 Prompt 2.A: Gateway needs a session table for the
-- `POST /auth/session` endpoint and the bearer-token auth middleware.
-- Not in SCHEMA-LOCK.md S1-S6 (not in spec). Explicitly permitted as
-- a non-foundation addition per BUILD-PLAN §0 non-negotiables and
-- the Wave 2-A prompt.
--
-- One of the three permitted additions (alongside 0004 think_trigger_queue
-- and, later, entity_review_queue in Wave 2-B).
-- Idempotent; immutable once committed. Subsequent changes in 0005_*.sql.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS actor_sessions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL REFERENCES actors(id),
  token_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at TIMESTAMPTZ
);

-- Lookup by actor filtered to live sessions (partial index).
CREATE INDEX IF NOT EXISTS actor_sessions_actor_idx
  ON actor_sessions (actor_id)
  WHERE revoked_at IS NULL;

-- Expiry sweep (partial index).
CREATE INDEX IF NOT EXISTS actor_sessions_expires_idx
  ON actor_sessions (expires_at)
  WHERE revoked_at IS NULL;

COMMIT;
