-- =====================================================================
-- 0052_embedding_backlog_state.sql
--   M3.3 cursor persistence for the backlog embedding service.
-- =====================================================================
-- One row per service instance (`instance_name`); the row carries the
-- (ingested_at, observation_id) tuple at which the last batch's
-- scan stopped. On restart the service reads this row and resumes
-- the scan with `WHERE (ingested_at, id) > (cursor_ingested_at,
-- cursor_id)`.
--
-- Why a table (vs. a Redis key):
--   The service is already a Path A consumer (asyncpg pool for the
--   observation UPDATEs). Adding a Redis dependency just for cursor
--   state would expand the failure surface and split durability —
--   the cursor and the UPDATE could disagree if Redis is restarted
--   between persists. A Postgres table is the same durability
--   guarantee as the UPDATE itself, with no extra infrastructure.
--
-- Cursor reset semantics:
--   When the scan reaches the end (no rows with embedding_pending=
--   TRUE past the cursor), the service NULLs both cursor columns
--   so the next iteration starts from the beginning of the table.
--   This catches new arrivals with ingested_at < the previous
--   cursor. The reset is the same shape as the LLD §12.1
--   "re-SELECT each batch" pattern, with the cursor providing a
--   linear-scan optimisation between resets.
--
-- Constitution alignment:
--   §I — bounded to the backlog service.
--   §II — additive; idempotent CREATE.
--   §III — no RLS (service runs as a privileged operator role; the
--          cursor itself has no tenant data).
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS embedding_backlog_state (
    instance_name        TEXT         PRIMARY KEY,
    -- NULL = "scan from the beginning". The pair is the strict
    -- lower bound (`> cursor`, not `>=`).
    cursor_ingested_at   TIMESTAMPTZ,
    cursor_id            UUID,
    -- Diagnostics. Updated on every persist; lets an operator
    -- `SELECT updated_at` to gauge whether the service is alive.
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- Both NULL or both non-NULL.
    CHECK (
        (cursor_ingested_at IS NULL AND cursor_id IS NULL)
        OR (cursor_ingested_at IS NOT NULL AND cursor_id IS NOT NULL)
    )
);

COMMIT;
