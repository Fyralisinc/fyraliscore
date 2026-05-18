-- =====================================================================
-- 0053_circuit_breaker_state.sql
--   M5.1 — Ingestion cutover circuit breaker state.
-- =====================================================================
-- Per-tenant breach-window tracking for the cutover circuit breaker
-- (LLD §11.2). One row per (instance_name, tenant_id); the row carries:
--
--   • consecutive_breach_ticks — how many consecutive scans have
--     shown the tenant's partition exceeding the lag threshold.
--   • tripped — once flipped TRUE, this tenant's kafka_path_enabled
--     flag was forcibly set FALSE by the breaker; auto-recovery is
--     intentionally DISABLED (see circuit_breaker.py docstring) so
--     this row stays tripped until an operator manually re-enables.
--   • tripped_at — observability; when the trip fired.
--   • last_tick_at — for stale-state GC (entries not updated in >1h
--     can be safely dropped; their tenant has gone silent).
--
-- Why a table (vs. a Redis key):
--   Same reasoning as 0052 (M3.3 embedding backlog cursor). The
--   breaker is already a Path A consumer (asyncpg pool for tenant
--   flag UPSERTs via TenantFlags.set_bool). Putting breach-window
--   state in Postgres keeps durability in one place — the cursor
--   advance and the flag flip happen against the same DB, atomic
--   per-tenant in the breaker's update path. A separate Redis
--   key would split durability and add a failure mode where the
--   counter says "tripped" but the flag never flipped (or vice
--   versa) after a Redis restart.
--
-- Instance scoping:
--   `instance_name` defaults to 'default' but is configurable so
--   parallel breakers (e.g. one per region in a future deploy)
--   don't collide. Matches the embedding_backlog_state pattern.
--
-- Constitution alignment:
--   §I — bounded to the cutover breaker.
--   §II — additive; idempotent CREATE.
--   §III — no RLS (service runs as a privileged operator role; the
--          breach state itself has no tenant-private data).
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS circuit_breaker_state (
    instance_name              TEXT         NOT NULL,
    tenant_id                  UUID         NOT NULL REFERENCES tenants(id),
    consecutive_breach_ticks   INTEGER      NOT NULL DEFAULT 0,
    tripped                    BOOLEAN      NOT NULL DEFAULT FALSE,
    tripped_at                 TIMESTAMPTZ,
    last_tick_at               TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_name, tenant_id),
    -- A tripped row MUST have tripped_at populated (audit invariant).
    CHECK (
        (tripped = FALSE AND tripped_at IS NULL)
        OR (tripped = TRUE AND tripped_at IS NOT NULL)
    )
);

-- Operator-facing index: find recently tripped tenants quickly.
CREATE INDEX IF NOT EXISTS circuit_breaker_state_tripped_at_idx
    ON circuit_breaker_state (tripped_at DESC)
    WHERE tripped = TRUE;

COMMIT;
