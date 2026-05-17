-- =====================================================================
-- 0050_tenant_flags.sql
--   Ingestion LLD §1.7 — cutover feature flag table.
-- =====================================================================
-- Generic per-(tenant, flag) toggle store. First user is the
-- ingestion cutover flag `ingestion.kafka_path_enabled` (LLD §11)
-- which gates the inline-vs-Kafka path during the M5/M6 ramp.
-- Default missing → false (inline path).
--
-- Lives in its own table (not `tenants`) to avoid bloating the
-- canonical tenant row with a column that will be removed
-- post-migration. Multiple flags share the table.
--
-- No RLS — flag reads run with service role; tenant-scoped code
-- doesn't query this table directly.
--
-- Constitution alignment:
--   §I — substrate for cross-cutting cutover gating; not per-feature
--        but bounded to ingestion's M5/M6 needs in this milestone.
--   §II — additive; idempotent.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS tenant_flags (
    tenant_id   UUID         NOT NULL REFERENCES tenants(id),
    flag_name   TEXT         NOT NULL,
    flag_value  BOOLEAN      NOT NULL DEFAULT false,
    set_by      TEXT,                                 -- operator id or 'auto:circuit_breaker'
    set_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    note        TEXT,
    PRIMARY KEY (tenant_id, flag_name)
);

-- No RLS — service-level table. See header.

COMMIT;
