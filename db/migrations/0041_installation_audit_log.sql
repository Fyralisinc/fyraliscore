-- =====================================================================
-- 0041_installation_audit_log.sql — installation lifecycle audit trail
-- =====================================================================
-- Per-installation lifecycle audit table. Records install / uninstall /
-- token_refresh / rejected_collision events for downstream debugging
-- and admin visibility.
--
-- IMPORTANT: this table is DISTINCT from `audit_events` (migration 0030),
-- which records Model state transitions per Constitution §VII. The two
-- chains record different domains; do not collapse them. Per §I,
-- per-feature side audit tables are explicitly permitted.
--
-- Append-only by code discipline: service code MUST NOT issue UPDATE
-- or DELETE against this table. The discipline matches `audit_events`;
-- no DB trigger enforces it (review-gate item).
--
-- Constitution alignment:
--   §I  — side audit table; not a new Foundation.
--   §II — additive (CREATE … IF NOT EXISTS), idempotent.
--   §III — tenant-scoped: tenant_id FK + ENABLE+FORCE RLS +
--         tenant-prefixed index on (tenant_id, created_at DESC).
--   §VII — IDs are uuid7() allocated app-side.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS installation_audit_log (
    id                   UUID         PRIMARY KEY,              -- uuid7() app-side
    tenant_id            UUID         NOT NULL
                                       REFERENCES tenants(id)
                                       DEFERRABLE INITIALLY IMMEDIATE,
    installation_row_id  UUID         REFERENCES provider_installations(id),
    -- NULL allowed: collision-rejected installs don't create a row.
    -- ON DELETE behavior left as default (NO ACTION); audit history
    -- survives even if provider_installations row is ever deleted.
    provider             TEXT         NOT NULL,                 -- 'slack' at MVP
    action               TEXT         NOT NULL
                                       CHECK (action IN (
                                           'install',
                                           'uninstall',
                                           'token_refresh',
                                           'rejected_collision'
                                       )),
    status               TEXT         NOT NULL
                                       CHECK (status IN (
                                           'ok',
                                           'rejected_collision',
                                           'error'
                                       )),
    context              JSONB        NOT NULL DEFAULT '{}'::jsonb,
    -- Free-form structured context. MUST NOT contain plaintext
    -- team_id, secret material, or any value that would let an
    -- attacker correlate audit rows with the workspace identifier.
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Tenant-prefixed index for the admin "history for tenant X" path,
-- ordered newest-first.
CREATE INDEX IF NOT EXISTS idx_installation_audit_log_tenant_created
    ON installation_audit_log (tenant_id, created_at DESC);

-- Partial index for the narrower "history for installation Y" path.
-- `installation_row_id` already implies a tenant via its FK, so the
-- tenant-prefixed index above is sufficient for §III compliance; this
-- partial index is a query-shape optimization.
CREATE INDEX IF NOT EXISTS idx_installation_audit_log_installation
    ON installation_audit_log (installation_row_id)
    WHERE installation_row_id IS NOT NULL;

ALTER TABLE installation_audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE installation_audit_log FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON installation_audit_log;
CREATE POLICY tenant_isolation ON installation_audit_log
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
