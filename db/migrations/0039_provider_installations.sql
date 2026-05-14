-- =====================================================================
-- 0039_provider_installations.sql — webhook tenant-resolution registry
-- =====================================================================
-- Maps a provider-native installation identifier (Slack team_id,
-- GitHub installation.id, Linear organizationId, Stripe account,
-- Discord guild_id / application_id) to a Company OS tenant.
--
-- Consumed by services/webhooks/tenant_resolver.py at the webhook
-- ingress edge: incoming requests carry no Bearer token, so we cannot
-- read the tenant from the auth path. The (provider, installation_id)
-- pair is the only stable identifier the request carries; this table
-- gives it a meaning.
--
-- Disabled rows are externally indistinguishable from missing rows
-- (FR-005, SC-003) — both produce HTTP 401, never 404. The resolver
-- SQL filters `enabled = true`, so a disabled row is invisible to the
-- hot path.
--
-- This is a per-feature side table for a cross-cutting concern
-- (tenant routing); it is NOT a new substrate foundation
-- (Constitution §I). It IS tenant-scoped, so the §III triad applies:
-- tenant_id FK, RLS, tenant-prefixed index.
--
-- Idempotent: re-running this file is a no-op against any DB where
-- the objects already exist (Constitution §II.2).
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS provider_installations (
    id              UUID         PRIMARY KEY,                      -- uuid7() allocated app-side
    tenant_id       UUID         NOT NULL
                                 REFERENCES tenants(id)
                                 DEFERRABLE INITIALLY IMMEDIATE,
    provider        TEXT         NOT NULL,
    installation_id TEXT         NOT NULL,
    secret_ref      TEXT,
    enabled         BOOLEAN      NOT NULL DEFAULT true,
    installed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, installation_id)
);

-- Tenant-prefixed index for the admin enumeration path
-- ("list installations belonging to tenant X"). The UNIQUE
-- (provider, installation_id) already covers the resolver lookup.
CREATE INDEX IF NOT EXISTS idx_provider_installations_tenant_provider
    ON provider_installations (tenant_id, provider);

-- RLS: same pattern as migration 0036.
ALTER TABLE provider_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_installations FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON provider_installations;
CREATE POLICY tenant_isolation ON provider_installations
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
