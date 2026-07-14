-- =====================================================================
-- 0188_slack_app_credentials.sql — tenant-owned Slack app credentials
-- =====================================================================
-- BYOC Slack onboarding creates a customer-owned Slack app from a manifest.
-- The non-secret app identifiers live here; generated app secrets are stored
-- in encrypted_secrets and referenced by *_ref columns.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS slack_app_credentials (
    id                     UUID         PRIMARY KEY,
    tenant_id              UUID         NOT NULL
                                             REFERENCES tenants(id)
                                             DEFERRABLE INITIALLY IMMEDIATE,
    app_id                 TEXT         NOT NULL,
    client_id              TEXT         NOT NULL,
    client_secret_ref      TEXT         NOT NULL,
    signing_secret_ref     TEXT         NOT NULL,
    verification_token_ref TEXT,
    oauth_authorize_url    TEXT,
    manifest_sha256        TEXT,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_used_at           TIMESTAMPTZ,
    disabled_at            TIMESTAMPTZ,
    UNIQUE (tenant_id, app_id)
);

CREATE INDEX IF NOT EXISTS idx_slack_app_credentials_tenant_active
    ON slack_app_credentials (tenant_id, disabled_at, last_used_at DESC, created_at DESC);

ALTER TABLE slack_app_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE slack_app_credentials FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON slack_app_credentials;
CREATE POLICY tenant_isolation ON slack_app_credentials
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
