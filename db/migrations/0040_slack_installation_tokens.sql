-- =====================================================================
-- 0040_slack_installation_tokens.sql — secret store + OAuth state ledger
-- =====================================================================
-- Two NEW tenant-scoped tables, shipped together because both are
-- prerequisites for the Slack OAuth install flow (IN-08):
--
--   1. encrypted_secrets — generic row store backing the
--      lib/shared/secrets envelope-encryption store. provider_installations
--      .secret_ref will resolve to encrypted_secrets.id (a uuid7). The
--      table is intentionally provider-agnostic so IN-09 (GitHub),
--      IN-11 (Stripe), etc. reuse it without additional DDL.
--
--   2. oauth_install_states — single-use nonce ledger for the OAuth
--      callback state token. The callback consumes the row atomically
--      and rejects any state token whose nonce is missing, already
--      consumed, or past expires_at. Stateless HMAC + expiry alone
--      would surface replays as Slack 5xx (code-already-used); the
--      state ledger gives a state-token-shaped 4xx instead.
--
-- Constitution alignment:
--   §I  — both tables are per-feature side stores for cross-cutting
--         concerns (credentials / auth flow). NOT new Foundations.
--   §II — additive (CREATE … IF NOT EXISTS), idempotent.
--   §III — both tables are tenant-scoped: tenant_id FK with
--         DEFERRABLE INITIALLY IMMEDIATE, ENABLE+FORCE RLS with the
--         tenant_isolation policy (migration 0036 pattern), and a
--         tenant-prefixed index on the lookup predicate.
--   §VII — primary keys are uuid7() allocated app-side.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 1. encrypted_secrets — envelope-encrypted row store
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS encrypted_secrets (
    id          UUID         PRIMARY KEY,                       -- uuid7() app-side
    tenant_id   UUID         NOT NULL
                              REFERENCES tenants(id)
                              DEFERRABLE INITIALLY IMMEDIATE,
    label       TEXT         NOT NULL,                          -- e.g. "slack_bot_token:T123"
    ciphertext  BYTEA        NOT NULL,                          -- Fernet token bytes
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    rotated_at  TIMESTAMPTZ
);

-- Tenant-prefixed index: every lookup carries (tenant_id, id), so
-- this index is defense-in-depth on top of the PK lookup. Matches
-- §III "tenant-prefixed indexes are non-negotiable" phrasing.
CREATE INDEX IF NOT EXISTS idx_encrypted_secrets_tenant
    ON encrypted_secrets (tenant_id, id);

ALTER TABLE encrypted_secrets ENABLE ROW LEVEL SECURITY;
ALTER TABLE encrypted_secrets FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON encrypted_secrets;
CREATE POLICY tenant_isolation ON encrypted_secrets
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

-- ---------------------------------------------------------------------
-- 2. oauth_install_states — single-use nonce ledger
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS oauth_install_states (
    id           UUID         PRIMARY KEY,                      -- uuid7() app-side
    tenant_id    UUID         NOT NULL
                               REFERENCES tenants(id)
                               DEFERRABLE INITIALLY IMMEDIATE,
    nonce        TEXT         NOT NULL UNIQUE,                  -- secrets.token_urlsafe(32); globally unique
    provider     TEXT         NOT NULL,                         -- 'slack' at MVP
    expires_at   TIMESTAMPTZ  NOT NULL,
    consumed_at  TIMESTAMPTZ,                                   -- NULL until first callback consumes
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_oauth_install_states_tenant_expires
    ON oauth_install_states (tenant_id, expires_at);

ALTER TABLE oauth_install_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE oauth_install_states FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON oauth_install_states;
CREATE POLICY tenant_isolation ON oauth_install_states
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
