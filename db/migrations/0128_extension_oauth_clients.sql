-- =====================================================================
-- 0128_extension_oauth_clients.sql — extension identity (OAuth2 clients)
-- =====================================================================
-- ADR-0004 DP1.4 / roadmap M1. Each extension authenticates to Fyralis as a
-- registered OAuth2 client (client_credentials grant → short-lived bearer JWT).
-- This is the IDENTITY of the extension, distinct from the per-(tenant,extension)
-- capability GRANT in `extension_grants` (0127): the token says "who is calling",
-- the grant says "what they may do, for which tenant".
--
--   * `client_secret_hash` is a PBKDF2-SHA256 verifier ("pbkdf2$iter$salt$hash") —
--     the plaintext secret is shown once at registration and never stored.
--   * `environment` separates sandbox vs production credentials for one extension.
--   * Rotation = update the hash + bump `rotated_at`; revoke = set `revoked_at`.
--
-- Host-managed table (no tenant_id, no RLS): extension identity is cross-tenant.
-- Not granted to fyralis_ext_readonly — extensions never read the client store.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS extension_oauth_clients (
  client_id           TEXT PRIMARY KEY,            -- public id, e.g. "ext_ab12cd34..."
  extension_id        TEXT NOT NULL,               -- the manifest id this client acts as
  environment         TEXT NOT NULL DEFAULT 'production'
                      CHECK (environment IN ('sandbox', 'production')),
  client_secret_hash  TEXT NOT NULL,               -- "pbkdf2$<iter>$<salt_hex>$<hash_hex>"
  display_name        TEXT,
  callback_url        TEXT,                         -- for webhook egress (M3) + domain verification
  created_by          TEXT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  rotated_at          TIMESTAMPTZ,
  revoked_at          TIMESTAMPTZ,
  last_token_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS extension_oauth_clients_ext_idx
  ON extension_oauth_clients (extension_id)
  WHERE revoked_at IS NULL;

COMMIT;
