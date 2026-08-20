-- =====================================================================
-- 0225_figma_oauth.sql
--   OAuth-first Figma connection state.
--
-- The original Figma connector stored one long-lived PAT in ``secret_ref``.
-- OAuth installations use the same encrypted access-token pointer, plus an
-- encrypted refresh-token pointer and a small amount of non-secret grant
-- metadata.  ``oauth_install_states.context`` is deliberately limited to
-- short-lived, non-token callback context; the PKCE verifier itself is stored
-- in ``encrypted_secrets`` and only its opaque ref lives in this JSON document.
-- =====================================================================

BEGIN;

-- OAuth state rows are shared by the source OAuth callbacks.  Adding context
-- keeps the existing nonce/HMAC/single-use semantics while allowing Figma to
-- bind a callback to its selected file keys and encrypted PKCE verifier.
ALTER TABLE oauth_install_states
  ADD COLUMN IF NOT EXISTS context JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Figma retains ``secret_ref`` as the encrypted access-token ref so existing
-- fetchers continue to have one credential pointer.  The extra columns only
-- describe OAuth grants; PAT installs continue to work with auth_kind='pat'.
ALTER TABLE figma_installations
  ADD COLUMN IF NOT EXISTS auth_kind TEXT NOT NULL DEFAULT 'pat',
  ADD COLUMN IF NOT EXISTS refresh_secret_ref TEXT,
  ADD COLUMN IF NOT EXISTS token_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS oauth_user_id TEXT,
  ADD COLUMN IF NOT EXISTS granted_scopes TEXT[] NOT NULL DEFAULT '{}'::text[],
  ADD COLUMN IF NOT EXISTS connection_state TEXT NOT NULL DEFAULT 'connected',
  ADD COLUMN IF NOT EXISTS last_error TEXT,
  ADD COLUMN IF NOT EXISTS connected_at TIMESTAMPTZ;

ALTER TABLE figma_installations
  DROP CONSTRAINT IF EXISTS figma_installations_auth_kind_check;
ALTER TABLE figma_installations
  ADD CONSTRAINT figma_installations_auth_kind_check
  CHECK (auth_kind IN ('pat', 'oauth')) NOT VALID;

ALTER TABLE figma_installations
  DROP CONSTRAINT IF EXISTS figma_installations_connection_state_check;
ALTER TABLE figma_installations
  ADD CONSTRAINT figma_installations_connection_state_check
  CHECK (connection_state IN (
    'pending', 'connected', 'degraded', 'reauthorization_required',
    'disconnected', 'error'
  )) NOT VALID;

CREATE INDEX IF NOT EXISTS figma_installations_tenant_connection_state_idx
  ON figma_installations (tenant_id, connection_state)
  WHERE disabled_at IS NULL;

CREATE INDEX IF NOT EXISTS figma_installations_oauth_user_idx
  ON figma_installations (oauth_user_id)
  WHERE oauth_user_id IS NOT NULL AND disabled_at IS NULL;

-- A Figma OAuth grant is a user credential.  Do not let the same grant holder
-- silently bind to two Fyralis tenants; the callback maps this deterministic
-- constraint violation to an opaque installation_collision result.
CREATE UNIQUE INDEX IF NOT EXISTS figma_installations_active_oauth_user_unique
  ON figma_installations (oauth_user_id)
  WHERE auth_kind = 'oauth'
    AND oauth_user_id IS NOT NULL
    AND disabled_at IS NULL;

COMMENT ON COLUMN oauth_install_states.context IS
  'Short-lived provider callback context. Never stores raw OAuth tokens or PKCE verifier plaintext.';
COMMENT ON COLUMN figma_installations.secret_ref IS
  'Opaque encrypted access-token ref; PAT or OAuth Bearer token according to auth_kind.';
COMMENT ON COLUMN figma_installations.refresh_secret_ref IS
  'Opaque encrypted Figma OAuth refresh-token ref.';

COMMIT;
