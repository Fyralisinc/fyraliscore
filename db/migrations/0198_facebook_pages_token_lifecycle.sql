-- =====================================================================
-- 0198_facebook_pages_token_lifecycle.sql
--   Exact-installation Facebook Page token recovery.
--
-- Meta does not expose a refresh grant for Page access tokens.  Fyralis can
-- only re-derive a Page token by calling /me/accounts with a still-valid
-- long-lived User access token.  An expired or missing User token therefore
-- requires the administrator to complete Facebook Login again.
--
-- Existing Page-token refs are intentionally retained.  Legacy rows do not
-- contain the User credential needed for a supported recovery and are marked
-- reauthorization_required instead of being "refreshed" by an invented flow.
-- =====================================================================

BEGIN;

ALTER TABLE facebook_page_installations
    ADD COLUMN IF NOT EXISTS user_access_token_ref TEXT,
    ADD COLUMN IF NOT EXISTS user_token_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS connection_state TEXT NOT NULL
        DEFAULT 'reauthorization_required',
    ADD COLUMN IF NOT EXISTS reauthorization_required_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS page_token_recovery_next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS page_token_recovery_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS page_token_recovery_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS page_recovery_last_error_code TEXT,
    ADD COLUMN IF NOT EXISTS page_recovery_lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS page_token_recovery_lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS page_token_rotated_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'facebook_page_installations'::regclass
           AND conname = 'facebook_page_installations_connection_state_check'
    ) THEN
        ALTER TABLE facebook_page_installations
            ADD CONSTRAINT facebook_page_installations_connection_state_check
            CHECK (
                connection_state IN (
                    'connected',
                    'degraded',
                    'reauthorization_required'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'facebook_page_installations'::regclass
           AND conname = 'facebook_page_installations_recovery_attempts_check'
    ) THEN
        ALTER TABLE facebook_page_installations
            ADD CONSTRAINT facebook_page_installations_recovery_attempts_check
            CHECK (page_token_recovery_attempts >= 0);
    END IF;
END
$$;

-- Migration data is deliberately fail-closed.  The existing Page credential
-- remains referenced and enabled, but no runtime may claim it is recoverable
-- until Facebook Login stores the exact owning User credential and expiry.
UPDATE facebook_page_installations
   SET connection_state = 'reauthorization_required',
       reauthorization_required_at = COALESCE(
           reauthorization_required_at,
           now()
       ),
       page_token_recovery_next_attempt_at = NULL,
       page_recovery_last_error_code =
           COALESCE(
               page_recovery_last_error_code,
               'long_lived_user_token_missing'
           ),
       page_recovery_lease_owner = NULL,
       page_token_recovery_lease_until = NULL
 WHERE user_access_token_ref IS NULL
    OR user_token_expires_at IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'facebook_page_installations'::regclass
           AND conname = 'facebook_page_installations_connected_user_token_check'
    ) THEN
        ALTER TABLE facebook_page_installations
            ADD CONSTRAINT facebook_page_installations_connected_user_token_check
            CHECK (
                connection_state <> 'connected'
                OR (
                    user_access_token_ref IS NOT NULL
                    AND user_token_expires_at IS NOT NULL
                )
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS facebook_page_installations_token_recovery_due_idx
    ON facebook_page_installations (
        page_token_recovery_next_attempt_at,
        tenant_id,
        id
    )
    WHERE enabled = TRUE
      AND connection_state = 'degraded'
      AND page_token_recovery_next_attempt_at IS NOT NULL;

COMMENT ON COLUMN facebook_page_installations.user_access_token_ref IS
    'Encrypted ref for the long-lived User token used only to re-derive this exact Page token through /me/accounts.';
COMMENT ON COLUMN facebook_page_installations.user_token_expires_at IS
    'Provider-returned expiry for the long-lived User token. Expired tokens require Facebook Login; Fyralis never exchanges an expired token.';
COMMENT ON COLUMN facebook_page_installations.connection_state IS
    'Credential lifecycle state. reauthorization_required preserves the prior Page secret but forbids an undocumented refresh attempt.';
COMMENT ON COLUMN facebook_page_installations.page_token_recovery_next_attempt_at IS
    'Durable not-before for exact-installation Page-token re-derivation after a retryable provider failure.';
COMMENT ON COLUMN facebook_page_installations.page_recovery_last_error_code IS
    'Controlled Fyralis error code only; provider payloads and token material are never stored here.';

COMMIT;
