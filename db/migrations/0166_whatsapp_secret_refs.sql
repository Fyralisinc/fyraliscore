-- =====================================================================
-- 0166_whatsapp_secret_refs.sql
-- =====================================================================
-- Move WhatsApp credential material onto the shared encrypted secret-store
-- pattern while preserving legacy plaintext columns for a staged rollout.
--
-- New writes should populate *_secret_ref columns and clear the legacy
-- plaintext columns. The legacy columns remain readable until existing
-- installations are migrated and the final destructive cleanup can be
-- scheduled separately.
-- =====================================================================

BEGIN;

ALTER TABLE whatsapp_installations
    ADD COLUMN IF NOT EXISTS app_secret_ref TEXT,
    ADD COLUMN IF NOT EXISTS verify_token_ref TEXT,
    ADD COLUMN IF NOT EXISTS access_token_ref TEXT;

COMMENT ON COLUMN whatsapp_installations.app_secret IS
    'Legacy plaintext Meta app secret. New writes must use app_secret_ref.';
COMMENT ON COLUMN whatsapp_installations.verify_token IS
    'Legacy plaintext webhook verify token. New writes must use verify_token_ref.';
COMMENT ON COLUMN whatsapp_installations.access_token IS
    'Legacy plaintext Graph API token. New writes must use access_token_ref.';
COMMENT ON COLUMN whatsapp_installations.app_secret_ref IS
    'Opaque encrypted_secrets ref for the Meta app secret.';
COMMENT ON COLUMN whatsapp_installations.verify_token_ref IS
    'Opaque encrypted_secrets ref for the webhook verify token.';
COMMENT ON COLUMN whatsapp_installations.access_token_ref IS
    'Opaque encrypted_secrets ref for the Graph API access token.';

COMMIT;
