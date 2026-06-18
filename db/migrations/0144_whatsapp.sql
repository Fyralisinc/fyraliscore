-- 0144_whatsapp.sql
--   WhatsApp (WhatsApp Business Platform / Cloud API) — LIVE ingestion only.
--
-- Phase 1 of the WhatsApp source: real-time inbound messages + delivery-status
-- callbacks delivered by Meta as HTTPS webhooks. Backfill (Coexistence history
-- webhook / BSP bulk export / chat-export import) is a deferred later phase and
-- is intentionally NOT wired here — so this migration does NOT touch the M6
-- backfill source-CHECK constraints (source_onboarding_runs / onboarding_shards
-- / ingestion_failures / onboarding_triggers) nor the Kafka SourceLiteral. Live
-- ingestion runs through the inline `ingest()` path, and `observations.source_channel`
-- is free text ("whatsapp:message" / "whatsapp:status"), so no enum widening is
-- required for Phase 1.
--
-- whatsapp_installations — one row per connected business phone number
--   (Meta `phone_number_id`). This is a ROUTING + CREDENTIAL table: the webhook
--   receiver resolves the tenant from the inbound payload's
--   `entry[].changes[].value.metadata.phone_number_id`, then uses the row's
--   `app_secret` to verify Meta's `X-Hub-Signature-256` HMAC.
--
-- RLS: intentionally NOT enabled. Like `provider_installations` this is a
--   webhook resolver table, but unlike it the lookup key here is the globally
--   unique `phone_number_id` and the receiver must read the row BEFORE any
--   tenant context exists (it needs the row to learn which tenant + which secret
--   to verify against). Mirrors the no-RLS posture of infra/routing tables such
--   as `workflow_states` / `writer_poison_attempts`.
--
-- SECURITY NOTE (dev-grade): `app_secret` / `access_token` / `verify_token` are
--   stored as plaintext columns here to keep the Phase-1 live demo self-contained
--   (give creds → see live ingestion). PRODUCTION HARDENING (TODO): move these
--   behind the envelope-encrypted secret store via a `secret_ref` column, exactly
--   like `provider_installations.secret_ref` resolved by services/app/webhooks/secrets.py.

BEGIN;

CREATE TABLE IF NOT EXISTS whatsapp_installations (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id             UUID        NOT NULL
                                      REFERENCES tenants (id) ON DELETE CASCADE,
    -- Meta identifiers.
    phone_number_id       TEXT        NOT NULL UNIQUE,   -- webhook routing key
    waba_id               TEXT,                          -- WhatsApp Business Account id
    display_phone_number  TEXT,                          -- human-readable +1 555…
    -- Credentials (see SECURITY NOTE above — dev-grade plaintext for Phase 1).
    app_secret            TEXT,        -- Meta App Secret for X-Hub-Signature-256 HMAC
    verify_token          TEXT,        -- webhook GET subscribe-handshake token
    access_token          TEXT,        -- Graph API token (media download / future backfill)
    enabled               BOOLEAN     NOT NULL DEFAULT true,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS whatsapp_installations_tenant_idx
    ON whatsapp_installations (tenant_id);

COMMIT;
