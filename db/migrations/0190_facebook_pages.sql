-- =====================================================================
-- 0190_facebook_pages.sql
--   Facebook Page / Messenger messages — OAuth, live webhooks, and
--   all history available through Meta Graph API pagination.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS facebook_page_installations (
    id                         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                  UUID        NOT NULL
                                                 REFERENCES tenants (id) ON DELETE CASCADE,
    page_id                    TEXT        NOT NULL UNIQUE,
    page_name                  TEXT,
    page_access_token_ref      TEXT        NOT NULL,
    app_secret_ref             TEXT,
    verify_token_ref           TEXT,
    granted_scopes             TEXT[]      NOT NULL DEFAULT '{}'::text[],
    subscribed_fields          TEXT[]      NOT NULL DEFAULT '{}'::text[],
    webhook_subscribed_at      TIMESTAMPTZ,
    enabled                    BOOLEAN     NOT NULL DEFAULT true,
    oldest_message_at          TIMESTAMPTZ,
    backfill_exhausted_at      TIMESTAMPTZ,
    backfill_exhausted_reason  TEXT,
    conversation_count         INTEGER     NOT NULL DEFAULT 0 CHECK (conversation_count >= 0),
    message_count              INTEGER     NOT NULL DEFAULT 0 CHECK (message_count >= 0),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS facebook_page_installations_tenant_idx
    ON facebook_page_installations (tenant_id);

CREATE TABLE IF NOT EXISTS facebook_page_conversations (
    id                            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    facebook_page_installation_id UUID        NOT NULL
                                                    REFERENCES facebook_page_installations (id)
                                                    ON DELETE CASCADE,
    tenant_id                     UUID        NOT NULL
                                                    REFERENCES tenants (id) ON DELETE CASCADE,
    page_id                       TEXT        NOT NULL,
    conversation_id               TEXT        NOT NULL,
    participant_ids               TEXT[]      NOT NULL DEFAULT '{}'::text[],
    updated_time                  TIMESTAMPTZ,
    oldest_message_at             TIMESTAMPTZ,
    newest_message_at             TIMESTAMPTZ,
    message_count                 INTEGER     NOT NULL DEFAULT 0 CHECK (message_count >= 0),
    backfill_cursor               TEXT,
    state                         TEXT        NOT NULL DEFAULT 'active'
                                                    CHECK (state IN ('active', 'exhausted', 'skipped', 'disabled')),
    exhausted_at                  TIMESTAMPTZ,
    exhausted_reason              TEXT,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (facebook_page_installation_id, conversation_id)
);

CREATE INDEX IF NOT EXISTS facebook_page_conversations_tenant_page_idx
    ON facebook_page_conversations (tenant_id, page_id);

CREATE INDEX IF NOT EXISTS facebook_page_conversations_install_state_idx
    ON facebook_page_conversations (facebook_page_installation_id, state);

COMMENT ON TABLE facebook_page_installations IS
    'Facebook Page/Messenger install rows keyed by Meta Page id. Page tokens and app secrets are stored in encrypted_secrets refs.';
COMMENT ON COLUMN facebook_page_installations.oldest_message_at IS
    'Oldest retrievable message timestamp actually reached during Graph pagination.';
COMMENT ON COLUMN facebook_page_installations.backfill_exhausted_reason IS
    'Terminal coverage reason; all_available_history means Graph pagination exhausted for accessible conversations.';

-- Source-registry CHECK widening. Keep this list in lockstep with
-- RawEnvelope.SourceLiteral and workflow VALID_SOURCES.
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'facebook_pages')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'facebook_pages')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'facebook_pages')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'facebook_pages')) NOT VALID;

COMMIT;
