-- 0217_instagram_ingestion.sql
--   Instagram Messaging (Instagram Professional account DMs) as a Kafka-first
--   ingestion source.
--
-- Shape:
--   instagram_installations   — tenant-scoped install/config + secret refs.
--   instagram_webhook_routes  — minimal service-only pre-tenant routing table.
--   instagram_conversations   — one active history shard per IG conversation.
--
-- The webhook receiver must resolve tenant + app secret before it can verify
-- Meta's X-Hub-Signature-256 HMAC, so instagram_webhook_routes intentionally
-- does not use tenant RLS. Its `resolved_tenant_id` is a routing result rather
-- than a tenant-scoping column; it contains only routing keys and opaque secret refs;
-- tenant-facing install/conversation state is RLS-protected.

BEGIN;

CREATE TABLE IF NOT EXISTS instagram_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  base_url TEXT NOT NULL DEFAULT 'https://graph.facebook.com',
  auth_model TEXT NOT NULL DEFAULT 'facebook_login_business'
    CHECK (auth_model IN ('facebook_login_business', 'instagram_login_business')),
  ig_business_account_id TEXT NOT NULL,
  page_id TEXT,
  instagram_username TEXT,
  display_name TEXT,
  app_id TEXT,
  app_secret_ref TEXT,
  verify_token_ref TEXT,
  access_token_ref TEXT,
  token_expires_at TIMESTAMPTZ,
  history_lookback_days INTEGER NOT NULL DEFAULT 90
    CHECK (history_lookback_days BETWEEN 1 AND 3650),
  conversation_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, ig_business_account_id)
);

CREATE INDEX IF NOT EXISTS instagram_installations_tenant_idx
  ON instagram_installations (tenant_id);

CREATE INDEX IF NOT EXISTS instagram_installations_ig_account_idx
  ON instagram_installations (ig_business_account_id);

CREATE TABLE IF NOT EXISTS instagram_webhook_routes (
  id UUID PRIMARY KEY,
  resolved_tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  instagram_installation_id UUID NOT NULL
    REFERENCES instagram_installations(id) ON DELETE CASCADE,
  ig_business_account_id TEXT NOT NULL UNIQUE,
  page_id TEXT,
  app_secret_ref TEXT,
  verify_token_ref TEXT,
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS instagram_webhook_routes_tenant_idx
  ON instagram_webhook_routes (resolved_tenant_id);

CREATE INDEX IF NOT EXISTS instagram_webhook_routes_page_idx
  ON instagram_webhook_routes (page_id) WHERE page_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS instagram_conversations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  instagram_installation_id UUID NOT NULL
    REFERENCES instagram_installations(id) ON DELETE CASCADE,
  conversation_id TEXT NOT NULL,
  participant_id TEXT,
  participant_username TEXT,
  participant_display_name TEXT,
  last_message_at TIMESTAMPTZ,
  messages_cursor TEXT,
  high_water_message_id TEXT,
  state TEXT NOT NULL DEFAULT 'active'
    CHECK (state IN ('active', 'archived', 'disabled')),
  last_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (instagram_installation_id, conversation_id)
);

CREATE INDEX IF NOT EXISTS instagram_conversations_tenant_idx
  ON instagram_conversations (tenant_id);

CREATE INDEX IF NOT EXISTS instagram_conversations_install_state_idx
  ON instagram_conversations (instagram_installation_id, state);

ALTER TABLE instagram_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE instagram_installations FORCE ROW LEVEL SECURITY;
ALTER TABLE instagram_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE instagram_conversations FORCE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'instagram_installations',
    'instagram_conversations'
  ]
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_tenant_isolation ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %I_tenant_isolation ON %I '
      'USING (tenant_id = current_setting(''app.current_tenant'', true)::uuid) '
      'WITH CHECK (tenant_id = current_setting(''app.current_tenant'', true)::uuid)',
      t, t
    );
  END LOOP;
END $$;

ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'instagram')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'instagram')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'instagram')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin', 'whatsapp', 'instagram')) NOT VALID;

COMMIT;
