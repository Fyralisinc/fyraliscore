-- 0188_instagram_production_contract.sql
--
-- Production hardening for Instagram Messaging. 0187 introduced the source;
-- this migration separates a local thread key from Meta's conversation id,
-- adds connection health metadata, and gives external DM participants a
-- tenant-scoped contact record without treating them as employees.

BEGIN;

ALTER TABLE instagram_installations
  ALTER COLUMN auth_model SET DEFAULT 'instagram_login_business';

ALTER TABLE instagram_installations
  ADD COLUMN IF NOT EXISTS granted_scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS access_token_kind TEXT NOT NULL DEFAULT 'instagram_user',
  ADD COLUMN IF NOT EXISTS webhook_subscribed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS webhook_subscription_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS connection_status TEXT NOT NULL DEFAULT 'active'
    CHECK (connection_status IN ('pending', 'active', 'degraded', 'revoked')),
  ADD COLUMN IF NOT EXISTS last_health_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_error_code TEXT,
  ADD COLUMN IF NOT EXISTS last_error_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS business_actor_id UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS conversation_discovery_cursor TEXT,
  ADD COLUMN IF NOT EXISTS conversation_discovered_at TIMESTAMPTZ;

UPDATE instagram_installations
   SET base_url = 'https://graph.instagram.com',
       auth_model = 'instagram_login_business'
 WHERE auth_model = 'instagram_login_business'
   AND base_url = 'https://graph.facebook.com';

CREATE TABLE IF NOT EXISTS instagram_contacts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  instagram_installation_id UUID NOT NULL
    REFERENCES instagram_installations(id) ON DELETE CASCADE,
  instagram_scoped_user_id TEXT NOT NULL,
  source_actor_ref TEXT NOT NULL,
  username TEXT,
  display_name TEXT,
  profile_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  inbound_message_count BIGINT NOT NULL DEFAULT 0
    CHECK (inbound_message_count >= 0),
  actor_id UUID REFERENCES actors(id),
  customer_resource_id UUID REFERENCES resources(id),
  promotion_state TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (promotion_state IN ('unresolved', 'candidate', 'promoted', 'suppressed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (instagram_installation_id, instagram_scoped_user_id),
  UNIQUE (tenant_id, source_actor_ref)
);

CREATE INDEX IF NOT EXISTS instagram_contacts_tenant_recent_idx
  ON instagram_contacts (tenant_id, last_seen_at DESC);

ALTER TABLE instagram_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE instagram_contacts FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS instagram_contacts_tenant_isolation ON instagram_contacts;
CREATE POLICY instagram_contacts_tenant_isolation ON instagram_contacts
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

ALTER TABLE instagram_conversations
  ADD COLUMN IF NOT EXISTS thread_key TEXT,
  ADD COLUMN IF NOT EXISTS provider_conversation_id TEXT,
  ADD COLUMN IF NOT EXISTS contact_id UUID REFERENCES instagram_contacts(id),
  ADD COLUMN IF NOT EXISTS provider_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS coverage_state TEXT NOT NULL DEFAULT 'unknown'
    CHECK (coverage_state IN ('unknown', 'partial', 'complete', 'limited_by_provider')),
  ADD COLUMN IF NOT EXISTS oldest_retrieved_at TIMESTAMPTZ;

UPDATE instagram_conversations
   SET thread_key = COALESCE(thread_key, conversation_id)
 WHERE thread_key IS NULL;

-- 0187 stored the Graph id in conversation_id for history discovery. A
-- webhook-only row uses the local `business_id:scoped_user_id` shape and is
-- intentionally left without a provider id until a later discovery pass.
UPDATE instagram_conversations
   SET provider_conversation_id = conversation_id
 WHERE provider_conversation_id IS NULL
   AND position(':' IN conversation_id) = 0;

CREATE UNIQUE INDEX IF NOT EXISTS instagram_conversations_thread_key_idx
  ON instagram_conversations (instagram_installation_id, thread_key)
  WHERE thread_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS instagram_conversations_provider_id_idx
  ON instagram_conversations (instagram_installation_id, provider_conversation_id)
  WHERE provider_conversation_id IS NOT NULL;

ALTER TABLE instagram_webhook_routes
  ADD COLUMN IF NOT EXISTS app_id TEXT;

CREATE INDEX IF NOT EXISTS instagram_installations_health_idx
  ON instagram_installations (tenant_id, connection_status)
  WHERE disabled_at IS NULL;

COMMIT;
