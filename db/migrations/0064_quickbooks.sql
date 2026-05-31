-- 0064_quickbooks.sql
--   QuickBooks Online — accounting/GL as a finance ingestion source.
--
-- QuickBooks Online is an accounting REST API authenticated with OAuth 2.0; the
-- access token (~60 min) is held in encrypted_secrets behind secret_ref and the
-- rotating refresh token behind refresh_secret_ref. Every call is scoped to a
-- company `realmId`. QuickBooks needs a tenant-scoped install plus an enumerated
-- set of entity types (Invoice/Bill/BillPayment/Payment) to shard on — the same
-- shape as jira_installations / jira_projects:
--
--   quickbooks_installations — one row per (tenant, realm_id)
--   quickbooks_entities      — one row per entity type the planner shards on
--
-- Plus the source-registry CHECK widening. NUMBERING: 0064 lands AFTER
-- 0063_mercury.sql; both DROP+re-ADD the SAME four source-CHECK constraints, so
-- the last applied wins — this migration carries BOTH 'mercury' AND
-- 'quickbooks' forward (a strict superset of 0063's list) so applying it does
-- not silently drop 'mercury' from the allowed set.
--
-- §II compliance: append-only + idempotent + tenant-isolated (RLS), mirroring
-- the jira_* policy template (0062).

BEGIN;

-- ---------------------------------------------------------------------
-- quickbooks_installations — one row per (tenant, realm_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quickbooks_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The QuickBooks company id; scopes every API call and is the webhook
  -- tenant-resolution key (the webhook payload carries realmId).
  realm_id TEXT NOT NULL,
  -- API host base, e.g. https://quickbooks.api.intuit.com (no trailing slash);
  -- sandbox uses https://sandbox-quickbooks.api.intuit.com. The realm-scoped
  -- path (/v3/company/{realm}/...) is composed by the client.
  base_url TEXT NOT NULL,
  -- encrypted_secrets pointer for the OAuth access token (Bearer).
  secret_ref TEXT,
  -- encrypted_secrets pointer for the rotating OAuth refresh token (owned by
  -- the oauth_poller in production; QBO invalidates the prior token within 24h
  -- of each refresh, so the poller must persist the new one each cycle).
  refresh_secret_ref TEXT,
  -- When the current access token expires (for the poller's refresh schedule).
  token_expires_at TIMESTAMPTZ,
  -- encrypted_secrets pointer for the webhook verifier token (HMAC-SHA256).
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, realm_id)
);

CREATE INDEX IF NOT EXISTS quickbooks_installations_tenant_idx
  ON quickbooks_installations (tenant_id);

-- realm_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS quickbooks_installations_realm_idx
  ON quickbooks_installations (realm_id);

-- ---------------------------------------------------------------------
-- quickbooks_entities — one row per (install, entity_type) the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quickbooks_entities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  quickbooks_installation_id UUID NOT NULL
        REFERENCES quickbooks_installations(id) ON DELETE CASCADE,
  -- 'Invoice' | 'Bill' | 'BillPayment' | 'Payment' (a QBO entity name).
  entity_type TEXT NOT NULL,
  -- Incremental delta primitive: the high-water Metadata.LastUpdatedTime (ISO)
  -- of the newest entity ingested for this type. The incremental poll re-runs
  -- the fetcher with WHERE Metadata.LastUpdatedTime > cursor. NULL until first
  -- sync.
  updated_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (quickbooks_installation_id, entity_type)
);

CREATE INDEX IF NOT EXISTS quickbooks_entities_install_idx
  ON quickbooks_entities (quickbooks_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the jira_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE quickbooks_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE quickbooks_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE quickbooks_entities      ENABLE ROW LEVEL SECURITY;
ALTER TABLE quickbooks_entities      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'quickbooks_installations',
    'quickbooks_entities'
  ]
  LOOP
    EXECUTE format(
      'DROP POLICY IF EXISTS %I_tenant_isolation ON %I',
      t, t
    );
    EXECUTE format(
      'CREATE POLICY %I_tenant_isolation ON %I '
      'USING (tenant_id = current_setting(''app.current_tenant'', true)::uuid) '
      'WITH CHECK (tenant_id = current_setting(''app.current_tenant'', true)::uuid)',
      t, t
    );
  END LOOP;
END $$;

-- ---------------------------------------------------------------------
-- Source-registry CHECK widening — admit 'quickbooks' on all four M6
-- substrate tables, carrying 'mercury' (0063) AND every prior source forward.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks'));

COMMIT;
