-- 0074_mercury.sql
--   Mercury — banking/cash as a finance ingestion source.
--
-- Mercury is a business-banking REST API authenticated with a long-lived API
-- token (HTTP Bearer; the token is also accepted as the Basic-auth username).
-- The token is held in encrypted_secrets and referenced by secret_ref; the
-- base_url lives on the install row. Mercury needs a tenant-scoped install plus
-- an enumerated set of sub-resources (accounts) to shard on — the same shape as
-- jira_installations / jira_projects (NOT the provider_installations
-- OAuth-bot-token path):
--
--   mercury_installations — one row per (tenant, base_url)
--   mercury_accounts      — one row per account the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0062 to add 'jira'):
--   - source_onboarding_runs  (migration 0055)
--   - onboarding_shards        (migration 0045)
--   - ingestion_failures       (migration 0046)
--   - onboarding_triggers      (migration 0047)
-- Missing any one breaks a different stage: triggers (onboarding emits one),
-- runs + shards (backfill planning), failures (DLQ writer).
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added with the same name. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     jira_* / google_drive_* policy template (0061 / 0062).
--
-- NUMBERING: 0063 is the next free migration on integration/ingestion-hardening
-- (0062_jira.sql is the latest). QuickBooks lands next as 0064 and carries
-- 'mercury' forward in its four source-CHECK lists (the newest-migration-must-
-- list-every-prior-source rule).

BEGIN;

-- ---------------------------------------------------------------------
-- mercury_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mercury_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Mercury API base, e.g. https://api.mercury.com/api/v1 (no trailing slash).
  base_url TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the API token (Bearer).
  secret_ref TEXT,
  -- Mercury organization id; used for webhook tenant resolution (the webhook
  -- payload carries organizationId). NULL until resolved.
  organization_id TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC signing secret.
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS mercury_installations_tenant_idx
  ON mercury_installations (tenant_id);

-- organization_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS mercury_installations_org_idx
  ON mercury_installations (organization_id) WHERE organization_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- mercury_accounts — one row per account the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mercury_accounts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  mercury_installation_id UUID NOT NULL
        REFERENCES mercury_installations(id) ON DELETE CASCADE,
  -- Mercury account id (the API resource id).
  account_id TEXT NOT NULL,
  account_name TEXT,
  -- 'checking' | 'savings' | ... (Mercury account type; informational).
  account_kind TEXT,
  -- Incremental delta primitive: the high-water transaction createdAt (ISO) of
  -- the newest transaction ingested for this account. The incremental poll
  -- re-runs the fetcher with start=<cursor date>. NULL until the first sync.
  txn_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (mercury_installation_id, account_id)
);

CREATE INDEX IF NOT EXISTS mercury_accounts_install_idx
  ON mercury_accounts (mercury_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the jira_* / google_drive_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE mercury_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE mercury_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE mercury_accounts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE mercury_accounts      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'mercury_installations',
    'mercury_accounts'
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
-- Source-registry CHECK widening — admit 'mercury' on all four M6
-- substrate tables. Carries every prior source forward (the newest-
-- migration-must-list-every-prior-source rule).
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury')) NOT VALID;

COMMIT;
