-- 0095_brex.sql
--   Brex — corporate card / cash management as a finance ingestion source.
--
-- Brex is a fintech REST API authenticated with a long-lived API token (HTTP
-- Bearer; no token refresh). The token is held in encrypted_secrets and
-- referenced by secret_ref; the base_url lives on the install row. Brex follows
-- the Mercury Bearer archetype (0074): a tenant-scoped install plus an
-- enumerated set of sub-resources (accounts) to shard on — the same shape as
-- mercury_installations / mercury_accounts (NOT the provider_installations
-- OAuth-bot-token path):
--
--   brex_installations — one row per (tenant, base_url)
--   brex_accounts      — one row per account the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0094 to add 'telegram'; here we add 'brex'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry ALL prior sources forward (a strict superset) so
-- applying this migration last cannot drop an existing source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     mercury_* policy template (0074).
--
-- NUMBERING: 0095 is the next free migration (0094_telegram.sql is the latest).
-- Ramp lands next as 0096 and carries 'brex' forward in its four source-CHECK
-- lists (the newest-migration-must-list-every-prior-source rule).

BEGIN;

-- ---------------------------------------------------------------------
-- brex_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS brex_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Brex API base, e.g. https://platform.brexapis.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the API token (Bearer).
  secret_ref TEXT,
  -- Brex organization id; used for webhook tenant resolution (the webhook
  -- payload is expected to carry an organization id). NULL until resolved.
  organization_id TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC signing secret.
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS brex_installations_tenant_idx
  ON brex_installations (tenant_id);

-- organization_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS brex_installations_org_idx
  ON brex_installations (organization_id) WHERE organization_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- brex_accounts — one row per account the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS brex_accounts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  brex_installation_id UUID NOT NULL
        REFERENCES brex_installations(id) ON DELETE CASCADE,
  -- Brex account id (the API resource id).
  account_id TEXT NOT NULL,
  -- Display metadata (mirrors mercury_accounts so the cloned planner/loader can
  -- surface account name/kind on observations). Nullable — sharding keys on
  -- account_id alone.
  account_name TEXT,
  account_kind TEXT,
  -- Incremental delta primitive: the high-water transaction cursor for this
  -- account. The incremental poll re-runs the fetcher from this cursor. NULL
  -- until the first sync.
  txn_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (brex_installation_id, account_id)
);

CREATE INDEX IF NOT EXISTS brex_accounts_install_idx
  ON brex_accounts (brex_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the mercury_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE brex_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE brex_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE brex_accounts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE brex_accounts      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'brex_installations',
    'brex_accounts'
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
-- Source-registry CHECK widening — admit 'brex' on all four M6 substrate
-- tables. Carries every prior source forward (strict superset of 0094's list)
-- so applying this migration last does not drop any of them.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex')) NOT VALID;

COMMIT;
