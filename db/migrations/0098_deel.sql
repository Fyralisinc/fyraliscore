-- 0098_deel.sql
--   Deel — global payroll / contractor payments as a finance ingestion source.
--
-- Deel is a payroll/EOR REST API authenticated with a long-lived API token (HTTP
-- Bearer; no token refresh). The token is held in encrypted_secrets and
-- referenced by secret_ref; the base_url lives on the install row. Deel follows
-- the Mercury Bearer archetype (0074): a tenant-scoped install plus an
-- enumerated set of sub-resources (contracts) to shard on — the same shape as
-- mercury_installations / mercury_accounts (NOT the provider_installations
-- OAuth-bot-token path):
--
--   deel_installations — one row per (tenant, base_url)
--   deel_contracts     — one row per contract the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0097 to add 'gusto'; here we add 'deel'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry ALL prior sources forward (a strict superset, now
-- including 'brex','ramp','gusto') so applying this migration last cannot drop
-- an existing source from the set. This is the final source in the finance
-- batch — the four-table IN-list now enumerates all 16 sources.
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
-- NUMBERING: 0098 lands AFTER 0097_gusto.sql; both DROP+re-ADD the SAME four
-- source-CHECK constraints, so the last applied wins — this migration carries
-- 'brex','ramp','gusto' AND 'deel' forward (a strict superset of 0097's list)
-- so applying it does not silently drop any prior source.

BEGIN;

-- ---------------------------------------------------------------------
-- deel_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS deel_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Deel API base, e.g. https://api.letsdeel.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the API token (Bearer).
  secret_ref TEXT,
  -- Deel organization id; used for webhook tenant resolution (the webhook
  -- payload is expected to carry an organization id). NULL until resolved.
  organization_id TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC signing secret.
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS deel_installations_tenant_idx
  ON deel_installations (tenant_id);

-- organization_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS deel_installations_org_idx
  ON deel_installations (organization_id) WHERE organization_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- deel_contracts — one row per contract the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS deel_contracts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  deel_installation_id UUID NOT NULL
        REFERENCES deel_installations(id) ON DELETE CASCADE,
  -- Deel contract id (the API resource id).
  contract_id TEXT NOT NULL,
  -- Incremental delta primitive: the high-water payment cursor for this
  -- contract. The incremental poll re-runs the fetcher from this cursor. NULL
  -- until the first sync.
  payment_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (deel_installation_id, contract_id)
);

CREATE INDEX IF NOT EXISTS deel_contracts_install_idx
  ON deel_contracts (deel_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the mercury_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE deel_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE deel_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE deel_contracts     ENABLE ROW LEVEL SECURITY;
ALTER TABLE deel_contracts     FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'deel_installations',
    'deel_contracts'
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
-- Source-registry CHECK widening — admit 'deel' on all four M6 substrate
-- tables, carrying 'brex' (0095), 'ramp' (0096), 'gusto' (0097) AND every prior
-- source forward. This is the full 16-source IN-list.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel')) NOT VALID;

COMMIT;
