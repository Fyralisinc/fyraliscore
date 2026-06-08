-- 0096_ramp.sql
--   Ramp — corporate card / spend management as a finance ingestion source.
--
-- Ramp is a fintech REST API authenticated with OAuth 2.0; the access token
-- (~hours) is held in encrypted_secrets behind secret_ref and the rotating
-- refresh token behind refresh_secret_ref. Every call is scoped to a
-- `business_id` (the realm-equivalent). Ramp follows the QuickBooks OAuth
-- archetype (0075): a tenant-scoped install plus an enumerated set of entity
-- types to shard on — the same shape as quickbooks_installations /
-- quickbooks_entities:
--
--   ramp_installations — one row per (tenant, business_id)
--   ramp_entities      — one row per entity type the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0095 to add 'brex'; here we add 'ramp'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry ALL prior sources forward (a strict superset, now
-- including 'brex') so applying this migration last cannot drop an existing
-- source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     quickbooks_* policy template (0075).
--
-- NUMBERING: 0096 lands AFTER 0095_brex.sql; both DROP+re-ADD the SAME four
-- source-CHECK constraints, so the last applied wins — this migration carries
-- BOTH 'brex' AND 'ramp' forward (a strict superset of 0095's list) so applying
-- it does not silently drop 'brex' from the allowed set. Gusto lands next as
-- 0097 and carries 'brex','ramp' forward.

BEGIN;

-- ---------------------------------------------------------------------
-- ramp_installations — one row per (tenant, business_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ramp_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The Ramp business id; scopes every API call and is the webhook
  -- tenant-resolution key.
  business_id TEXT NOT NULL,
  -- API host base, e.g. https://api.ramp.com/developer/v1 (no trailing slash).
  base_url TEXT NOT NULL,
  -- encrypted_secrets pointer for the OAuth access token (Bearer).
  secret_ref TEXT,
  -- encrypted_secrets pointer for the rotating OAuth refresh token (owned by
  -- the oauth_poller in production; the poller must persist the new one each
  -- refresh cycle).
  refresh_secret_ref TEXT,
  -- When the current access token expires (for the poller's refresh schedule).
  token_expires_at TIMESTAMPTZ,
  -- encrypted_secrets pointer for the webhook verifier token (HMAC-SHA256).
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, business_id)
);

CREATE INDEX IF NOT EXISTS ramp_installations_tenant_idx
  ON ramp_installations (tenant_id);

-- business_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS ramp_installations_scope_idx
  ON ramp_installations (business_id);

-- ---------------------------------------------------------------------
-- ramp_entities — one row per (install, entity_type) the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ramp_entities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  ramp_installation_id UUID NOT NULL
        REFERENCES ramp_installations(id) ON DELETE CASCADE,
  -- 'transaction' | 'card' | 'reimbursement' | ... (a Ramp entity kind).
  entity_type TEXT NOT NULL,
  -- Incremental delta primitive: the high-water "updated/modified" timestamp
  -- (ISO) of the newest entity ingested for this type. The incremental poll
  -- re-runs the fetcher filtered above this cursor. NULL until first sync.
  updated_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ramp_installation_id, entity_type)
);

CREATE INDEX IF NOT EXISTS ramp_entities_install_idx
  ON ramp_entities (ramp_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the quickbooks_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE ramp_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ramp_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE ramp_entities      ENABLE ROW LEVEL SECURITY;
ALTER TABLE ramp_entities      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ramp_installations',
    'ramp_entities'
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
-- Source-registry CHECK widening — admit 'ramp' on all four M6 substrate
-- tables, carrying 'brex' (0095) AND every prior source forward.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp')) NOT VALID;

COMMIT;
