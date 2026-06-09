-- 0099_fireflies.sql
--   Fireflies — AI meeting-notetaker (transcripts) as an ingestion source.
--
-- Fireflies.ai is a SaaS REST/GraphQL API authenticated with a long-lived API
-- token (HTTP Bearer; no token refresh). The token is held in encrypted_secrets
-- and referenced by secret_ref; the base_url lives on the install row. Fireflies
-- follows the Brex Bearer archetype (0095) but is workspace-scoped with NO
-- sharded child resource — a workspace's transcripts are a single stream — so
-- there is ONE install table (NOT a mercury_accounts / brex_accounts child
-- table):
--
--   fireflies_installations — one row per (tenant, base_url), carrying the
--   workspace_id the planner shards on.
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0098 to add 'deel'; here we add 'fireflies'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the full canonical 22-source set forward (a strict
-- superset of any prior list) so applying this migration last cannot drop an
-- existing source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS is additive; the CHECK widening
--     admits a strict superset of the prior allowed set, so no existing row can
--     violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): the new table ENABLEs + FORCEs RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     brex_* policy template (0095).
--
-- NUMBERING: 0099 is the next free migration (0098_deel.sql is the latest).

BEGIN;

-- ---------------------------------------------------------------------
-- fireflies_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fireflies_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Fireflies API base, e.g. https://api.fireflies.ai (no trailing slash).
  base_url TEXT NOT NULL,
  -- Fireflies workspace id the token is scoped to; namespaces every
  -- transcript's external_id and keys webhook tenant resolution. NULL until
  -- resolved at install time.
  workspace_id TEXT,
  -- Display name for the workspace (UI only).
  workspace_name TEXT,
  -- Opaque pointer into encrypted_secrets for the API token (Bearer).
  secret_ref TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC signing secret.
  webhook_secret_ref TEXT,
  -- Incremental delta primitive: the high-water transcript cursor for this
  -- workspace. The incremental poll re-runs the fetcher from this cursor. NULL
  -- until the first sync.
  transcript_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS fireflies_installations_tenant_idx
  ON fireflies_installations (tenant_id);

-- workspace_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS fireflies_installations_workspace_idx
  ON fireflies_installations (workspace_id) WHERE workspace_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- RLS — mirror the brex_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE fireflies_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE fireflies_installations FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'fireflies_installations'
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
-- Source-registry CHECK widening — admit 'fireflies' on all four M6 substrate
-- tables. Carries the full canonical 22-source set forward (strict superset of
-- any prior list) so applying this migration last does not drop any of them.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta')) NOT VALID;

COMMIT;
