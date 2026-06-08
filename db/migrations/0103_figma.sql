-- 0103_figma.sql
--   Figma — collaborative design files as a design ingestion source.
--
-- Figma is a REST/JSON API (v1) authenticated with a long-lived org/team access
-- token (HTTP Bearer; no token refresh in v1). The token is held in
-- encrypted_secrets and referenced by secret_ref; the base_url lives on the
-- install row. Figma follows the Brex Bearer archetype (0095): a tenant-scoped
-- install plus an enumerated set of sub-resources (files) to shard on — the same
-- shape as brex_installations / brex_accounts (NOT the provider_installations
-- OAuth-bot-token path):
--
--   figma_installations — one row per (tenant, base_url)
--   figma_files         — one row per file the planner shards on
--
-- WEBHOOK DIVERGENCE: real Figma webhooks (V2) authenticate via a PASSCODE in
-- the request body, not an HMAC header. The webhook_secret_ref column therefore
-- points at the per-tenant passcode in production; the live edge + the synthetic
-- gate treat it as an HMAC signing secret (see signatures/figma.py TODO). The
-- install schema is identical either way.
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (here we add 'figma' and carry the full canonical set forward):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the full canonical 22-source set so applying this
-- migration last cannot drop an existing source from the allowed set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of any prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     brex_* policy template (0095).

BEGIN;

-- ---------------------------------------------------------------------
-- figma_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS figma_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Figma API base, e.g. https://api.figma.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the access token (Bearer).
  secret_ref TEXT,
  -- Figma team id; used for webhook tenant resolution (the webhook payload is
  -- expected to carry a team id) and to namespace every external_id. NULL until
  -- resolved.
  team_id TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook signing secret /
  -- passcode (passcode-in-body in real Figma; HMAC secret for the gate).
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS figma_installations_tenant_idx
  ON figma_installations (tenant_id);

-- team_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS figma_installations_team_idx
  ON figma_installations (team_id) WHERE team_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- figma_files — one row per file the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS figma_files (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  figma_installation_id UUID NOT NULL
        REFERENCES figma_installations(id) ON DELETE CASCADE,
  -- Figma file key (the API resource id).
  file_key TEXT NOT NULL,
  -- Display metadata (mirrors brex_accounts so the cloned planner/loader can
  -- surface file name/project on observations). Nullable — sharding keys on
  -- file_key alone.
  file_name TEXT,
  project_name TEXT,
  -- Incremental delta primitive: the high-water event cursor for this file. The
  -- incremental poll re-runs the fetcher from this cursor. NULL until the first
  -- sync.
  event_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (figma_installation_id, file_key)
);

CREATE INDEX IF NOT EXISTS figma_files_install_idx
  ON figma_files (figma_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the brex_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE figma_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE figma_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE figma_files         ENABLE ROW LEVEL SECURITY;
ALTER TABLE figma_files         FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'figma_installations',
    'figma_files'
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
-- Source-registry CHECK widening — admit 'figma' on all four M6 substrate
-- tables. Carries the full canonical 22-source set forward (a strict superset)
-- so applying this migration last does not drop any existing source.
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
