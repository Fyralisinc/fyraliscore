-- 0102_miro.sql
--   Miro — collaborative whiteboard / design as an ingestion source.
--
-- Miro is a REST API authenticated with a long-lived org-app Bearer token (HTTP
-- Bearer; no token refresh). The token is held in encrypted_secrets and
-- referenced by secret_ref; the base_url lives on the install row. Miro follows
-- the Brex Bearer archetype (0095): a tenant-scoped install plus an enumerated
-- set of sub-resources (boards) to shard on — the same shape as
-- brex_installations / brex_accounts (NOT the provider_installations
-- OAuth-bot-token path):
--
--   miro_installations — one row per (tenant, base_url); org-level token install
--   miro_boards        — one row per board the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (here we add 'miro'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the FULL canonical 22-source set verbatim (a
-- strict superset of every prior list) so applying this migration last cannot
-- drop an existing source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     brex_* policy template (0095).

BEGIN;

-- ---------------------------------------------------------------------
-- miro_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS miro_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Miro API base, e.g. https://api.miro.com/v2 (no trailing slash).
  base_url TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the org-app API token (Bearer).
  secret_ref TEXT,
  -- Miro org id; used for webhook tenant resolution AND as the external_id
  -- namespacing identifier (`miro:{org_id}:item:…`). NULL until resolved.
  org_id TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC signing secret.
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS miro_installations_tenant_idx
  ON miro_installations (tenant_id);

-- org_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS miro_installations_org_idx
  ON miro_installations (org_id) WHERE org_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- miro_boards — one row per board the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS miro_boards (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  miro_installation_id UUID NOT NULL
        REFERENCES miro_installations(id) ON DELETE CASCADE,
  -- Miro board id (the API resource id).
  board_id TEXT NOT NULL,
  -- Display metadata (mirrors brex_accounts so the cloned planner/loader can
  -- surface board name/kind on observations). Nullable — sharding keys on
  -- board_id alone.
  board_name TEXT,
  board_kind TEXT,
  -- Incremental delta primitive: the high-water item cursor for this board.
  -- The incremental poll re-runs the fetcher from this cursor. NULL until the
  -- first sync.
  item_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (miro_installation_id, board_id)
);

CREATE INDEX IF NOT EXISTS miro_boards_install_idx
  ON miro_boards (miro_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the brex_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE miro_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE miro_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE miro_boards        ENABLE ROW LEVEL SECURITY;
ALTER TABLE miro_boards        FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'miro_installations',
    'miro_boards'
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
-- Source-registry CHECK widening — admit 'miro' on all four M6 substrate
-- tables. Carries the FULL canonical 22-source set forward (strict superset of
-- every prior list) so applying this migration last does not drop any of them.
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
