-- 0104_carta.sql
--   Carta — cap-table / equity management as an ingestion source.
--
-- Carta is a cap-table REST API authenticated with OAuth 2.0; the access token
-- (~hours) is held in encrypted_secrets behind secret_ref and the rotating
-- refresh token behind refresh_secret_ref. Every call is scoped to a `firm_id`
-- (the scope-id, analogous to Gusto's company_uuid / QuickBooks' realmId). Carta
-- follows the Gusto OAuth archetype (0097): a tenant-scoped install plus an
-- enumerated set of cap-table entity types (shareholders/share classes/SAFEs/
-- option grants) to shard on — the same shape as gusto_installations /
-- gusto_entities, with ONE difference:
--
--   Carta is POLL-ONLY — there is NO webhook — so carta_installations has NO
--   webhook_secret_ref column (the live edge is the poller, which resolves the
--   tenant directly from carta_installations; no provider_installations row).
--
--   carta_installations — one row per (tenant, firm_id)
--   carta_entities      — one row per entity type the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0098 to add 'deel'; here we add 'carta'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the FULL canonical 22-source set forward (a strict
-- superset of every prior list) so applying this migration last cannot drop an
-- existing source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added NOT VALID. Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     gusto_* policy template (0097).
--
-- NUMBERING: 0104 lands AFTER the other final ingestion sources (0099-0103); its
-- source-CHECK carries the FULL canonical 22-source set verbatim, so applying it
-- last is a strict superset and cannot silently drop any prior source.

BEGIN;

-- ---------------------------------------------------------------------
-- carta_installations — one row per (tenant, firm_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS carta_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The Carta firm id; scopes every API call (path-scoped under
  -- /v1/firms/{firm_id}/...) and is the poll tenant-resolution key.
  firm_id TEXT NOT NULL,
  -- API host base, e.g. https://api.carta.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- encrypted_secrets pointer for the OAuth access token (Bearer).
  secret_ref TEXT,
  -- encrypted_secrets pointer for the rotating OAuth refresh token (owned by
  -- the oauth_poller in production; the poller must persist the new one each
  -- refresh cycle).
  refresh_secret_ref TEXT,
  -- When the current access token expires (for the poller's refresh schedule).
  token_expires_at TIMESTAMPTZ,
  -- NOTE: NO webhook_secret_ref — Carta is poll-only (no webhook edge).
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, firm_id)
);

CREATE INDEX IF NOT EXISTS carta_installations_tenant_idx
  ON carta_installations (tenant_id);

-- firm_id lookup is the poll tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS carta_installations_scope_idx
  ON carta_installations (firm_id);

-- ---------------------------------------------------------------------
-- carta_entities — one row per (install, entity_type) the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS carta_entities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  carta_installation_id UUID NOT NULL
        REFERENCES carta_installations(id) ON DELETE CASCADE,
  -- 'Shareholder' | 'ShareClass' | 'SafeNote' | 'OptionGrant' | ... (a Carta
  -- cap-table entity kind).
  entity_type TEXT NOT NULL,
  -- Incremental delta primitive: the high-water updated_at (ISO) of the newest
  -- entity ingested for this type. The incremental poll re-runs the fetcher
  -- filtered above this cursor. NULL until first sync.
  updated_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (carta_installation_id, entity_type)
);

CREATE INDEX IF NOT EXISTS carta_entities_install_idx
  ON carta_entities (carta_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gusto_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE carta_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE carta_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE carta_entities      ENABLE ROW LEVEL SECURITY;
ALTER TABLE carta_entities      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'carta_installations',
    'carta_entities'
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
-- Source-registry CHECK widening — admit 'carta' on all four M6 substrate
-- tables, carrying the FULL canonical 22-source set forward.
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
