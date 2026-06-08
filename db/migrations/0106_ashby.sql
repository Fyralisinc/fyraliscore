-- 0106_ashby.sql
--   Ashby — recruiting applicant-tracking-system (ATS) as an ingestion source.
--
-- Ashby is a recruiting ATS REST/RPC API authenticated with an API KEY presented
-- as HTTP Basic (the key is the username, the password is empty —
-- base64("KEY:")); NOT OAuth, so there is NO refresh token. Every call is scoped
-- to an organization `org_id` (the scope-id, analogous to Gusto's company_uuid /
-- Carta's firm_id). Ashby follows the Gusto entity-model archetype (0097): a
-- tenant-scoped install plus an enumerated set of recruiting entity types
-- (candidate / application / job / interview / offer) to shard on — the same
-- shape as gusto_installations / gusto_entities, with TWO differences:
--
--   1. Auth is an API key (NO refresh_secret_ref / token_expires_at columns).
--   2. Ashby HAS an HMAC webhook (HMAC-SHA256 / hex / Ashby-Signature), so
--      ashby_installations KEEPS a webhook_secret_ref column (unlike Carta,
--      which is poll-only). The live edge resolves the tenant via the
--      provider_installations row (provider='ashby', installation_id=org_id).
--
--   ashby_installations — one row per (tenant, org_id)
--   ashby_entities      — one row per entity type the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0105 to add 'hibob'; here we add 'ashby'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the FULL canonical 25-source set forward (a strict
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
--     gusto_* / carta_* policy template (0097 / 0104).
--
-- NUMBERING: 0106 lands AFTER 0105_hibob; its source-CHECK carries the FULL
-- canonical 25-source set verbatim, so applying it after 0105 is a strict
-- superset and cannot silently drop any prior source.

BEGIN;

-- ---------------------------------------------------------------------
-- ashby_installations — one row per (tenant, org_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ashby_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The Ashby organization id; scopes every API call and is the webhook
  -- tenant-resolution key (provider_installations.installation_id = org_id).
  org_id TEXT NOT NULL,
  -- API host base, e.g. https://api.ashbyhq.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- encrypted_secrets pointer for the Ashby API key (Basic username; empty
  -- password). NO refresh token — API-key archetype (Brex/Jira).
  secret_ref TEXT,
  -- encrypted_secrets pointer for the webhook HMAC signing secret. Ashby HAS a
  -- webhook (unlike Carta), so this column is present.
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, org_id)
);

CREATE INDEX IF NOT EXISTS ashby_installations_tenant_idx
  ON ashby_installations (tenant_id);

-- org_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS ashby_installations_scope_idx
  ON ashby_installations (org_id);

-- ---------------------------------------------------------------------
-- ashby_entities — one row per (install, entity_type) the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ashby_entities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  ashby_installation_id UUID NOT NULL
        REFERENCES ashby_installations(id) ON DELETE CASCADE,
  -- 'candidate' | 'application' | 'job' | 'interview' | 'offer' (an Ashby
  -- recruiting entity kind).
  entity_type TEXT NOT NULL,
  -- Incremental delta primitive: the persisted Ashby `syncToken` for this
  -- entity type. The incremental poll re-runs the fetcher with this token so
  -- only entities changed since it was minted come back. NULL until first sync.
  sync_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ashby_installation_id, entity_type)
);

CREATE INDEX IF NOT EXISTS ashby_entities_install_idx
  ON ashby_entities (ashby_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gusto_* / carta_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE ashby_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE ashby_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE ashby_entities      ENABLE ROW LEVEL SECURITY;
ALTER TABLE ashby_entities      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'ashby_installations',
    'ashby_entities'
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
-- Source-registry CHECK widening — admit 'ashby' on all four M6 substrate
-- tables, carrying the FULL canonical 25-source set forward.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta', 'hibob', 'ashby', 'linkedin')) NOT VALID;

COMMIT;
