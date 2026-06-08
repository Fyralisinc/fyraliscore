-- 0105_hibob.sql
--   HiBob ("Bob") — People / HR platform as an ingestion source.
--
-- HiBob's API is authenticated with a **service user**: a service_user_id +
-- token presented as HTTP Basic auth (base64(id:token)) — NOT OAuth, so there is
-- NO refresh token (the long-lived-credential posture of Brex, but Basic instead
-- of Bearer). Every call is scoped to a HiBob account; the scope-id we shard /
-- resolve on is the `company_id` (analogous to Gusto's company_uuid / Carta's
-- firm_id). HiBob follows the Gusto entity-model archetype (0097): a
-- tenant-scoped install plus an enumerated set of People/HR entity types
-- (employee / lifecycle / timeoff / payroll) to shard on — the same shape as
-- gusto_installations / gusto_entities, with TWO differences:
--
--   1. Auth is a service-user token (the secret half) + a public
--      service_user_id (carried on the install row), NOT an OAuth access +
--      refresh pair — so there is NO refresh_secret_ref / token_expires_at, and
--      the install carries service_user_id instead.
--   2. HiBob HAS a live webhook (HMAC-SHA512 / base64 / Bob-Signature), so
--      hibob_installations DOES carry webhook_secret_ref (unlike poll-only
--      Carta), and a provider_installations row is seeded for the live edge.
--
--   hibob_installations — one row per (tenant, company_id)
--   hibob_entities      — one row per entity type the planner shards on
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0104 to add 'carta'; here we add 'hibob'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry the FULL canonical 25-source set forward (a strict
-- superset of every prior list) so applying this migration cannot drop an
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
--     gusto_* / carta_* policy template.
--
-- NUMBERING: 0105 lands after 0104_carta; its source-CHECK carries the FULL
-- canonical 25-source set verbatim (the 22 prior + hibob/ashby/linkedin), so it
-- is a strict superset and cannot silently drop any prior source.

BEGIN;

-- ---------------------------------------------------------------------
-- hibob_installations — one row per (tenant, company_id)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hibob_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The HiBob account/company id; the scope-id that scopes every API call and is
  -- the webhook tenant-resolution key (gate stand-in — see onboarding TODO).
  company_id TEXT NOT NULL,
  -- The public half of the Basic credential (base64(service_user_id:token)).
  -- The token half lives in encrypted_secrets behind secret_ref.
  service_user_id TEXT NOT NULL,
  -- API host base, e.g. https://api.hibob.com (no trailing slash).
  base_url TEXT NOT NULL,
  -- encrypted_secrets pointer for the service-user TOKEN (the secret half of the
  -- HTTP Basic credential). NO refresh token — long-lived (Brex posture).
  secret_ref TEXT,
  -- encrypted_secrets pointer for the webhook HMAC signing secret (Bob-Signature).
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, company_id)
);

CREATE INDEX IF NOT EXISTS hibob_installations_tenant_idx
  ON hibob_installations (tenant_id);

-- company_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS hibob_installations_scope_idx
  ON hibob_installations (company_id);

-- ---------------------------------------------------------------------
-- hibob_entities — one row per (install, entity_type) the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hibob_entities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  hibob_installation_id UUID NOT NULL
        REFERENCES hibob_installations(id) ON DELETE CASCADE,
  -- 'employee' | 'lifecycle' | 'timeoff' | 'payroll' (a HiBob People/HR entity
  -- kind).
  entity_type TEXT NOT NULL,
  -- Incremental delta primitive: the high-water modified/version (ISO) of the
  -- newest entity ingested for this type. The incremental poll re-runs the
  -- fetcher filtered above this cursor. NULL until first sync.
  updated_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (hibob_installation_id, entity_type)
);

CREATE INDEX IF NOT EXISTS hibob_entities_install_idx
  ON hibob_entities (hibob_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gusto_* / carta_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE hibob_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE hibob_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE hibob_entities      ENABLE ROW LEVEL SECURITY;
ALTER TABLE hibob_entities      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hibob_installations',
    'hibob_entities'
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
-- Source-registry CHECK widening — admit 'hibob' on all four M6 substrate
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
