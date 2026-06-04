-- 0071_google_calendar.sql
--   IN-15 — Google Calendar as a sixth ingestion source.
--
-- Google Calendar is a Google Workspace API and reuses the EXISTING Gmail
-- Domain-Wide-Delegation auth substrate (services/integrations/gmail/dwd.py:
-- get_minter() + GoogleHttpClient). It does NOT use the provider_installations
-- OAuth-bot-token path (slack/github/discord/notion). Its install + per-resource
-- shape therefore mirrors Gmail's gmail_installations / gmail_mailbox_watches,
-- not provider_installations:
--
--   google_calendar_installations — one row per (tenant, workspace_domain)
--   google_calendar_calendars      — one row per included calendar to fetch
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (each `CHECK (source IN ('slack','github','discord','gmail','notion'))`,
-- last widened by 0059):
--   - source_onboarding_runs  (migration 0055, widened 0059)
--   - onboarding_shards        (migration 0045, widened 0059)
--   - ingestion_failures       (migration 0046, widened 0059)
--   - onboarding_triggers      (migration 0047, widened 0059)
-- Missing any one breaks a different stage: triggers (onboarding emits one),
-- runs + shards (backfill planning), failures (DLQ writer).
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS (×2) is additive; the CHECK
--     widening admits a strict superset of the prior allowed set, so no
--     existing row can violate it — non-destructive, no staged plan required.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added with the same Postgres-assigned inline name
--     (`<table>_source_check`). Re-running is a no-op.
--   - Tenant isolation (§III): both new tables ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     gmail_* policy template (migration 0031).

BEGIN;

-- ---------------------------------------------------------------------
-- google_calendar_installations — one row per (tenant, workspace_domain)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS google_calendar_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  workspace_domain TEXT NOT NULL,
  service_account_email TEXT NOT NULL,
  -- Long Calendar scope alias; readonly is the only v1 scope.
  scope TEXT NOT NULL DEFAULT 'calendar.readonly'
        CHECK (scope IN ('calendar.readonly')),
  -- Admin-authored selection: {"users":[...],"groups":[...],"org_units":[...]}.
  inclusion_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolved_calendar_count INTEGER NOT NULL DEFAULT 0,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, workspace_domain)
);

CREATE INDEX IF NOT EXISTS google_calendar_installations_tenant_idx
  ON google_calendar_installations (tenant_id);

-- ---------------------------------------------------------------------
-- google_calendar_calendars — one row per calendar the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS google_calendar_calendars (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  google_calendar_installation_id UUID NOT NULL
        REFERENCES google_calendar_installations(id) ON DELETE CASCADE,
  -- calendarId as Google addresses it; for a user's primary calendar this
  -- is their email address.
  calendar_id TEXT NOT NULL,
  owner_email TEXT,
  -- Google's incremental delta primitive; stamped at the end of a full sync
  -- and consumed by the poll re-run (D2). NULL until the first full sync.
  sync_token TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (google_calendar_installation_id, calendar_id)
);

CREATE INDEX IF NOT EXISTS google_calendar_calendars_install_idx
  ON google_calendar_calendars (google_calendar_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gmail_* tenant_isolation policy template (0031).
-- ---------------------------------------------------------------------
ALTER TABLE google_calendar_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_calendar_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE google_calendar_calendars     ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_calendar_calendars     FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'google_calendar_installations',
    'google_calendar_calendars'
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
-- Source-registry CHECK widening — admit 'google_calendar' on all four
-- M6 substrate tables.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar')) NOT VALID;

COMMIT;
