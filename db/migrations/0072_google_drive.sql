-- 0072_google_drive.sql
--   IN-16 — Google Drive as a seventh ingestion source.
--
-- Google Drive is a Google Workspace API and reuses the EXISTING Gmail
-- Domain-Wide-Delegation auth substrate (services/integrations/gmail/dwd.py:
-- get_minter() + GoogleHttpClient), exactly like Google Calendar (0060). It
-- does NOT use the provider_installations OAuth-bot-token path. Its install +
-- per-resource shape mirrors the Calendar tables:
--
--   google_drive_installations — one row per (tenant, workspace_domain)
--   google_drive_targets        — one row per drive to fetch: a user's
--                                 My Drive OR an org Shared Drive.
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0060 to admit 'google_calendar'):
--   - source_onboarding_runs  (migration 0055)
--   - onboarding_shards        (migration 0045)
--   - ingestion_failures       (migration 0046)
--   - onboarding_triggers      (migration 0047)
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
--     gmail_* / google_calendar_* policy template (0031 / 0060).

BEGIN;

-- ---------------------------------------------------------------------
-- google_drive_installations — one row per (tenant, workspace_domain)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS google_drive_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  workspace_domain TEXT NOT NULL,
  service_account_email TEXT NOT NULL,
  -- Drive content export requires the readonly (not metadata-only) scope.
  scope TEXT NOT NULL DEFAULT 'drive.readonly'
        CHECK (scope IN ('drive.readonly')),
  -- Admin-authored selection: {"users":[...],"groups":[...],"org_units":[...]}.
  -- Shared drives are enumerated org-wide at onboarding (not part of the spec).
  inclusion_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
  -- Whether to enumerate + ingest org Shared Drives in addition to My Drives.
  include_shared_drives BOOLEAN NOT NULL DEFAULT TRUE,
  resolved_target_count INTEGER NOT NULL DEFAULT 0,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, workspace_domain)
);

CREATE INDEX IF NOT EXISTS google_drive_installations_tenant_idx
  ON google_drive_installations (tenant_id);

-- ---------------------------------------------------------------------
-- google_drive_targets — one row per drive the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS google_drive_targets (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  google_drive_installation_id UUID NOT NULL
        REFERENCES google_drive_installations(id) ON DELETE CASCADE,
  -- 'my_drive' (per-user corpus, addressed by impersonating owner_email) or
  -- 'shared_drive' (a Team/Shared Drive addressed by drive_id).
  drive_kind TEXT NOT NULL
        CHECK (drive_kind IN ('my_drive', 'shared_drive')),
  -- For my_drive this is the sentinel 'my-drive'; for shared_drive it is the
  -- Google driveId. Kept non-NULL so the UNIQUE key is stable.
  drive_id TEXT NOT NULL DEFAULT 'my-drive',
  -- The impersonated identity: the My-Drive owner, or an admin who can see the
  -- shared drive.
  owner_email TEXT NOT NULL,
  display_name TEXT,
  -- Google's incremental delta primitive (changes.getStartPageToken); captured
  -- at the START of a full backfill and consumed by the poll re-run (D2). NULL
  -- until the first full sync seeds it.
  start_page_token TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (google_drive_installation_id, drive_kind, drive_id, owner_email)
);

CREATE INDEX IF NOT EXISTS google_drive_targets_install_idx
  ON google_drive_targets (google_drive_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gmail_* / google_calendar_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE google_drive_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_drive_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE google_drive_targets       ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_drive_targets       FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'google_drive_installations',
    'google_drive_targets'
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
-- Source-registry CHECK widening — admit 'google_drive' on all four
-- M6 substrate tables.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive'));

COMMIT;
