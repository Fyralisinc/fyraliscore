-- 0073_jira.sql
--   IN-17 — Jira as an ingestion source.
--
-- Jira Cloud is a SaaS REST API (v3) authenticated with HTTP Basic
-- (account_email:api_token). The api_token is held in encrypted_secrets and
-- referenced by secret_ref; the base_url + account_email live on the install
-- row. Jira needs a workspace-scoped install plus an enumerated set of
-- sub-resources (projects) to shard on, exactly the gmail/google_calendar
-- shape — so its install + per-resource tables mirror
-- google_calendar_installations / google_calendar_calendars, NOT the
-- provider_installations OAuth-bot-token path (slack/github/discord/notion):
--
--   jira_installations — one row per (tenant, base_url)
--   jira_projects      — one row per project the planner shards on
--
-- NUMBERING (was 0061 on the IN-17 branch): renumbered to 0062 because IN-16
-- Google Drive landed first as 0061_google_drive.sql. That migration and this
-- one both DROP+re-ADD the SAME four source-CHECK constraints, so the LAST one
-- applied wins. To avoid silently dropping 'google_drive' from the allowed set,
-- the CHECK lists below carry BOTH 'google_drive' AND 'jira' (a strict superset
-- of 0061's list). Listing google_drive is harmless even if 0061 hasn't run.
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (widened by 0060 to add 'google_calendar', by 0061 to add
-- 'google_drive', and here to add 'jira'):
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
-- jira_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jira_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Site base URL, e.g. https://acme.atlassian.net (no trailing slash).
  base_url TEXT NOT NULL,
  -- Atlassian account email used as the Basic-auth username.
  account_email TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the API token (the Basic-auth
  -- password). Mirrors provider_installations.secret_ref.
  secret_ref TEXT,
  -- Atlassian cloudId for the site; used for webhook tenant resolution
  -- (the webhook payload carries the site/cloudId). NULL until resolved.
  cloud_id TEXT,
  -- Per-installation opaque token embedded in the webhook callback URL.
  -- Jira Cloud dynamic webhooks are NOT HMAC-signed, so the edge verifies
  -- this constant-time instead (see services/webhooks/signatures/jira.py).
  webhook_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS jira_installations_tenant_idx
  ON jira_installations (tenant_id);

-- cloud_id lookup is the webhook tenant-resolution hot path.
CREATE INDEX IF NOT EXISTS jira_installations_cloud_id_idx
  ON jira_installations (cloud_id) WHERE cloud_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- jira_projects — one row per project the planner shards on
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jira_projects (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  jira_installation_id UUID NOT NULL
        REFERENCES jira_installations(id) ON DELETE CASCADE,
  -- Project key as users address it, e.g. "ENG".
  project_key TEXT NOT NULL,
  -- Numeric/string project id from the REST API.
  project_id TEXT,
  project_name TEXT,
  -- Incremental delta primitive: the high-water `updated` timestamp of the
  -- newest issue ingested for this project (ISO-8601). The incremental poll
  -- re-runs the fetcher with JQL `updated >= updated_cursor`. NULL until the
  -- first full sync — analogous to google_calendar_calendars.sync_token.
  updated_cursor TEXT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
        CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (jira_installation_id, project_key)
);

CREATE INDEX IF NOT EXISTS jira_projects_install_idx
  ON jira_projects (jira_installation_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the gmail_* / google_calendar_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE jira_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE jira_installations FORCE  ROW LEVEL SECURITY;
ALTER TABLE jira_projects      ENABLE ROW LEVEL SECURITY;
ALTER TABLE jira_projects      FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'jira_installations',
    'jira_projects'
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
-- Source-registry CHECK widening — admit 'jira' on all four M6 substrate
-- tables. Carries 'google_drive' forward (IN-16 / 0061) so applying this
-- migration last does not drop it from the allowed set.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira'));

COMMIT;
