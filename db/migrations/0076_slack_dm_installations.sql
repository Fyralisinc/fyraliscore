-- 0076_slack_dm_installations.sql
--   Per-user Slack DM ingestion (human↔human direct messages) via per-user
--   OAuth user tokens (xoxp).
--
-- Slack's existing bot path (provider_installations, keyed per-workspace
-- team_id) handles CHANNEL signals and can never read human↔human DMs. DM
-- ingestion is consent-based: each user who authorizes the app grants a user
-- token (xoxp) that can read THAT user's own DMs/MPIMs. This introduces a
-- per-USER identity grain (user_id + team_id) ALONGSIDE the per-workspace
-- grain — it does NOT replace provider_installations.
--
--   slack_dm_installations — one row per (tenant, team_id, user_id) consenting
--                            user. The planner shards DM windows per row; the
--                            user token is held in encrypted_secrets and
--                            referenced by user_token_secret_ref.
--
-- §II compliance:
--   - Append-only / additive: CREATE TABLE IF NOT EXISTS; no column drops.
--   - Idempotent: table + indexes guarded IF NOT EXISTS; the RLS policy is
--     dropped IF EXISTS then re-created with the same name. Re-running is a
--     no-op.
--   - Tenant isolation (§III): ENABLE + FORCE RLS with a tenant_isolation
--     policy keyed on app.current_tenant, mirroring the mercury_* / jira_*
--     template (0063 / 0062).
--
-- NUMBERING: 0065 is the next free migration (0064_quickbooks.sql is latest).
-- NO source-registry CHECK widening: DM observations reuse source='slack'
-- (already admitted), so the four M6 substrate CHECKs are untouched and the
-- newest-migration-must-list-every-prior-source rule does not apply here.

BEGIN;

-- ---------------------------------------------------------------------
-- slack_dm_installations — one row per consenting user (per workspace)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS slack_dm_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Slack workspace id the user authorized in (xoxp tokens are workspace-scoped).
  team_id TEXT NOT NULL,
  -- The consenting user's Slack id (U…). DMs this user participates in become
  -- visible to us via their user token.
  user_id TEXT NOT NULL,
  -- Slack Web API base for this install (production or explicit Provider Lab URL);
  -- mirrors how _build_source_client resolves the endpoint. NULL → resolver default.
  base_url TEXT,
  -- Opaque pointer into encrypted_secrets for the user token (xoxp). The raw
  -- token never leaves the secret store; resolution is by label
  -- 'slack_user_token:{team_id}:{user_id}'.
  user_token_secret_ref TEXT,
  -- Comma-separated user scopes granted (audit/diagnostic), e.g.
  -- 'im:history,mpim:history,users:read'. Informational.
  granted_user_scopes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, team_id, user_id)
);

CREATE INDEX IF NOT EXISTS slack_dm_installations_tenant_idx
  ON slack_dm_installations (tenant_id);

-- Active-install enumeration is the planner hot path (shard one DM window per
-- consenting user per tenant).
CREATE INDEX IF NOT EXISTS slack_dm_installations_active_idx
  ON slack_dm_installations (tenant_id, team_id) WHERE disabled_at IS NULL;

-- ---------------------------------------------------------------------
-- RLS — mirror the mercury_* / jira_* tenant_isolation template.
-- ---------------------------------------------------------------------
ALTER TABLE slack_dm_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE slack_dm_installations FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'slack_dm_installations'
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

COMMIT;
