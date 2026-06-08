-- 0094_telegram.sql
--   IN-TELEGRAM — Telegram as an ingestion source (MTProto user-account API).
--
-- Telegram is the 12th ingestion source. Unlike the Bot API (token + HTTP
-- webhook, but NO historical message access), the MTProto USER-ACCOUNT API can
-- page a dialog's history via messages.getHistory ("Only users can use this
-- method") and receives live updates over a PERSISTENT push connection (there is
-- no HTTP webhook for MTProto). See ADR-0003 and docs/ingestion/sources/telegram.md.
--
-- CREDENTIAL MODEL: the durable secret is a per-DC auth_key, negotiated once via
-- Diffie-Hellman and never sent on the wire. We persist it as a Telethon
-- StringSession in encrypted_secrets and reference it by secret_ref — NOT a bot
-- token. Two independent authorizations are minted per account (ADR-0003 §6,
-- "Topology B"): a LIVE session owned by the telegram_gateway_worker (persistent
-- updates connection) and a BACKFILL session owned by the per-account backfill
-- path (multiplexed messages.getHistory). They share the account-wide FLOOD_WAIT
-- budget but never share one auth_key across processes.
--
-- TABLES (mirror the Jira/Mercury install + per-resource-cursor shape, plus a
-- live-state table):
--   telegram_installations — one row per (tenant, account).
--   telegram_dialogs       — per-dialog BACKFILL cursor (the jira_projects analog);
--                             one shard per dialog, cursored on offset_id.
--   telegram_update_state  — per-install LIVE update state (pts/qts/seq/date +
--                             per-channel pts), the getDifference cursor.
--
-- DUAL EDGE:
--   - BACKFILL (pull): messages.getHistory paged on offset_id/add_offset/limit;
--     the oldest returned message id becomes the next page's offset_id. Cursor
--     persisted on telegram_dialogs.offset_id_cursor (BACKFILL session).
--   - LIVE (push): the persistent updates connection delivers updateNewMessage
--     etc.; gap recovery via updates.getDifference / updates.getChannelDifference
--     against the pts/qts/seq/date state on telegram_update_state (LIVE session).
--     Live updates shadow-write to ingestion.raw.telegram (ingress_kind=gateway)
--     so they flow through the SAME normalizer->observation_writer chain as
--     backfill — landing in observations while backfill is still in flight.
--
-- Plus the source-registry CHECK widening every new ingestion source needs: the
-- M6 substrate pins allowed `source` values with an inline CHECK on FOUR tables
-- (last widened by 0081 to add 'grafana'; here we add 'telegram'):
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry ALL prior sources forward (a strict superset) so
-- applying this migration last cannot drop an existing source from the set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS is additive; the CHECK widening
--     admits a strict superset of the prior allowed set, so no existing row can
--     violate it — non-destructive, no staged plan required.
--   - Idempotent: tables guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added with the same Postgres-assigned inline name
--     (`<table>_source_check`). Re-running is a no-op.
--   - Tenant isolation (§III): each new table ENABLEs + FORCEs RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     jira_installations / mercury_installations policy template (0073 / 0074).

BEGIN;

-- ---------------------------------------------------------------------
-- telegram_installations — one row per (tenant, account)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telegram_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Human-facing account identity (phone in E.164 or @username) for display +
  -- the per-tenant uniqueness key. Not a credential.
  account_label TEXT NOT NULL,
  -- MTProto application credentials (https://my.telegram.org). api_id is public;
  -- api_hash is a secret held in encrypted_secrets and referenced here.
  api_id TEXT,
  api_hash_secret_ref TEXT,
  -- Opaque pointer into encrypted_secrets for the LIVE Telethon StringSession
  -- (the persisted auth_key the gateway worker's updates connection uses).
  session_secret_ref TEXT,
  -- Opaque pointer into encrypted_secrets for the BACKFILL StringSession — a
  -- SECOND authorization on the same account (ADR-0003 Topology B) so the
  -- backfill getHistory sweeps never share the live connection's auth_key.
  backfill_session_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, account_label)
);

CREATE INDEX IF NOT EXISTS telegram_installations_tenant_idx
  ON telegram_installations (tenant_id);

CREATE INDEX IF NOT EXISTS telegram_installations_active_idx
  ON telegram_installations (tenant_id) WHERE disabled_at IS NULL;

-- ---------------------------------------------------------------------
-- telegram_dialogs — per-dialog backfill cursor (one shard per dialog)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telegram_dialogs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  telegram_installation_id UUID NOT NULL
    REFERENCES telegram_installations(id) ON DELETE CASCADE,
  -- MTProto peer id of the dialog (user/chat/channel). int64 -> BIGINT.
  dialog_id BIGINT NOT NULL,
  dialog_kind TEXT NOT NULL CHECK (dialog_kind IN ('user', 'chat', 'channel')),
  -- access_hash needed to address channels/users in MTProto (int64). Nullable
  -- for basic chats which don't carry one.
  access_hash BIGINT,
  title TEXT,
  -- Backfill high-water: the OLDEST message id reached as we page toward the
  -- start of history (messages.getHistory offset_id). NULL until the first page.
  -- The next page requests offset_id = this value; backfill is done when a page
  -- returns no older messages.
  offset_id_cursor BIGINT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
    CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (telegram_installation_id, dialog_id)
);

CREATE INDEX IF NOT EXISTS telegram_dialogs_install_idx
  ON telegram_dialogs (telegram_installation_id);

-- ---------------------------------------------------------------------
-- telegram_update_state — per-install LIVE update state (getDifference cursor)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telegram_update_state (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  telegram_installation_id UUID NOT NULL UNIQUE
    REFERENCES telegram_installations(id) ON DELETE CASCADE,
  -- The common event-sequence state (private chats + basic groups), reconciled
  -- by updates.getDifference. pts/qts/seq are int4 in the protocol; stored as
  -- BIGINT for forward-headroom. update_date = the updates `date` field (epoch s).
  pts BIGINT,
  qts BIGINT,
  seq BIGINT,
  update_date BIGINT,
  -- Per-channel pts (channels/supergroups have their own sequences), reconciled
  -- by updates.getChannelDifference. JSON object keyed by channel id -> pts.
  channel_pts JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- RLS — mirror the jira_installations / mercury_installations template.
-- ---------------------------------------------------------------------
ALTER TABLE telegram_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE telegram_installations FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS telegram_installations_tenant_isolation ON telegram_installations;
CREATE POLICY telegram_installations_tenant_isolation ON telegram_installations
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

ALTER TABLE telegram_dialogs ENABLE ROW LEVEL SECURITY;
ALTER TABLE telegram_dialogs FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS telegram_dialogs_tenant_isolation ON telegram_dialogs;
CREATE POLICY telegram_dialogs_tenant_isolation ON telegram_dialogs
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

ALTER TABLE telegram_update_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE telegram_update_state FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS telegram_update_state_tenant_isolation ON telegram_update_state;
CREATE POLICY telegram_update_state_tenant_isolation ON telegram_update_state
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

-- ---------------------------------------------------------------------
-- Source-registry CHECK widening — admit 'telegram' on all four M6 substrate
-- tables. Carries every prior source forward (strict superset of 0081's list)
-- so applying this migration last does not drop any of them.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram'));

COMMIT;
