-- 0100_signal.sql
--   IN-SIGNAL — Signal as an ingestion source (linked-device messaging).
--
-- Signal is one of the final ingestion sources. Like Telegram (its archetype),
-- it is a USER-ACCOUNT / LINKED-DEVICE messaging surface (not a bot/webhook API):
-- a linked device can page a thread's message history for backfill AND receives
-- live messages over a PERSISTENT linked-device session (there is no HTTP webhook
-- for Signal). See ADR-0003 and the telegram source it clones.
--
-- CREDENTIAL MODEL: the durable secret is a per-device libsignal registration
-- (the identity/session store negotiated when the device is LINKED). We persist
-- it in encrypted_secrets and reference it by secret_ref — NOT a token. Two
-- independent linked devices are minted per account (ADR-0003 §6, "Topology B"):
-- a LIVE session owned by the signal_gateway_worker (persistent receive loop) and
-- a BACKFILL session owned by the per-account backfill path (history replay).
-- They never share one device registration across processes.
--
-- COVERAGE: own/linked-account only — a linked Signal device sees only the
-- threads its account participates in (self-coverage, like Telegram's
-- user-account session).
--
-- TABLES (mirror the Telegram install + per-thread-cursor shape, plus a
-- live-state table):
--   signal_installations — one row per (tenant, account).
--   signal_threads       — per-thread BACKFILL cursor (the telegram_dialogs
--                            analog); one shard per thread, cursored on offset_id.
--   signal_update_state  — per-install LIVE update state (the sync cursor).
--
-- DUAL EDGE:
--   - BACKFILL (pull): thread history paged on offset_id/limit; the oldest
--     returned message id becomes the next page's offset_id. Cursor persisted on
--     signal_threads.offset_id_cursor (BACKFILL linked device).
--   - LIVE (push): the persistent linked-device session delivers new messages;
--     gap recovery via sync replay against the cursor on signal_update_state
--     (LIVE linked device). Live messages shadow-write to ingestion.raw.signal
--     (ingress_kind=gateway) so they flow through the SAME
--     normalizer->observation_writer chain as backfill — landing in observations
--     while backfill is still in flight.
--
-- Plus the source-registry CHECK widening every new ingestion source needs: the
-- M6 substrate pins allowed `source` values with an inline CHECK on FOUR tables.
-- Here we add 'signal' AND carry the full canonical source set forward:
--   - source_onboarding_runs
--   - onboarding_shards
--   - ingestion_failures
--   - onboarding_triggers
-- The CHECK lists below carry ALL sources (a strict superset) so applying this
-- migration last cannot drop an existing source from the set.
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
--     telegram_installations / telegram_dialogs policy template (0094).

BEGIN;

-- ---------------------------------------------------------------------
-- signal_installations — one row per (tenant, account)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Human-facing account identity (E.164 number or account uuid) for display +
  -- the per-tenant uniqueness key. Not a credential.
  account_label TEXT NOT NULL,
  -- Opaque pointer into encrypted_secrets for the LIVE linked-device session
  -- (the libsignal registration the gateway worker's receive loop uses).
  session_secret_ref TEXT,
  -- Opaque pointer into encrypted_secrets for the BACKFILL linked-device session
  -- — a SECOND linked device on the same account (ADR-0003 Topology B) so the
  -- backfill history sweeps never share the live session's registration.
  backfill_session_secret_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, account_label)
);

CREATE INDEX IF NOT EXISTS signal_installations_tenant_idx
  ON signal_installations (tenant_id);

CREATE INDEX IF NOT EXISTS signal_installations_active_idx
  ON signal_installations (tenant_id) WHERE disabled_at IS NULL;

-- ---------------------------------------------------------------------
-- signal_threads — per-thread backfill cursor (one shard per thread)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_threads (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  signal_installation_id UUID NOT NULL
    REFERENCES signal_installations(id) ON DELETE CASCADE,
  -- Numeric thread id of the conversation (direct or group). int64 -> BIGINT.
  thread_id BIGINT NOT NULL,
  thread_kind TEXT NOT NULL CHECK (thread_kind IN ('direct', 'group')),
  title TEXT,
  -- Backfill high-water: the OLDEST message id reached as we page toward the
  -- start of history. NULL until the first page. The next page requests
  -- offset_id = this value; backfill is done when a page returns no older
  -- messages.
  offset_id_cursor BIGINT,
  last_synced_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'active'
    CHECK (state IN ('pending', 'active', 'paused', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (signal_installation_id, thread_id)
);

CREATE INDEX IF NOT EXISTS signal_threads_install_idx
  ON signal_threads (signal_installation_id);

-- ---------------------------------------------------------------------
-- signal_update_state — per-install LIVE update state (sync cursor)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_update_state (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  signal_installation_id UUID NOT NULL UNIQUE
    REFERENCES signal_installations(id) ON DELETE CASCADE,
  -- The advancing live receive cursor (the last delivered message timestamp /
  -- sync position), reconciled by the linked-device sync replay on reconnect.
  sync_cursor BIGINT,
  update_date BIGINT,
  -- Per-thread sync position headroom (groups can advance independently). JSON
  -- object keyed by thread id -> cursor.
  thread_cursors JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- RLS — mirror the telegram_installations / telegram_dialogs template.
-- ---------------------------------------------------------------------
ALTER TABLE signal_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_installations FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS signal_installations_tenant_isolation ON signal_installations;
CREATE POLICY signal_installations_tenant_isolation ON signal_installations
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

ALTER TABLE signal_threads ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_threads FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS signal_threads_tenant_isolation ON signal_threads;
CREATE POLICY signal_threads_tenant_isolation ON signal_threads
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

ALTER TABLE signal_update_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_update_state FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS signal_update_state_tenant_isolation ON signal_update_state;
CREATE POLICY signal_update_state_tenant_isolation ON signal_update_state
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

-- ---------------------------------------------------------------------
-- Source-registry CHECK widening — admit 'signal' on all four M6 substrate
-- tables. Carries the full canonical source set forward (a strict superset) so
-- applying this migration last does not drop any of them.
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
