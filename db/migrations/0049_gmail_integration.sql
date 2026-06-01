-- 0049_gmail_integration.sql
-- specs/003-gmail-integration/plan.md
--
-- Workspace DWD-only Gmail integration v1. Self-contained: the FK from
-- gmail_installations.installation_id → provider_installations(id) is
-- deferred until IN-13 (provider_installations + installation_audit_log)
-- lands. A future migration re-attaches that FK; v1 carries the same
-- conceptual columns inline.
--
-- Tables created here:
--   gmail_installations      — one row per (tenant, workspace_domain)
--   gmail_install_audit      — per-install action log (mini installation_audit_log)
--   gmail_pubsub_topics      — per-tenant topic + subscription names
--   gmail_mailbox_watches    — one row per actively-watched mailbox
--   gmail_mailbox_optouts    — per-user opt-out store
--   gmail_threads_canonical  — canonical RFC 5322 thread record
--   gmail_thread_members     — message_id → canonical thread lookup
--   gmail_read_audit         — append-only per-message read attestation log
--
-- Plus a nullable column on observations:
--   observations.thread_canonical_id  — Gmail-specific thread linkage
--
-- All gmail_* tables enforce RLS via app.current_tenant (see
-- lib/shared/tenant_context.py). Policies mirror future
-- provider_installations policies.

BEGIN;

-- ---------------------------------------------------------------------
-- gmail_installations
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- installation_id is the FK target once provider_installations exists.
  -- v1 nullable so connect can complete without IN-13 infra.
  installation_id UUID,
  workspace_domain TEXT NOT NULL,
  service_account_email TEXT NOT NULL,
  scope TEXT NOT NULL CHECK (scope IN ('gmail.metadata', 'gmail.readonly')),
  inclusion_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
  resolved_user_count INTEGER NOT NULL DEFAULT 0,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, workspace_domain)
);

CREATE INDEX IF NOT EXISTS gmail_installations_tenant_idx
  ON gmail_installations (tenant_id);

-- ---------------------------------------------------------------------
-- gmail_install_audit — mini audit log until IN-13 lands installation_audit_log
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_install_audit (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID REFERENCES gmail_installations(id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  -- 'gmail.install', 'gmail.scope_changed', 'gmail.inclusion_updated',
  -- 'gmail.disabled', 'gmail.optout_added', 'gmail.optout_removed'
  actor_email TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gmail_install_audit_install_idx
  ON gmail_install_audit (gmail_installation_id, occurred_at DESC);

-- ---------------------------------------------------------------------
-- gmail_pubsub_topics
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_pubsub_topics (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  topic_name TEXT NOT NULL UNIQUE,
  subscription_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  teardown_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS gmail_pubsub_subscription_idx
  ON gmail_pubsub_topics (subscription_name)
  WHERE teardown_at IS NULL;

-- ---------------------------------------------------------------------
-- gmail_mailbox_watches
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_mailbox_watches (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  email_address TEXT NOT NULL,
  google_user_id TEXT,
  history_id TEXT,
  watch_expiration TIMESTAMPTZ,
  last_push_at TIMESTAMPTZ,
  last_poll_at TIMESTAMPTZ,
  consecutive_poll_failures INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'pending'
       CHECK (state IN ('pending', 'active', 'paused', 'opted_out', 'errored')),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (gmail_installation_id, email_address)
);

CREATE INDEX IF NOT EXISTS gmail_watches_expiry_idx
  ON gmail_mailbox_watches (watch_expiration)
  WHERE state = 'active';
CREATE INDEX IF NOT EXISTS gmail_watches_poll_idx
  ON gmail_mailbox_watches (last_poll_at NULLS FIRST)
  WHERE state = 'active';
CREATE INDEX IF NOT EXISTS gmail_watches_email_idx
  ON gmail_mailbox_watches (email_address)
  WHERE state = 'active';

-- ---------------------------------------------------------------------
-- gmail_mailbox_optouts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_mailbox_optouts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  email_address TEXT NOT NULL,
  reason TEXT,
  opted_out_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (gmail_installation_id, email_address)
);

-- ---------------------------------------------------------------------
-- gmail_threads_canonical
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_threads_canonical (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  canonical_message_id TEXT NOT NULL,
  subject_normalized TEXT,
  participant_emails TEXT[] NOT NULL DEFAULT '{}',
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  message_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (gmail_installation_id, canonical_message_id)
);

CREATE INDEX IF NOT EXISTS gmail_threads_participants_idx
  ON gmail_threads_canonical USING GIN (participant_emails);
CREATE INDEX IF NOT EXISTS gmail_threads_last_seen_idx
  ON gmail_threads_canonical (gmail_installation_id, last_seen_at DESC);

-- ---------------------------------------------------------------------
-- gmail_thread_members
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_thread_members (
  message_id TEXT NOT NULL,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  thread_canonical_id UUID NOT NULL REFERENCES gmail_threads_canonical(id) ON DELETE CASCADE,
  PRIMARY KEY (gmail_installation_id, message_id)
);

-- ---------------------------------------------------------------------
-- gmail_read_audit
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gmail_read_audit (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  gmail_installation_id UUID NOT NULL REFERENCES gmail_installations(id) ON DELETE CASCADE,
  email_address TEXT NOT NULL,
  message_id TEXT NOT NULL,
  scope_used TEXT NOT NULL,
  read_path TEXT NOT NULL CHECK (read_path IN ('push', 'poll')),
  read_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gmail_read_audit_lookup_idx
  ON gmail_read_audit (gmail_installation_id, email_address, read_at DESC);

-- ---------------------------------------------------------------------
-- observations.thread_canonical_id (nullable — non-Gmail rows unaffected)
-- ---------------------------------------------------------------------
ALTER TABLE observations
  ADD COLUMN IF NOT EXISTS thread_canonical_id UUID;

CREATE INDEX IF NOT EXISTS observations_thread_canonical_idx
  ON observations (thread_canonical_id)
  WHERE thread_canonical_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------
ALTER TABLE gmail_installations        ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_installations        FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_install_audit        ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_install_audit        FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_pubsub_topics        ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_pubsub_topics        FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_mailbox_watches      ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_mailbox_watches      FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_mailbox_optouts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_mailbox_optouts      FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_threads_canonical    ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_threads_canonical    FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_thread_members       ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_thread_members       FORCE  ROW LEVEL SECURITY;
ALTER TABLE gmail_read_audit           ENABLE ROW LEVEL SECURITY;
ALTER TABLE gmail_read_audit           FORCE  ROW LEVEL SECURITY;

-- Policy template: every gmail_* table requires app.current_tenant to
-- match tenant_id. The poll path (push_handler, scheduler, poller) does
-- its lookup-then-bind dance before any tenant-scoped read.
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'gmail_installations',
    'gmail_install_audit',
    'gmail_pubsub_topics',
    'gmail_mailbox_watches',
    'gmail_mailbox_optouts',
    'gmail_threads_canonical',
    'gmail_thread_members',
    'gmail_read_audit'
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
