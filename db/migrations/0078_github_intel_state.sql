-- 0078_github_intel_state.sql
--   GitHub Intelligence Layer — current-state model (services/github_intel).
--
-- Dedicated GitHub-state FSMs fed by the existing github:webhook observations.
-- Each entity has a state row keyed by (tenant, repo, entity-number/id). Every
-- row carries the ordering guard fields state_version + last_event_at;
-- transitions apply only when an incoming event's occurred_at >= last_event_at,
-- so out-of-order / replayed / backfilled webhooks never regress live state.
--
-- NOT AN INGESTION SOURCE — emits no observations, touches no source-CHECK
-- tables. See 0077 header.
--
-- §II compliance: append-only, idempotent, tenant-isolation RLS (0061 template).

BEGIN;

-- ---------------------------------------------------------------------
-- github_repo_state — one per (tenant, repo). head_sha is the join key to
-- the code-intel snapshot (the "what code exists now" anchor).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_repo_state (
  tenant_id      UUID NOT NULL REFERENCES tenants(id),
  repo           TEXT NOT NULL,
  default_branch TEXT,
  head_sha       TEXT,
  head_sha_at    TIMESTAMPTZ,
  last_event_at  TIMESTAMPTZ,
  state_version  BIGINT NOT NULL DEFAULT 0,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, repo)
);

-- ---------------------------------------------------------------------
-- github_branch_state — one per (tenant, repo, branch).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_branch_state (
  tenant_id     UUID NOT NULL REFERENCES tenants(id),
  repo          TEXT NOT NULL,
  branch        TEXT NOT NULL,
  head_sha      TEXT,
  is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
  last_push_by  TEXT,
  last_event_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, repo, branch)
);

-- ---------------------------------------------------------------------
-- github_pr_state — two orthogonal FSMs on one row (lifecycle + ci_state).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_pr_state (
  tenant_id        UUID NOT NULL REFERENCES tenants(id),
  repo             TEXT NOT NULL,
  pr_number        INTEGER NOT NULL,
  pr_node_id       TEXT,
  title            TEXT,
  author           TEXT,
  base_ref         TEXT,
  head_ref         TEXT,
  head_sha         TEXT,
  lifecycle        TEXT NOT NULL DEFAULT 'open'
        CHECK (lifecycle IN ('open','draft','review_requested',
                             'changes_requested','approved','merged','closed')),
  ci_state         TEXT NOT NULL DEFAULT 'unknown'
        CHECK (ci_state IN ('unknown','pending','passing','failing','error')),
  merged           BOOLEAN NOT NULL DEFAULT FALSE,
  merge_commit_sha TEXT,
  opened_at        TIMESTAMPTZ,
  closed_at        TIMESTAMPTZ,
  last_event_at    TIMESTAMPTZ,
  state_version    BIGINT NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, repo, pr_number)
);

CREATE INDEX IF NOT EXISTS github_pr_state_node_idx
  ON github_pr_state (tenant_id, pr_node_id);
CREATE INDEX IF NOT EXISTS github_pr_state_lifecycle_idx
  ON github_pr_state (tenant_id, repo, lifecycle);

-- ---------------------------------------------------------------------
-- github_issue_state — one per (tenant, repo, issue_number).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_issue_state (
  tenant_id     UUID NOT NULL REFERENCES tenants(id),
  repo          TEXT NOT NULL,
  issue_number  INTEGER NOT NULL,
  issue_node_id TEXT,
  title         TEXT,
  author        TEXT,
  status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed')),
  last_event_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, repo, issue_number)
);

-- ---------------------------------------------------------------------
-- github_check_state — granular CI rollup feeding github_pr_state.ci_state.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_check_state (
  tenant_id     UUID NOT NULL REFERENCES tenants(id),
  repo          TEXT NOT NULL,
  head_sha      TEXT NOT NULL,
  check_name    TEXT NOT NULL,
  status        TEXT NOT NULL,             -- queued|in_progress|completed
  conclusion    TEXT,                      -- success|failure|cancelled|...
  last_event_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, repo, head_sha, check_name)
);

CREATE INDEX IF NOT EXISTS github_check_state_sha_idx
  ON github_check_state (tenant_id, repo, head_sha);

-- ---------------------------------------------------------------------
-- RLS — tenant_isolation on every table (0061 template).
-- ---------------------------------------------------------------------
ALTER TABLE github_repo_state    ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_repo_state    FORCE  ROW LEVEL SECURITY;
ALTER TABLE github_branch_state  ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_branch_state  FORCE  ROW LEVEL SECURITY;
ALTER TABLE github_pr_state      ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_pr_state      FORCE  ROW LEVEL SECURITY;
ALTER TABLE github_issue_state   ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_issue_state   FORCE  ROW LEVEL SECURITY;
ALTER TABLE github_check_state   ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_check_state   FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'github_repo_state',
    'github_branch_state',
    'github_pr_state',
    'github_issue_state',
    'github_check_state'
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
