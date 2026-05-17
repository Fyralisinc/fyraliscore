-- =====================================================================
-- 0045_onboarding_runs_and_shards.sql
--   Ingestion LLD §1.1 (onboarding_runs) + §1.2 (onboarding_shards).
-- =====================================================================
-- Backfill substrate. One `onboarding_runs` row per
-- TenantOnboardingWorkflow execution; many `onboarding_shards` rows per
-- run, each the unit of fetch work for a (source, shard_kind).
--
-- Co-located in a single migration because `onboarding_shards` carries
-- an FK to `onboarding_runs.id` and the project's migration runner
-- applies one file at a time; splitting would force an ordering
-- constraint between two files that always ship together.
--
-- Per ingestion LLD §1.1 §1.2:
--   - PK is UUID, allocated app-side (uuid7()).
--   - RLS pattern matches migration 0036 (permissive default) — service
--     code reads with `app.current_tenant` unset; tenant-scoped code
--     sets it before reading.
--   - `recency_score` is denormalized from the planner's exp(-age/τ);
--     pending-recency partial index serves the "next N shards" pull.
-- Constitution alignment:
--   §I — this is per-feature substrate for the backfill capability,
--        not cross-cutting plumbing. Bounded to ingestion concerns.
--   §II — additive; idempotent via IF NOT EXISTS.
--   §III — tenant-scoped triad (FK, RLS, tenant-prefixed index).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- §1.1 onboarding_runs — aggregate progress record.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onboarding_runs (
    id                  UUID         PRIMARY KEY,             -- uuid7() app-side
    tenant_id           UUID         NOT NULL
                                     REFERENCES tenants(id)
                                     DEFERRABLE INITIALLY IMMEDIATE,
    trigger_kind        TEXT         NOT NULL
                                     CHECK (trigger_kind IN (
                                       'install', 'reinstall', 'manual_replay'
                                     )),
    workflow_id         TEXT         NOT NULL,                -- Temporal workflow id (deterministic)
    workflow_run_id     TEXT,                                 -- Temporal run id; NULL until first start
    status              TEXT         NOT NULL DEFAULT 'pending'
                                     CHECK (status IN (
                                       'pending', 'running', 'feels_onboarded',
                                       'complete', 'partial', 'failed', 'cancelled'
                                     )),
    sources_enabled     TEXT[]       NOT NULL,                -- e.g. ['slack','github','gmail']
    started_at          TIMESTAMPTZ,
    feels_onboarded_at  TIMESTAMPTZ,                          -- first source achieving the milestone
    completed_at        TIMESTAMPTZ,
    error_summary       TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, workflow_id)
);

CREATE INDEX IF NOT EXISTS onboarding_runs_tenant_status_idx
    ON onboarding_runs (tenant_id, status);

CREATE INDEX IF NOT EXISTS onboarding_runs_status_started_idx
    ON onboarding_runs (status, started_at DESC)
    WHERE status IN ('pending', 'running');

ALTER TABLE onboarding_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE onboarding_runs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON onboarding_runs;
CREATE POLICY tenant_isolation ON onboarding_runs
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

-- ---------------------------------------------------------------------
-- §1.2 onboarding_shards — unit of fetch work.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS onboarding_shards (
    id                  UUID         PRIMARY KEY,             -- uuid7() app-side
    onboarding_run_id   UUID         NOT NULL
                                     REFERENCES onboarding_runs(id)
                                     ON DELETE CASCADE,
    tenant_id           UUID         NOT NULL,                -- denormalized for RLS + index locality
    source              TEXT         NOT NULL
                                     CHECK (source IN ('slack','github','discord','gmail')),
    shard_kind          TEXT         NOT NULL,                -- per-source: 'channel','repo','mailbox',…
    shard_identifier    JSONB        NOT NULL,                -- per-source-specific (see LLD §3)
    window_start        TIMESTAMPTZ,                          -- inclusive; NULL = "all time"
    window_end          TIMESTAMPTZ,                          -- exclusive; NULL = "until now"
    recency_score       DOUBLE PRECISION NOT NULL,            -- exp(-age_days/7); higher = earlier
    state               TEXT         NOT NULL DEFAULT 'pending'
                                     CHECK (state IN (
                                       'pending', 'in_progress', 'done',
                                       'failed', 'reconciliation_resharded'
                                     )),
    cursor_token        TEXT,                                 -- opaque per-source; advanced atomically
    last_cursor_advance TIMESTAMPTZ,
    pages_fetched       INTEGER      NOT NULL DEFAULT 0,
    observations_seen   INTEGER      NOT NULL DEFAULT 0,      -- normalized count (post-dedup)
    parent_shard_id     UUID         REFERENCES onboarding_shards(id),  -- reconciliation re-shards
    last_error          TEXT,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS onboarding_shards_run_state_idx
    ON onboarding_shards (onboarding_run_id, state, recency_score DESC);

CREATE INDEX IF NOT EXISTS onboarding_shards_tenant_source_idx
    ON onboarding_shards (tenant_id, source, state);

CREATE INDEX IF NOT EXISTS onboarding_shards_pending_recency_idx
    ON onboarding_shards (source, recency_score DESC)
    WHERE state = 'pending';

ALTER TABLE onboarding_shards ENABLE ROW LEVEL SECURITY;
ALTER TABLE onboarding_shards FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON onboarding_shards;
CREATE POLICY tenant_isolation ON onboarding_shards
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
