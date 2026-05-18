-- =====================================================================
-- 0055_source_onboarding_runs.sql
--   M6.1 — TenantOnboarding → SourceOnboarding handoff anchor.
-- =====================================================================
-- One row per (onboarding_run, source) that the M6.1 TenantOnboarding
-- orchestrator fans out into. M6.2's SourceOnboarding service will
-- claim rows where status='pending' and drive the per-source backfill
-- to completion, emitting `source_onboarding_completed` signals when
-- done. M6.1's orchestrator polls those signals and rolls the parent
-- `onboarding_runs.status` to 'complete' or 'failed' accordingly.
--
-- Schema decisions:
--   - PK is `(onboarding_run_id, source)`. A run has at most one row
--     per source (the prompt's "source_kind" — column named `source`
--     for consistency with `onboarding_shards.source` and
--     `onboarding_runs.sources_enabled[]`; same {slack, github,
--     discord, gmail} domain).
--   - `tenant_id` is denormalized (matches `onboarding_shards`
--     pattern) so RLS policies can index on tenant_id directly without
--     joining `onboarding_runs`. The FK to `onboarding_runs(id)` keeps
--     the relationship honest.
--   - `failure_reason` is TEXT NULLABLE — populated when status=
--     'failed' (the M6.2 service surfaces failures via the
--     `source_onboarding_completed` signal's failure_reason field;
--     the orchestrator copies it here when marking the row failed).
--   - No `state_data JSONB`: M6.2 will likely need per-source
--     orchestration state, but that state lives in `workflow_states`
--     under `(workflow_kind='source_onboarding', workflow_id=<...>)`
--     per the M6.0 substrate pattern. Keeping the M6.1-to-M6.2
--     handoff schema minimal avoids surprise coupling.
--
-- Why this is M6.1 scope, not M6.2:
--   The TenantOnboarding orchestrator (M6.1) WRITES the rows; M6.2's
--   SourceOnboarding service READS them. M6.1 cannot complete without
--   somewhere to write the handoff, so the schema lands with M6.1.
--   M6.2 ships the consumer.
--
-- Constitution alignment:
--   §I — bounded to M6.1's TenantOnboarding → SourceOnboarding seam.
--   §II — additive; idempotent CREATE.
--   §III — tenant-scoped triad (FK via parent + denormalized tenant_id
--          + RLS policy). Same pattern as `onboarding_shards`.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS source_onboarding_runs (
    onboarding_run_id   UUID         NOT NULL
                                     REFERENCES onboarding_runs(id)
                                     ON DELETE CASCADE,
    source              TEXT         NOT NULL
                                     CHECK (source IN ('slack','github','discord','gmail')),
    tenant_id           UUID         NOT NULL
                                     REFERENCES tenants(id),
    status              TEXT         NOT NULL DEFAULT 'pending'
                                     CHECK (status IN (
                                       'pending', 'in_progress',
                                       'completed', 'failed'
                                     )),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    failure_reason      TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (onboarding_run_id, source)
);

-- Hot path: TenantOnboarding orchestrator's completion check —
-- "list source rows for this onboarding_run, count completed vs
-- pending vs failed." Composite (onboarding_run_id, status) is
-- redundant with the PK on the first key but the PK isn't ordered
-- by status; this small index pays off when one run has many
-- sources and the completion check runs every tick.
CREATE INDEX IF NOT EXISTS source_onboarding_runs_run_status_idx
    ON source_onboarding_runs (onboarding_run_id, status);

-- Ops dashboards: "show me all pending source onboardings for this
-- tenant." Tenant_id is denormalized for this index's locality.
CREATE INDEX IF NOT EXISTS source_onboarding_runs_tenant_status_idx
    ON source_onboarding_runs (tenant_id, status);

ALTER TABLE source_onboarding_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_onboarding_runs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON source_onboarding_runs;
CREATE POLICY tenant_isolation ON source_onboarding_runs
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
