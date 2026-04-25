-- 0020_think_run_artifacts.sql
--
-- Sidecar capture of every Think pipeline stage so the /debug UI can
-- show the end-to-end processing log for each signal. Gated at write
-- time by the DEBUG_ARTIFACT_CAPTURE env flag (default on in dev).

BEGIN;

CREATE TABLE IF NOT EXISTS think_run_artifacts (
    id            uuid NOT NULL,
    run_id        uuid NOT NULL,
    tenant_id     uuid NOT NULL,
    stage         text NOT NULL,
    payload       jsonb NOT NULL,
    captured_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT think_run_artifacts_stage_check
        CHECK (stage IN (
            'trigger',
            'retrieval',
            'prompt',
            'response',
            'validation',
            'apply',
            'post_commit',
            'cascade',
            'error'
        ))
);

CREATE INDEX IF NOT EXISTS think_run_artifacts_run_idx
    ON think_run_artifacts (run_id, captured_at);

CREATE INDEX IF NOT EXISTS think_run_artifacts_tenant_time_idx
    ON think_run_artifacts (tenant_id, captured_at DESC);

COMMIT;
