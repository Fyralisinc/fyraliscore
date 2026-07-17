-- Durable contract for evaluation-only Think runs.  These runs reason and
-- validate normally but intentionally apply no semantic mutations, so their
-- proposals must never be stored in ops_applied.

ALTER TABLE think_runs
  ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'normal';

ALTER TABLE think_runs
  ADD COLUMN IF NOT EXISTS validation_result JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'think_runs_execution_mode_check'
  ) THEN
    ALTER TABLE think_runs
      ADD CONSTRAINT think_runs_execution_mode_check
      CHECK (execution_mode IN ('normal', 'validate_only'));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'think_runs_validate_only_result_check'
  ) THEN
    ALTER TABLE think_runs
      ADD CONSTRAINT think_runs_validate_only_result_check
      CHECK (
        (execution_mode = 'normal' AND validation_result IS NULL)
        OR
        (execution_mode = 'validate_only' AND validation_result IS NOT NULL
          AND ops_applied IS NULL)
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS think_runs_validate_only_tenant_time_idx
  ON think_runs (tenant_id, started_at DESC)
  WHERE execution_mode = 'validate_only';
