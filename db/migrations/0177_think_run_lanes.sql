-- 0177_think_run_lanes.sql — Persist operational Think lane attribution.
--
-- Think lanes route triggers to specialized worker processes, but all lanes
-- still converge through the same validation/apply/post-commit kernel. This
-- column makes per-lane latency, cost joins, validation drops, and retry rates
-- queryable from think_runs.

ALTER TABLE think_runs
  ADD COLUMN IF NOT EXISTS lane TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'think_runs_lane_valid'
  ) THEN
    ALTER TABLE think_runs
      ADD CONSTRAINT think_runs_lane_valid
      CHECK (
        lane IS NULL OR lane IN (
          'reflex',
          'batch_memory',
          'relationship',
          'deep_synthesis',
          'repair'
        )
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS think_runs_lane_time_idx
  ON think_runs (lane, started_at DESC)
  WHERE lane IS NOT NULL;
