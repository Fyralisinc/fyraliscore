-- 0161_post_commit_projection_materialization_action.sql
--
-- Allow the post-commit worker to materialize projection snapshots from
-- committed Model events.

BEGIN;

DO $$
DECLARE
  existing_check TEXT;
BEGIN
  IF to_regclass('pending_post_commit_actions') IS NULL THEN
    RETURN;
  END IF;

  SELECT conname
  INTO existing_check
  FROM pg_constraint
  WHERE conrelid = 'pending_post_commit_actions'::regclass
    AND contype = 'c'
    AND pg_get_constraintdef(oid) LIKE '%action_kind%'
  LIMIT 1;

  IF existing_check IS NOT NULL THEN
    EXECUTE format(
      'ALTER TABLE pending_post_commit_actions DROP CONSTRAINT %I',
      existing_check
    );
  END IF;
END $$;

ALTER TABLE pending_post_commit_actions
  ADD CONSTRAINT pending_post_commit_actions_action_kind_check
  CHECK (
    action_kind IN (
      'publish_anomalies',
      'schedule_predictions',
      'broadcast_realtime',
      'invalidate_metrics',
      'materialize_projections',
      'discover_model_edges'
    )
  );

COMMIT;
