-- =====================================================================
-- 0146_post_commit_edge_discovery_action.sql
-- =====================================================================
-- Add the post-commit action that runs latent topology discovery for
-- Models created or updated by Think after the apply transaction commits.
-- =====================================================================

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
      'discover_model_edges'
    )
  );

COMMIT;
