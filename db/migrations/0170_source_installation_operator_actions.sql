-- =====================================================================
-- 0170_source_installation_operator_actions.sql
--   Bounded audit actions for source installation operator CLI
-- =====================================================================

BEGIN;

ALTER TABLE operator_action_log
  DROP CONSTRAINT IF EXISTS operator_action_log_action_check;

ALTER TABLE operator_action_log
  ADD CONSTRAINT operator_action_log_action_check CHECK (
    action IN (
      'dead_letter.list',
      'dead_letter.retry',
      'dead_letter.quarantine',
      'role.grant',
      'role.revoke',
      'source_installation.status',
      'source_installation.pause',
      'source_installation.resume',
      'queue_depth.inspect'
    )
  );

COMMIT;
