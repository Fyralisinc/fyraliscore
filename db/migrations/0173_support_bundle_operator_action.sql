-- =====================================================================
-- 0173_support_bundle_operator_action.sql
--   Bounded audit action for sanitized support bundle export
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
      'source_installation.secret.rotate',
      'queue_depth.inspect',
      'support_bundle.export'
    )
  );

COMMIT;
