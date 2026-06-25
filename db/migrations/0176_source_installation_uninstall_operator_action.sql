-- =====================================================================
-- 0176_source_installation_uninstall_operator_action.sql
--   Bounded audit action for generic source-installation uninstall
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
      'role.list',
      'role.grant',
      'role.revoke',
      'source_installation.status',
      'source_installation.pause',
      'source_installation.resume',
      'source_installation.uninstall',
      'source_installation.secret.rotate',
      'queue_depth.inspect',
      'support_bundle.export',
      'kafka_path.reenable'
    )
  );

COMMIT;
