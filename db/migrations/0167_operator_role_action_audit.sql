-- =====================================================================
-- 0167_operator_role_action_audit.sql
-- =====================================================================
-- Role grants and revocations are production operator actions. Extend the
-- bounded operator_action_log action set so role-management CLIs can record
-- auditable tenant-scoped privilege changes.
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
      'role.revoke'
    )
  );

COMMIT;
