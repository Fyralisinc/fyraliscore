-- =====================================================================
-- 0044_fix_installation_audit_log_action_check.sql
--   IN-13 follow-up: actually widen the audit_log.action CHECK
-- =====================================================================
-- Migration 0043 attempted to widen the CHECK constraint via a DO block
-- guarded by an ILIKE pattern match against `pg_get_constraintdef`. The
-- guard failed because Postgres renders CHECK definitions as
-- `action = ANY (ARRAY[...])` not `action IN (...)`, so the DROP path
-- was skipped and the second DO's IF-NOT-EXISTS guard then prevented
-- the ADD. Net effect: 0043 was a silent no-op.
--
-- This migration replaces the constraint unconditionally:
--   - DROP CONSTRAINT IF EXISTS (safe even if 0043 succeeded)
--   - ADD CONSTRAINT with the full widened set
--
-- Discovered during the IN-13 OAuth-callback rehearsal: a real
-- callback with `setup_action='update'` failed to write its audit row
-- because action='update' is not in the legacy IN-08 vocabulary.
-- Surface: gateway log line `github_oauth_audit_failed action=update`.
--
-- Constitution alignment:
--   §II — additive in effect (widens the allowed set; preserves all
--         prior rows). Idempotent via DROP IF EXISTS.
-- =====================================================================

BEGIN;

ALTER TABLE installation_audit_log
    DROP CONSTRAINT IF EXISTS installation_audit_log_action_check;

ALTER TABLE installation_audit_log
    ADD CONSTRAINT installation_audit_log_action_check
    CHECK (action IN (
        'install',
        'reinstall',
        'update',
        'uninstall',
        'suspend',
        'unsuspend',
        'repo_change',
        'token_refresh',
        'installation_created_noop',
        'repository_fetch_failed',
        'rejected_collision'
    ));

COMMIT;
