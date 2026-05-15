-- =====================================================================
-- 0043_widen_installation_audit_log_actions.sql
--   IN-13: widen installation_audit_log.action CHECK to cover the
--   GitHub lifecycle vocabulary
-- =====================================================================
-- Migration 0041 created `installation_audit_log` with a CHECK
-- constraint pinning `action` to {install, uninstall, token_refresh,
-- rejected_collision} — the IN-08 Slack vocabulary. IN-09 reused those
-- four. IN-13 needs additional GitHub-specific transitions:
--
--   - reinstall                       (US2.5: same installation_id +
--                                      tenant flips enabled FALSE → TRUE)
--   - update                          (FR-004: setup_action='update'
--                                      refreshes selected_repositories)
--   - suspend                         (FR-009: installation.action='suspend')
--   - unsuspend                       (FR-009: installation.action='unsuspend')
--   - repo_change                     (FR-010: installation_repositories.*)
--   - installation_created_noop       (FR-009: marketplace direct-install
--                                      where the row exists; logged for
--                                      forensic traceability)
--   - repository_fetch_failed         (R9: callback succeeded but
--                                      GET /installation/repositories
--                                      failed; selected_repositories
--                                      stays NULL with context flag)
--
-- The constraint is DROPped then re-ADDed with the widened set; both
-- statements idempotent. No existing rows are invalidated (the new
-- vocabulary is a strict superset of the prior set).
--
-- Constitution alignment:
--   §II  — additive in effect (widens the allowed set; no row loss).
--          Idempotent: DROP IF EXISTS + ADD CONSTRAINT … not
--          re-applicable, so wrap in a DO block that checks the
--          constraint's current `consrc` text to make the migration
--          re-runnable. Following the same idempotent-CHECK pattern as
--          migration 0035 (`proposition_kind_constraints`).
-- =====================================================================

BEGIN;

DO $$
BEGIN
    -- Drop the existing CHECK constraint by name if present (matches
    -- the implicit name PG generates from migration 0041).
    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'installation_audit_log'::regclass
           AND contype = 'c'
           AND pg_get_constraintdef(oid)
               ILIKE '%action%IN%''install''%uninstall''%token_refresh''%'
    ) THEN
        ALTER TABLE installation_audit_log
            DROP CONSTRAINT IF EXISTS installation_audit_log_action_check;
    END IF;
END $$;

-- Add the widened constraint. IF NOT EXISTS not supported on ADD
-- CONSTRAINT pre-PG18, so guard with DO block on existence by name.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'installation_audit_log'::regclass
           AND conname = 'installation_audit_log_action_check'
    ) THEN
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
    END IF;
END $$;

COMMIT;
