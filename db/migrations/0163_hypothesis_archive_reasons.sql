-- =====================================================================
-- 0163_hypothesis_archive_reasons.sql
-- =====================================================================
-- Allow the hypothesis ratification lifecycle to use the archive reasons
-- already emitted by services/product/recommendations/handlers.py.
--
-- destructive-migration-approved: backup=pre-release-schema-snapshot rollback=restore-0160-archive-reason-constraint owner=platform
-- =====================================================================

BEGIN;

ALTER TABLE models DROP CONSTRAINT IF EXISTS models_archive_reason_check;

ALTER TABLE models
  ADD CONSTRAINT models_archive_reason_check
  CHECK (
    archive_reason IS NULL OR archive_reason IN (
      'decay',
      'falsifier_triggered',
      'contested_incorrect',
      'contested_reading_incorrect',
      'superseded',
      'manual',
      'resolved_confirmed',
      'resolved_violated',
      'severe_drift',
      'deprecated',
      'acted_upon',
      'dismissed_by_user',
      'situation_resolved',
      'hypothesis_dismissed_by_user',
      'hypothesis_user_approved',
      'hypothesis_user_corrected',
      'hypothesis_user_other'
    )
  );

ALTER TABLE models VALIDATE CONSTRAINT models_archive_reason_check;

COMMIT;
