-- =====================================================================
-- 0160_model_lifecycle_constraints.sql
-- =====================================================================
-- Promote model lifecycle enums from application-only Pydantic literals to
-- database corruption guards. Raw SQL writes must not create impossible model
-- statuses or archive reasons.
-- =====================================================================

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_status_check'
      AND conrelid = 'models'::regclass
  ) THEN
    ALTER TABLE models
      ADD CONSTRAINT models_status_check
      CHECK (
        status IN (
          'active',
          'archived',
          'superseded',
          'contested_false'
        )
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_archive_reason_check'
      AND conrelid = 'models'::regclass
  ) THEN
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
          'situation_resolved'
        )
      );
  END IF;
END $$;

ALTER TABLE models VALIDATE CONSTRAINT models_status_check;
ALTER TABLE models VALIDATE CONSTRAINT models_archive_reason_check;

COMMIT;
