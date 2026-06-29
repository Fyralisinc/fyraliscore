-- =====================================================================
-- 0158_backup_recovery_status.sql
-- =====================================================================
-- Current backup/restore status contract for external backup automation.
--
-- Fyralis core does not own every deployment's cloud backup engine, but it
-- does own the production-readiness contract: backup jobs and restore
-- rehearsals must report their latest status in a consistent, observable
-- shape that housekeeper can turn into bounded Prometheus metrics.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS backup_recovery_status (
  component TEXT NOT NULL,
  check_name TEXT NOT NULL,
  status TEXT NOT NULL,
  last_success_at TIMESTAMPTZ,
  last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  freshness_slo_seconds INTEGER NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (component, check_name),
  CONSTRAINT backup_recovery_status_component_check CHECK (
    component IN (
      'postgres',
      'object_store',
      'broker',
      'secrets',
      'application_config'
    )
  ),
  CONSTRAINT backup_recovery_status_check_name_check CHECK (
    check_name IN ('backup', 'restore_test', 'inventory')
  ),
  CONSTRAINT backup_recovery_status_status_check CHECK (
    status IN ('ok', 'failed', 'unknown')
  ),
  CONSTRAINT backup_recovery_status_slo_positive_check CHECK (
    freshness_slo_seconds > 0
  )
);

CREATE INDEX IF NOT EXISTS backup_recovery_status_updated_idx
  ON backup_recovery_status (updated_at DESC);

CREATE INDEX IF NOT EXISTS backup_recovery_status_success_idx
  ON backup_recovery_status (component, check_name, last_success_at DESC);

COMMIT;
