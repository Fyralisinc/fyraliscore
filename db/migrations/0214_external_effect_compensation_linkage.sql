-- 0214_external_effect_compensation_linkage.sql
--
-- Bind compensation fates to one separate spec, authorization, attempt, and receipt.

ALTER TABLE external_effect_attempt_heads
  ADD COLUMN IF NOT EXISTS current_compensation_spec_digest TEXT;
ALTER TABLE external_effect_attempt_heads
  ADD COLUMN IF NOT EXISTS current_compensation_authorization_decision_id UUID;
ALTER TABLE external_effect_attempt_heads
  ADD COLUMN IF NOT EXISTS current_compensation_attempt_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'external_effect_compensation_spec_digest_check'
  ) THEN
    ALTER TABLE external_effect_attempt_heads
      ADD CONSTRAINT external_effect_compensation_spec_digest_check
      CHECK (
        current_compensation_spec_digest IS NULL
        OR current_compensation_spec_digest ~ '^[0-9a-f]{64}$'
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'external_effect_compensation_state_binding_check'
  ) THEN
    ALTER TABLE external_effect_attempt_heads
      ADD CONSTRAINT external_effect_compensation_state_binding_check
      CHECK (
        (current_state NOT IN (
          'compensation_proposed', 'compensation_authorized',
          'compensation_rejected', 'compensation_expired',
          'compensation_attempt_linked', 'compensated',
          'compensation_failed', 'compensation_unknown',
          'compensation_reconciling'
        ))
        OR (
          current_compensation_spec_digest IS NOT NULL
          AND (
            current_state IN (
              'compensation_proposed', 'compensation_rejected',
              'compensation_expired'
            )
            OR current_compensation_authorization_decision_id IS NOT NULL
          )
          AND (
            current_state IN (
              'compensation_proposed', 'compensation_authorized',
              'compensation_rejected', 'compensation_expired'
            )
            OR current_compensation_attempt_id IS NOT NULL
          )
        )
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'external_effect_compensation_spec_fk'
  ) THEN
    ALTER TABLE external_effect_attempt_heads
      ADD CONSTRAINT external_effect_compensation_spec_fk
      FOREIGN KEY (tenant_id, current_compensation_spec_digest)
      REFERENCES consequential_intervention_specs (tenant_id, spec_digest)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'external_effect_compensation_authorization_fk'
  ) THEN
    ALTER TABLE external_effect_attempt_heads
      ADD CONSTRAINT external_effect_compensation_authorization_fk
      FOREIGN KEY (current_compensation_authorization_decision_id)
      REFERENCES consequential_authorization_decisions (id)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'external_effect_compensation_attempt_fk'
  ) THEN
    ALTER TABLE external_effect_attempt_heads
      ADD CONSTRAINT external_effect_compensation_attempt_fk
      FOREIGN KEY (tenant_id, current_compensation_attempt_id)
      REFERENCES external_effect_attempt_heads (tenant_id, effect_attempt_id)
      DEFERRABLE INITIALLY DEFERRED;
  END IF;
END $$;

DROP INDEX IF EXISTS external_effect_reconciliation_idx;
CREATE INDEX external_effect_reconciliation_idx
  ON external_effect_attempt_heads (tenant_id, updated_at)
  WHERE current_state IN (
    'unknown', 'reconciling', 'partially_executed',
    'compensation_unknown', 'compensation_reconciling'
  );
