-- =====================================================================
-- 0195_consequential_agency_immutability.sql
--
-- Forward hardening for databases that installed the 0194 protocol before
-- append-only mutation guards were part of its bootstrap definition.
-- =====================================================================

BEGIN;

CREATE OR REPLACE FUNCTION reject_consequential_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is append-only; corrections require a new governed object',
    TG_TABLE_NAME USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

DO $$
DECLARE
  t TEXT;
  trigger_name TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'consequential_intervention_specs',
    'consequential_predictions',
    'consequential_authorization_decisions',
    'consequential_outcomes',
    'consequential_settlements',
    'consequential_attributions'
  ]
  LOOP
    trigger_name := 'reject_' || t || '_mutation';
    IF to_regclass('public.' || t) IS NOT NULL AND NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgname = trigger_name
        AND tgrelid = to_regclass('public.' || t)
        AND NOT tgisinternal
    ) THEN
      EXECUTE format(
        'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation()',
        trigger_name,
        t
      );
    END IF;
  END LOOP;
END $$;

COMMIT;
