-- Mechanically fence canonical Model truth writes behind one command service.
--
-- PostgreSQL custom settings are not a security boundary against malicious SQL
-- executed by the same database role. They are, however, a transaction-local
-- capability that makes every accidental direct INSERT/UPDATE fail closed.
-- Static ratchets below the DB layer ensure only TruthKernelService may mint it.

BEGIN;

CREATE OR REPLACE FUNCTION require_truth_kernel_command_authority()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  capability text := NULLIF(
    current_setting('app.truth_kernel_command', true), ''
  );
BEGIN
  IF capability IS NULL OR capability !~
     '^model:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'canonical Model truth writes require TruthKernelService command authority'
      USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END;
$$;

DO $$
DECLARE
  guarded_table text;
BEGIN
  FOREACH guarded_table IN ARRAY ARRAY[
    'truth_candidates',
    'truth_admission_decisions',
    'model_truth_versions',
    'model_truth_lifecycle_events',
    'model_truth_heads',
    'model_truth_evidence_references',
    'model_truth_scope_bindings',
    'model_truth_scope_evidence'
  ]
  LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS %I_command_authority ON %I',
      guarded_table, guarded_table
    );
    EXECUTE format(
      'CREATE TRIGGER %I_command_authority BEFORE INSERT OR UPDATE ON %I '
      'FOR EACH ROW EXECUTE FUNCTION require_truth_kernel_command_authority()',
      guarded_table, guarded_table
    );
  END LOOP;
END $$;

COMMENT ON FUNCTION require_truth_kernel_command_authority() IS
  'Fail-closed accidental-write capability. Same-role malicious SQL is outside this boundary; static registry forbids other capability minters.';

COMMIT;
