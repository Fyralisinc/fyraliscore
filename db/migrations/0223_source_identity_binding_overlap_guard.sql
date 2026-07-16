-- =====================================================================
-- 0223_source_identity_binding_overlap_guard.sql
--
-- Prevent two transaction-current source identity bindings for the same
-- tenant/source-native identity from claiming overlapping valid time.
-- This is a database invariant so direct SQL and future independent writers
-- receive the same protection as SourceIdentityBindingRepo callers.
-- =====================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'source_identity_bindings_no_valid_time_overlap'
      AND conrelid = 'source_identity_bindings'::regclass
  ) THEN
    ALTER TABLE source_identity_bindings
      ADD CONSTRAINT source_identity_bindings_no_valid_time_overlap
      EXCLUDE USING gist (
        tenant_id WITH =,
        source_system WITH =,
        source_native_identifier WITH =,
        tstzrange(valid_from, valid_to, '[)') WITH &&
      )
      WHERE (transaction_to IS NULL);
  END IF;
END
$$;

COMMENT ON CONSTRAINT source_identity_bindings_no_valid_time_overlap
  ON source_identity_bindings IS
  'Transaction-current bindings for one tenant/source identity must have disjoint valid-time intervals.';

COMMIT;
