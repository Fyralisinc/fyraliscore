-- =====================================================================
-- 0092_sage_affordance_context_lookup.sql
--
-- Hot-path index for contextual affordance retrieval. The reader uses
-- activation_signatures to keep high-utility but irrelevant profiles
-- from crowding out the right Model in large tenants.
-- =====================================================================

BEGIN;

CREATE INDEX IF NOT EXISTS retrieval_affordance_profiles_activation_signatures_idx
  ON retrieval_affordance_profiles USING GIN (activation_signatures);

COMMIT;
