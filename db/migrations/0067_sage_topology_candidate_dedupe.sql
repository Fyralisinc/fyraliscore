-- 0067_sage_topology_candidate_dedupe.sql
--
-- The SAGE topology optimizer may be invoked concurrently for the same
-- inquiry/session. Deduping review candidates with a read-before-insert
-- query is not a durable invariant under concurrency, so enforce one open
-- review row per optimizer canonical_op_key at the database boundary.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS relationship_candidates_sage_topology_op_key_idx
  ON relationship_candidates (
    tenant_id,
    source,
    (metadata->>'canonical_op_key')
  )
  WHERE source = 'sage_topology_optimizer'
    AND metadata ? 'canonical_op_key'
    AND review_status IN ('candidate', 'needs_review', 'accepted');

COMMIT;
