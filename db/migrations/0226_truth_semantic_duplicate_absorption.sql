-- Record exact semantic duplicate absorption as an immutable truth receipt.

BEGIN;

ALTER TABLE truth_command_receipts
  DROP CONSTRAINT IF EXISTS truth_command_receipts_outcome_check;
ALTER TABLE truth_command_receipts
  ADD CONSTRAINT truth_command_receipts_outcome_check CHECK (
    outcome IN ('applied', 'absorbed_duplicate', 'rejected', 'conflict')
  );

CREATE TABLE IF NOT EXISTS truth_semantic_absorptions (
  command_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
  attempted_candidate_id UUID NOT NULL,
  attempted_model_id UUID NOT NULL,
  attempted_version_id UUID NOT NULL,
  absorbed_into_version_id UUID NOT NULL,
  attempted_command JSONB NOT NULL CHECK (
    jsonb_typeof(attempted_command) = 'object'
  ),
  recorded_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, command_id),
  FOREIGN KEY (tenant_id, command_id)
    REFERENCES truth_command_receipts (tenant_id, command_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, absorbed_into_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT
);

ALTER TABLE truth_semantic_absorptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE truth_semantic_absorptions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON truth_semantic_absorptions;
CREATE POLICY tenant_isolation ON truth_semantic_absorptions
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

DROP TRIGGER IF EXISTS truth_semantic_absorptions_immutable
  ON truth_semantic_absorptions;
CREATE TRIGGER truth_semantic_absorptions_immutable
  BEFORE UPDATE OR DELETE ON truth_semantic_absorptions
  FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation();

-- 0225 created this unnamed table-level check with PostgreSQL's deterministic
-- `truth_command_receipts_check` name. Absorption is a successful resolution,
-- so it must not fabricate a rejection code merely to satisfy the old shape.
ALTER TABLE truth_command_receipts
  DROP CONSTRAINT IF EXISTS truth_command_receipts_check;
ALTER TABLE truth_command_receipts
  DROP CONSTRAINT IF EXISTS truth_command_receipts_rejection_code_check;
ALTER TABLE truth_command_receipts
  ADD CONSTRAINT truth_command_receipts_rejection_code_check CHECK (
    outcome IN ('applied', 'absorbed_duplicate') OR rejection_code IS NOT NULL
  );

COMMENT ON COLUMN truth_command_receipts.outcome IS
  'absorbed_duplicate means a distinct command resolved to an already-active exact semantic version';
COMMENT ON TABLE truth_semantic_absorptions IS
  'Immutable reconstructable audit of discarded exact-duplicate admission commands';

COMMIT;
