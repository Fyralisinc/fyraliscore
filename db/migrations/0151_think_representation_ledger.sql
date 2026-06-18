-- 0151_think_representation_ledger.sql
--
-- Durable representation-budget and coverage audits for Think runs.
-- This is the scoreboard for large runs: not just "did the diff apply?",
-- but "did this evidence window actually improve the company twin?"

BEGIN;

CREATE TABLE IF NOT EXISTS think_representation_ledger (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  run_id UUID REFERENCES think_runs(id) ON DELETE SET NULL,
  trigger_id UUID NOT NULL,
  trigger_kind TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  observation_count INTEGER NOT NULL DEFAULT 0 CHECK (observation_count >= 0),
  model_context_count INTEGER NOT NULL DEFAULT 0 CHECK (model_context_count >= 0),
  claim_insert_count INTEGER NOT NULL DEFAULT 0 CHECK (claim_insert_count >= 0),
  model_update_count INTEGER NOT NULL DEFAULT 0 CHECK (model_update_count >= 0),
  evidence_attachment_count INTEGER NOT NULL DEFAULT 0 CHECK (evidence_attachment_count >= 0),
  near_duplicate_absorption_count INTEGER NOT NULL DEFAULT 0 CHECK (near_duplicate_absorption_count >= 0),
  relation_claim_count INTEGER NOT NULL DEFAULT 0 CHECK (relation_claim_count >= 0),
  relation_frame_count INTEGER NOT NULL DEFAULT 0 CHECK (relation_frame_count >= 0),
  edge_op_count INTEGER NOT NULL DEFAULT 0 CHECK (edge_op_count >= 0),
  source_digest_count INTEGER NOT NULL DEFAULT 0 CHECK (source_digest_count >= 0),
  model_adaptiveness INTEGER NOT NULL DEFAULT 0 CHECK (model_adaptiveness >= 0),
  edge_adaptiveness INTEGER NOT NULL DEFAULT 0 CHECK (edge_adaptiveness >= 0),
  source_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
  coverage_roles JSONB NOT NULL DEFAULT '[]'::jsonb,
  retrieval_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
  budget_status TEXT NOT NULL DEFAULT 'ok' CHECK (
    budget_status IN ('ok', 'warning', 'failed')
  ),
  warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS think_representation_ledger_tenant_time_idx
  ON think_representation_ledger (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS think_representation_ledger_trigger_idx
  ON think_representation_ledger (trigger_id);

CREATE INDEX IF NOT EXISTS think_representation_ledger_run_idx
  ON think_representation_ledger (run_id);

CREATE INDEX IF NOT EXISTS think_representation_ledger_status_idx
  ON think_representation_ledger (tenant_id, budget_status, created_at DESC);

CREATE INDEX IF NOT EXISTS think_representation_ledger_roles_gin
  ON think_representation_ledger USING gin (coverage_roles);

CREATE INDEX IF NOT EXISTS think_representation_ledger_tags_gin
  ON think_representation_ledger USING gin (retrieval_tags);

CREATE INDEX IF NOT EXISTS think_representation_ledger_source_coverage_gin
  ON think_representation_ledger USING gin (source_coverage);

ALTER TABLE think_representation_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE think_representation_ledger FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON think_representation_ledger;
CREATE POLICY tenant_isolation ON think_representation_ledger
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'company_os') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON TABLE think_representation_ledger TO company_os;
  END IF;
END
$$;

COMMIT;
