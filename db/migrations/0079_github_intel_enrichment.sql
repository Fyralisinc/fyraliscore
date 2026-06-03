-- 0079_github_intel_enrichment.sql
--   GitHub Intelligence Layer — signal enrichment record + ordered work queue.
--
-- github_signal_enrichment is the structured system-of-record: one row per
-- processed github:webhook observation (joined by observation_id). It mirrors
-- what the inline path writes into observations.content["intelligence"], but is
-- queryable for "current state", audit, and blast-radius lookups. The inline
-- per-signal view and this system-of-record reconcile via observation_id.
--
-- github_intel_queue is the claimable work queue the ordered state-advancement
-- worker drains (FOR UPDATE SKIP LOCKED + per-repo advisory lock + occurred_at
-- ordering), mirroring the 0065 workflow_signals pattern.
--
-- observation_id is an FK-by-convention UUID (NOT a real FK) because the
-- observations PK is the composite (id, occurred_at) of a partitioned table.
--
-- NOT AN INGESTION SOURCE. See 0077 header. The code-reindex signal reuses the
-- code_intel_index_triggers outbox created in 0077.
--
-- §II compliance: append-only, idempotent, tenant-isolation RLS (0061 template).

BEGIN;

-- ---------------------------------------------------------------------
-- github_signal_enrichment — structured per-signal causal context.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_signal_enrichment (
  id                UUID PRIMARY KEY,
  tenant_id         UUID NOT NULL REFERENCES tenants(id),
  observation_id    UUID NOT NULL,             -- FK-by-convention (partitioned obs)
  repo              TEXT,
  event_type        TEXT NOT NULL,
  action            TEXT,
  entity_kind       TEXT,                       -- pr|issue|branch|repo|check
  entity_ref        TEXT,                       -- "org/name#123" or branch name
  -- State transition caused (before -> after). NULL when no state change.
  state_before      JSONB,
  state_after       JSONB,
  state_changed     BOOLEAN NOT NULL DEFAULT FALSE,
  -- Affected code from the code-comprehension subsystem.
  affected_files    JSONB NOT NULL DEFAULT '[]'::jsonb,
  affected_symbols  JSONB NOT NULL DEFAULT '[]'::jsonb,
  blast_radius      JSONB NOT NULL DEFAULT '{}'::jsonb,
  code_snapshot_sha TEXT,
  related_entities  JSONB NOT NULL DEFAULT '[]'::jsonb,
  -- Causal reasoning.
  cause             TEXT,
  effect            TEXT,
  explanation       TEXT,
  confidence        REAL,
  reasoning_path    TEXT NOT NULL DEFAULT 'rule'   -- 'rule' | 'llm'
        CHECK (reasoning_path IN ('rule','llm','none')),
  enriched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  elapsed_ms        INTEGER,
  UNIQUE (observation_id)                        -- one enrichment per signal
);

CREATE INDEX IF NOT EXISTS github_signal_enrichment_entity_idx
  ON github_signal_enrichment (tenant_id, repo, entity_ref, enriched_at DESC);
CREATE INDEX IF NOT EXISTS github_signal_enrichment_repo_idx
  ON github_signal_enrichment (tenant_id, repo, enriched_at DESC);

-- ---------------------------------------------------------------------
-- github_intel_queue — claimable work queue for the ordered FSM worker.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github_intel_queue (
  id             UUID PRIMARY KEY,
  tenant_id      UUID NOT NULL REFERENCES tenants(id),
  observation_id UUID NOT NULL,
  repo           TEXT,
  occurred_at    TIMESTAMPTZ NOT NULL,          -- ORDER BY this for FSM safety
  enqueued_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at     TIMESTAMPTZ,
  claimed_by     TEXT,
  attempts       INTEGER NOT NULL DEFAULT 0,
  completed_at   TIMESTAMPTZ,
  last_error     TEXT,
  UNIQUE (observation_id)                        -- dedup / replay-safe
);

CREATE INDEX IF NOT EXISTS github_intel_queue_unclaimed_idx
  ON github_intel_queue (tenant_id, repo, occurred_at)
  WHERE completed_at IS NULL;

-- ---------------------------------------------------------------------
-- RLS — tenant_isolation (0061 template).
-- ---------------------------------------------------------------------
ALTER TABLE github_signal_enrichment ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_signal_enrichment FORCE  ROW LEVEL SECURITY;
ALTER TABLE github_intel_queue       ENABLE ROW LEVEL SECURITY;
ALTER TABLE github_intel_queue       FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'github_signal_enrichment',
    'github_intel_queue'
  ]
  LOOP
    EXECUTE format(
      'DROP POLICY IF EXISTS %I_tenant_isolation ON %I',
      t, t
    );
    EXECUTE format(
      'CREATE POLICY %I_tenant_isolation ON %I '
      'USING (tenant_id = current_setting(''app.current_tenant'', true)::uuid) '
      'WITH CHECK (tenant_id = current_setting(''app.current_tenant'', true)::uuid)',
      t, t
    );
  END LOOP;
END $$;

COMMIT;
