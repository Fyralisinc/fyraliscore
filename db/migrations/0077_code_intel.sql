-- 0077_code_intel.sql
--   GitHub Intelligence Layer — code-comprehension model (services/code_intel).
--
-- A living, commit-sha-versioned code graph per repo: files -> symbols ->
-- edges (contains/imports/references) + per-symbol embeddings (code-RAG).
-- Powers "blast radius" (given changed files/symbols, who depends on them?)
-- and semantic retrieval of code relevant to a GitHub signal.
--
-- NOT AN INGESTION SOURCE. This subsystem emits NO observations and touches
-- NONE of the four source-registry CHECK tables (source_onboarding_runs,
-- onboarding_shards, ingestion_failures, onboarding_triggers). It therefore
-- sidesteps the 0070/0072/0073 source-CHECK widening landmine entirely — a
-- future reader should not expect a CHECK widening here.
--
-- §II compliance:
--   - Append-only: all CREATE TABLE / INDEX are IF NOT EXISTS (additive).
--   - Idempotent: tables + indexes guarded by IF NOT EXISTS; RLS policies
--     dropped IF EXISTS and re-created. Re-running is a no-op.
--   - Tenant isolation (§III): every table ENABLE + FORCE RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     google_drive_* / jira_* template (0061 / 0062).
--   - Versioning: dedup keyed on commit_sha so re-index of the same commit
--     is a no-op upsert; code_files.blob_sha + code_symbols.symbol_hash let
--     the incremental worker copy unchanged rows forward without re-parsing.

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------
-- code_snapshots — one row per (tenant, repo, commit). Versioning anchor.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_snapshots (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  provider_installation_id UUID,         -- optional FK to provider_installations
  repo_full_name TEXT NOT NULL,          -- "owner/repo"
  branch TEXT NOT NULL DEFAULT 'main',
  commit_sha TEXT NOT NULL,
  parent_snapshot_id UUID REFERENCES code_snapshots(id),  -- prior indexed commit
  status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','indexing','ready','failed','skipped_too_large')),
  index_kind TEXT NOT NULL DEFAULT 'full'
        CHECK (index_kind IN ('full','incremental')),
  indexer TEXT,                          -- 'python_ast' | 'tree_sitter' | 'scip:<lang>'
  file_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  edge_count INTEGER NOT NULL DEFAULT 0,
  parse_error_files INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  last_error TEXT,
  UNIQUE (tenant_id, repo_full_name, commit_sha)
);

CREATE INDEX IF NOT EXISTS code_snapshots_repo_idx
  ON code_snapshots (tenant_id, repo_full_name, created_at DESC);
CREATE INDEX IF NOT EXISTS code_snapshots_ready_idx
  ON code_snapshots (tenant_id, repo_full_name, status, created_at DESC);

-- ---------------------------------------------------------------------
-- code_files — one row per file in a snapshot. blob_sha enables sharing.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_files (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  snapshot_id UUID NOT NULL REFERENCES code_snapshots(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  language TEXT,                          -- NULL = unknown / binary / generated
  blob_sha TEXT NOT NULL,                -- content hash; dedup + change-detect
  size_bytes INTEGER NOT NULL DEFAULT 0,
  line_count INTEGER NOT NULL DEFAULT 0,
  is_generated BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (snapshot_id, path)
);

CREATE INDEX IF NOT EXISTS code_files_snapshot_idx ON code_files (snapshot_id);
CREATE INDEX IF NOT EXISTS code_files_blob_idx ON code_files (tenant_id, blob_sha);
CREATE INDEX IF NOT EXISTS code_files_path_idx ON code_files (snapshot_id, path);

-- ---------------------------------------------------------------------
-- code_symbols — functions / classes / methods / etc. within a file.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_symbols (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  snapshot_id UUID NOT NULL REFERENCES code_snapshots(id) ON DELETE CASCADE,
  file_id UUID NOT NULL REFERENCES code_files(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,                    -- function|class|method|interface|type|const|module
  name TEXT NOT NULL,
  qualified_name TEXT NOT NULL,          -- "module.Class.method"
  parent_symbol_id UUID REFERENCES code_symbols(id),
  start_line INTEGER NOT NULL DEFAULT 0,
  end_line INTEGER NOT NULL DEFAULT 0,
  signature TEXT,
  docstring TEXT,
  symbol_hash TEXT NOT NULL,             -- hash(kind,qname,signature,span) for change-detect
  UNIQUE (snapshot_id, qualified_name, start_line)
);

CREATE INDEX IF NOT EXISTS code_symbols_snapshot_idx ON code_symbols (snapshot_id);
CREATE INDEX IF NOT EXISTS code_symbols_file_idx ON code_symbols (file_id);
CREATE INDEX IF NOT EXISTS code_symbols_qname_idx ON code_symbols (tenant_id, qualified_name);

-- ---------------------------------------------------------------------
-- code_edges — contains / imports / references. Reverse index = blast radius.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_edges (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  snapshot_id UUID NOT NULL REFERENCES code_snapshots(id) ON DELETE CASCADE,
  edge_kind TEXT NOT NULL CHECK (edge_kind IN ('contains','imports','references')),
  src_symbol_id UUID REFERENCES code_symbols(id) ON DELETE CASCADE,
  src_file_id   UUID REFERENCES code_files(id)   ON DELETE CASCADE,
  dst_symbol_id UUID REFERENCES code_symbols(id) ON DELETE CASCADE,
  dst_file_id   UUID REFERENCES code_files(id)   ON DELETE CASCADE,
  dst_unresolved TEXT,                   -- bare/external module spec when no dst row
  precision TEXT NOT NULL DEFAULT 'heuristic'
        CHECK (precision IN ('exact','heuristic'))   -- SCIP upgrades to 'exact'
);

-- Reverse traversal (who depends on X): given dst_symbol_id/dst_file_id,
-- find src_*. This is THE blast-radius access path.
CREATE INDEX IF NOT EXISTS code_edges_dst_symbol_idx
  ON code_edges (snapshot_id, dst_symbol_id) WHERE dst_symbol_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS code_edges_dst_file_idx
  ON code_edges (snapshot_id, dst_file_id) WHERE dst_file_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS code_edges_src_symbol_idx
  ON code_edges (snapshot_id, src_symbol_id) WHERE src_symbol_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS code_edges_unresolved_idx
  ON code_edges (snapshot_id, dst_unresolved) WHERE dst_unresolved IS NOT NULL;

-- ---------------------------------------------------------------------
-- code_embeddings — per-symbol code-RAG vectors (768-d, nomic-embed-text).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_embeddings (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  snapshot_id UUID NOT NULL REFERENCES code_snapshots(id) ON DELETE CASCADE,
  symbol_id UUID REFERENCES code_symbols(id) ON DELETE CASCADE,
  file_id UUID NOT NULL REFERENCES code_files(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL DEFAULT 0,
  content_text TEXT NOT NULL,            -- the chunk text that was embedded
  embedding vector(768),
  embedding_pending BOOLEAN NOT NULL DEFAULT TRUE,
  model_name TEXT,
  UNIQUE (snapshot_id, symbol_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS code_embeddings_snapshot_idx ON code_embeddings (snapshot_id);
CREATE INDEX IF NOT EXISTS code_embeddings_pending_idx
  ON code_embeddings (tenant_id, embedding_pending) WHERE embedding_pending = TRUE;
CREATE INDEX IF NOT EXISTS code_embeddings_hnsw
  ON code_embeddings USING hnsw (embedding vector_cosine_ops);

-- ---------------------------------------------------------------------
-- code_intel_index_triggers — outbox the ingestion handler writes on a
-- default-branch push / merged PR; the incremental indexer drains it.
-- Debounce/coalesce: UNIQUE(tenant, repo, commit_sha) so re-delivery of
-- the same sha is a no-op.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS code_intel_index_triggers (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  repo_full_name TEXT NOT NULL,
  branch TEXT,
  commit_sha TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'incremental'
        CHECK (kind IN ('full','incremental','push','merge')),
  status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','claimed','done','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  claimed_by TEXT,
  completed_at TIMESTAMPTZ,
  last_error TEXT,
  UNIQUE (tenant_id, repo_full_name, commit_sha)
);

CREATE INDEX IF NOT EXISTS code_intel_index_triggers_unclaimed_idx
  ON code_intel_index_triggers (tenant_id, created_at)
  WHERE status = 'pending';

-- ---------------------------------------------------------------------
-- RLS — tenant_isolation on every table (0061/0062 template).
-- ---------------------------------------------------------------------
ALTER TABLE code_snapshots            ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_snapshots            FORCE  ROW LEVEL SECURITY;
ALTER TABLE code_files                ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_files                FORCE  ROW LEVEL SECURITY;
ALTER TABLE code_symbols              ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_symbols              FORCE  ROW LEVEL SECURITY;
ALTER TABLE code_edges                ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_edges                FORCE  ROW LEVEL SECURITY;
ALTER TABLE code_embeddings           ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_embeddings           FORCE  ROW LEVEL SECURITY;
ALTER TABLE code_intel_index_triggers ENABLE ROW LEVEL SECURITY;
ALTER TABLE code_intel_index_triggers FORCE  ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'code_snapshots',
    'code_files',
    'code_symbols',
    'code_edges',
    'code_embeddings',
    'code_intel_index_triggers'
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
