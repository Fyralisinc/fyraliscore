-- 0031_model_edges.sql
--
-- S1 of the Self-Organizing Substrate plan — unified Model-to-Model
-- edge primitive. Replaces the seven ad-hoc connection mechanisms
-- (supporting_model_ids array, contributing_models array, pattern
-- back-link, archive_reason='superseded' lifecycle flag, latent
-- proposition-encoded edges) with a single typed-edge table whose
-- semantics are declared in `lib/shared/edge_registry.py`.
--
-- (Migration number 0030 was already taken by the audit-chain
-- migration that landed in parallel; this S1 work claims 0031.)
--
-- This migration ALSO drops the CHECK on model_reeval_queue.cause_kind
-- introduced in 0007. The cause_kind taxonomy is now declarative —
-- every edge_kind in the registry contributes its own cause_kind via
-- on_source_archive / on_target_archive callbacks. Hard-coding the
-- five-value enum at the DB layer would block every new edge_kind
-- from cascading. Validation lives in the registry.
--
-- Dual-write phase: arrays remain authoritative during S1. The applier
-- and pattern proposer write to BOTH model_edges AND the legacy
-- arrays. A drift detector worker (services/workers/edge_drift) runs
-- continuously and alarms on divergence. After 14 consecutive days
-- of zero drift, S2 cuts consumers over and S3 drops the array
-- columns. Until then the arrays remain the source of truth.
--
-- Symmetric edge_kinds (none in v1; reserved for future `contradicts`)
-- are stored as TWO rows kept in sync by the EdgesRepo helper, not as
-- a single canonicalized row + CHECK. This eliminates source/target
-- special-casing in every consumer at the cost of negligible storage.
--
-- See V1_PR_PROMPTS.md ("Unified Model-to-Model Edge Primitive") and
-- /Users/rachinkalakheti/.claude/plans/this-makes-me-think-imperative-gizmo.md
-- for the full design.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).

BEGIN;

-- ---------------------------------------------------------------------
-- Drop the cause_kind CHECK on model_reeval_queue (from migration 0007).
-- New edge_kinds in the registry produce new cause_kinds (e.g.
-- 'contributor_archived', 'pattern_archived', 'instance_archived'); a
-- hard-coded CHECK is incompatible with a declarative registry.
-- Validation now lives in services/think/deterministic.py + registry.
-- ---------------------------------------------------------------------
ALTER TABLE model_reeval_queue
  DROP CONSTRAINT IF EXISTS model_reeval_queue_cause_kind_check;

-- ---------------------------------------------------------------------
-- model_edges — the unified primitive
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_edges (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  -- App-enforced FKs to models(id). We do NOT use ON DELETE CASCADE
  -- because real Models are archived (status='archived'), never
  -- deleted; and edges to archived Models are kept (status='inert')
  -- for audit. If a Model row is ever physically deleted (test
  -- cleanup, manual ops), edges to it become orphans that the drift
  -- detector flags.
  source_model_id UUID NOT NULL,
  target_model_id UUID NOT NULL,
  -- edge_kind is registry-validated at the application layer. See
  -- lib/shared/edge_registry.py for the legal set + per-kind
  -- semantics (DAG scope, weight rules, cascade callbacks,
  -- mutually-exclusive-with). NOT enforced here as a CHECK because
  -- the registry is the single source of truth and adding a kind
  -- should not require a migration.
  edge_kind TEXT NOT NULL,
  -- Optional [0, 1]; some edge_kinds require it (future
  -- `contradicts`), some forbid it (`superseded_by`), some allow it
  -- (`supports`). Registry decides; repo enforces.
  weight FLOAT,
  -- Edge-kind-specific payload. e.g., for `contributes_to_resolution`
  -- a copy of the resolution criteria fragment; for `instance_of`
  -- the matched_context from PatternInstanceProposition.
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  -- Lifecycle. Edges go inert when either endpoint is archived
  -- (set in the same transaction as the Model archive). 'disputed'
  -- is reserved for future contradiction-resolution flows.
  status TEXT NOT NULL DEFAULT 'active',
  -- Provenance: who wrote this edge. The legal set is declared in
  -- lib/shared/types.py (EdgeDetectedBy). Validation app-side.
  detected_by TEXT NOT NULL,
  -- When the edge was created.
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- The triggering observation (or NULL if the edge is purely
  -- maintenance / backfill / cascade).
  created_by_event_id UUID,
  -- Lifecycle audit.
  status_changed_at TIMESTAMPTZ,
  status_reason TEXT,
  CHECK (source_model_id != target_model_id),
  -- Per-kind UNIQUE: at most one edge of each kind between an ordered
  -- pair. Symmetric edge_kinds keep two rows (one per direction); the
  -- repo helper enforces the mirror invariant.
  CONSTRAINT model_edges_unique UNIQUE
    (tenant_id, source_model_id, target_model_id, edge_kind)
);

-- Forward traversal: "what does M relate to via kind K?"
-- Partial on status='active' so retrieval expansion never has to
-- filter inert/disputed rows.
CREATE INDEX IF NOT EXISTS model_edges_source_idx
  ON model_edges (tenant_id, source_model_id, edge_kind)
  WHERE status = 'active';

-- Backward traversal: "what relates to M via kind K?" — load-bearing
-- for the archive cascade, future debug UI "what depends on this?"
-- panel, and any pathway that needs reverse expansion. This is the
-- capability the pre-S1 substrate could not answer in O(log n).
CREATE INDEX IF NOT EXISTS model_edges_target_idx
  ON model_edges (tenant_id, target_model_id, edge_kind)
  WHERE status = 'active';

-- Per-kind scan: "all edges of kind K in this tenant" — used by the
-- drift detector and any kind-wide audit. Includes all statuses so
-- the auditor can sweep inert + disputed too.
CREATE INDEX IF NOT EXISTS model_edges_kind_idx
  ON model_edges (tenant_id, edge_kind, status);

COMMIT;
