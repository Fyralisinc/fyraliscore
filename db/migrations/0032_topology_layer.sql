-- 0032_topology_layer.sql
--
-- S2 of the Self-Organizing Substrate plan — positional embeddings
-- + propagation queue + materialized neighborhoods. Sits on top of
-- the unified model_edges primitive (S1, migration 0031) and adds
-- the foundation for arrangement-as-meaning. NO retrieval changes
-- yet: topology is observable in this stage but not consequential.
-- Pathway F (topological retrieval), prompt-level neighborhood
-- context, the relocate claim_op, and topology-driven cascade are
-- all S3/S4.
--
-- What this migration adds
-- ------------------------
--
--   1. models.topo_embedding VECTOR(128) — the LEARNED positional
--      vector for each Model. Distinct from the content embedding
--      (768d, semantic). Initialized from content_anchor() at insert,
--      then updated continuously by services.workers.topology_updater
--      via the alpha-anchored neighbor-mean rule:
--
--        topo(M, t+1) = (1 - α) · weighted_mean( topo(N) for N ∈ neighbors(M) )
--                     + α · content_anchor(M)
--
--      α is the gravitational pull of content over arrangement;
--      α = 0.3 by default. Without it, isolated regions of the
--      graph would drift arbitrarily.
--
--      Stored NULL until first computed (ModelsRepo.insert sets it
--      synchronously; topology_updater refines it asynchronously).
--      The HNSW index is partial on `status='active' AND topo IS
--      NOT NULL` so Pathway F (S3) can do nearest-neighbor lookups
--      in O(log n) once it ships.
--
--   2. models.topo_updated_at — wall-clock of the last topo
--      recompute. Read by tests and by S3's prompt extension to
--      flag stale positional information to the LLM.
--
--   3. topo_dirty_queue — the propagation queue. When a Model's
--      topo changes by ‖Δ‖ > ε, neighbors are enqueued. The worker
--      dequeues with priority = delta_magnitude × γ^hop_depth
--      (damping factor γ ∈ (0,1)) and stops propagating once the
--      damped magnitude falls below ε_terminate. Same dedup pattern
--      as model_reeval_queue: NULLS NOT DISTINCT on
--      (tenant, model, processed_at) so unprocessed duplicates
--      collapse but processed rows can be re-enqueued.
--
--   4. model_neighborhoods — materialized communities. Detected
--      offline by services.workers.neighborhood_detector (hourly
--      default). Stable IDs across re-clusterings via greedy
--      centroid-distance + member-overlap matching. v1 uses
--      connected-components clustering on the active edge graph;
--      Louvain / label-propagation can swap in later without
--      schema changes.
--
--   5. model_neighborhood_membership — reverse lookup
--      (Model → neighborhood). Carries `centrality` (0-1, intra-
--      cluster eigenvector centrality approx) for use by S3's
--      neighborhood-summary in the LLM prompt.
--
-- Why these tables, not extensions of model_edges
-- -----------------------------------------------
-- Neighborhoods are emergent properties of the edge graph, not
-- relationships between Models. Conflating them with edges would
-- force every cascade and traversal to filter "is this an edge or a
-- cluster membership?" — exactly the fragmentation S1 cleaned up.
-- Keeping them in their own tables preserves the model_edges layer
-- as the relational substrate while the neighborhood layer
-- summarizes it.
--
-- See lib/topology/, services/topology/, and
-- services/workers/{topology_updater,neighborhood_detector}/ for
-- the consuming code.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).

BEGIN;

-- ---------------------------------------------------------------------
-- 1 + 2. Add topo_embedding + topo_updated_at to models.
-- ---------------------------------------------------------------------

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS topo_embedding VECTOR(128),
  ADD COLUMN IF NOT EXISTS topo_updated_at TIMESTAMPTZ;

-- HNSW partial index for S3's Pathway F. Building it now so the
-- index gets time to populate as the updater runs through Stage 2's
-- 4-week soak period. Partial on `topo_embedding IS NOT NULL` so it
-- excludes Models still awaiting first compute.
CREATE INDEX IF NOT EXISTS models_topo_embedding_idx
  ON models USING hnsw (topo_embedding vector_cosine_ops)
  WHERE status = 'active' AND topo_embedding IS NOT NULL;

-- ---------------------------------------------------------------------
-- 3. topo_dirty_queue — propagation queue
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS topo_dirty_queue (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL,
  -- Provenance: which Model's recompute caused this enqueue. Useful
  -- for debugging propagation cascades and (future) cycle prevention.
  cause_model_id UUID,
  -- How many hops the propagation wave has travelled from the
  -- originating change. 0 = direct effect (e.g. a new edge); larger
  -- depths come from neighbor-of-neighbor propagation. Used together
  -- with delta_magnitude to compute priority.
  hop_depth INTEGER NOT NULL DEFAULT 0,
  -- The expected magnitude of change at this depth. The worker
  -- recomputes the actual delta and decides whether to propagate.
  -- Used for queue ordering: high-magnitude updates jump ahead so a
  -- big structural shift doesn't sit behind a backlog of decay
  -- ticks.
  delta_magnitude FLOAT,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  CONSTRAINT topo_dirty_queue_dedup UNIQUE NULLS NOT DISTINCT
    (tenant_id, model_id, processed_at)
);

-- Worker poll: pending rows, highest delta_magnitude first, then
-- FIFO. Partial on processed_at IS NULL so the index stays small.
CREATE INDEX IF NOT EXISTS topo_dirty_queue_pending_idx
  ON topo_dirty_queue (tenant_id, delta_magnitude DESC NULLS LAST, enqueued_at)
  WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS topo_dirty_queue_model_idx
  ON topo_dirty_queue (model_id);

-- ---------------------------------------------------------------------
-- 4. model_neighborhoods — materialized communities
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS model_neighborhoods (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  -- Centroid in topo-embedding space. Used by matching to assign
  -- stable IDs across re-clusterings (greedy: each new cluster
  -- inherits the closest active neighborhood's id, falling back to
  -- a new id if no match within threshold).
  centroid_topo_embedding VECTOR(128) NOT NULL,
  -- Members at last_recomputed_at. Membership table is the
  -- authoritative reverse index; this array is denormalized for
  -- fast neighborhood-level reads.
  member_model_ids UUID[] NOT NULL,
  -- When this neighborhood first appeared (as opposed to inherited
  -- from a prior matched neighborhood). Stable across re-clusters.
  emergence_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- If this neighborhood emerged from a fission event (one
  -- neighborhood split into multiple), list the predecessor id(s).
  -- NULL for cleanly-emergent neighborhoods.
  predecessor_neighborhood_ids UUID[],
  -- LLM-generated semantic label ("engineering velocity",
  -- "customer commitments"). Set in S3 by the prompt extension.
  -- Until then, neighborhoods are unnamed.
  named_signature TEXT,
  named_at TIMESTAMPTZ,
  -- Internal connection density (edges within / max possible),
  -- precomputed at recompute time so consumers don't have to
  -- recount. Range [0, 1].
  density FLOAT,
  status TEXT NOT NULL DEFAULT 'active',
    -- 'active' | 'dissolved' | 'merged'
  status_changed_at TIMESTAMPTZ,
  status_reason TEXT,
  -- When the membership / centroid / density was last recomputed.
  last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: "give me the active neighborhoods for this tenant,
-- newest recompute first."
CREATE INDEX IF NOT EXISTS model_neighborhoods_active_idx
  ON model_neighborhoods (tenant_id, last_recomputed_at DESC)
  WHERE status = 'active';

-- ---------------------------------------------------------------------
-- 5. model_neighborhood_membership — reverse lookup
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS model_neighborhood_membership (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL,
  neighborhood_id UUID NOT NULL,
  -- Eigenvector-centrality approximation in [0, 1]; how central is
  -- this Model in its neighborhood. v1 uses degree-centrality; the
  -- spec name and column type are stable for swap-out later.
  centrality FLOAT,
  joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, neighborhood_id)
);

CREATE INDEX IF NOT EXISTS model_neighborhood_membership_neighborhood_idx
  ON model_neighborhood_membership (neighborhood_id);

CREATE INDEX IF NOT EXISTS model_neighborhood_membership_tenant_model_idx
  ON model_neighborhood_membership (tenant_id, model_id);

COMMIT;
