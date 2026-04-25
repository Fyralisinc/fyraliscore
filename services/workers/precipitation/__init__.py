"""
services/workers/precipitation — Wave 4-C precipitation worker.

Nightly job that turns a pile of related `hypothesis` / `concern`
Models into one `pattern_candidates` row per dense embedding cluster.
The candidate row is then promoted by Think T4 (trigger_subkind =
'pattern_review') into a first-class Pattern Model, with its
constituent Models gaining a `supporting_model_ids` pointer back to
the Pattern.

Pipeline
--------
1. `clustering.cluster_active_models(conn, *, tenant_id)` — pulls
   active hypothesis + concern Models with embeddings, runs HDBSCAN,
   returns `ClusterResult` per dense cluster (size ≥ 3, density ≥ 0.5).
2. `clustering.write_candidates(conn, clusters)` — inserts one
   `pattern_candidates` row per cluster (idempotent via a check on
   constituent_model_ids overlap).
3. `proposer.enqueue_pattern_review_triggers(conn, candidate_ids)` —
   enqueues a T4 `pattern_review` trigger for each fresh candidate.
4. `proposer.promote_pattern_candidate(...)` — called by Think T4's
   deterministic `pattern_review` branch to insert a Pattern Model +
   link constituents.

Entry point: `services.workers.precipitation.worker.run_once`.
"""
from services.workers.precipitation.worker import run_once, PrecipitationResult

__all__ = ["run_once", "PrecipitationResult"]
