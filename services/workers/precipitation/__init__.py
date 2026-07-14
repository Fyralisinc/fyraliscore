"""
services/workers/precipitation — Wave 4-C precipitation worker.

Nightly job that turns a pile of related `hypothesis` / `concern`
Models into one `pattern_candidates` row per dense embedding cluster.
The candidate row is weak evidence for a later semantic Think review.
It must not be promoted deterministically just because the embedding
cluster is dense.

Pipeline
--------
1. `clustering.cluster_active_models(conn, *, tenant_id)` — pulls
   active hypothesis + concern Models with embeddings, runs HDBSCAN,
   returns `ClusterResult` per dense cluster (size ≥ 3, density ≥ 0.5).
2. `proposer.write_candidates(conn, clusters)` — inserts one
   `pattern_candidates` row per cluster (idempotent via a check on
   constituent_model_ids overlap).
3. `proposer.enqueue_pattern_review_triggers(conn, candidate_ids)` —
   enqueues a T4 `pattern_review` trigger for each fresh candidate.
4. Inferential Think review may later call
   `proposer.promote_pattern_candidate(...)` only after semantic evidence
   justifies an explicit Pattern Model.

Entry point: `services.workers.precipitation.worker.run_once`.
"""
from services.workers.precipitation.quality_gate import (
    PrecipitationQualityObservation,
    PrecipitationQualityReport,
    assess_precipitation_quality,
    observation_from_review_payload,
)
from services.workers.precipitation.worker import PrecipitationResult, run_once

__all__ = [
    "PrecipitationQualityObservation",
    "PrecipitationQualityReport",
    "PrecipitationResult",
    "assess_precipitation_quality",
    "observation_from_review_payload",
    "run_once",
]
