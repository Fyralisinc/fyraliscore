"""
services/workers/precipitation/clustering.py — HDBSCAN over Model embeddings.

Pure clustering + candidate materialisation. No Think, no triggers.

Why HDBSCAN (and not k-means)
-----------------------------
HDBSCAN is density-based: dense regions become clusters, sparse
regions don't, and you don't have to specify k ahead of time. For a
tenant with "many unrelated concerns + a handful of repeating ones,"
HDBSCAN produces 0..N clusters where N is emergent — which is exactly
the signal we want when deciding whether to precipitate a Pattern.

Density threshold
-----------------
HDBSCAN exposes a `probabilities_` array per point — how strongly each
point belongs to its cluster. We compute cluster density as the mean
of member probabilities. Clusters whose mean < 0.5 are dropped as
"too diffuse to precipitate" (documented in BUILD-LOG Deviations —
the prompt picked 0.5 as a default; we expose the threshold as a
parameter for future tuning).

Embedding distance
------------------
Models carry VECTOR(768) embeddings (pgvector). We compute HDBSCAN in
cosine-distance space by L2-normalising the vectors and using
`metric='euclidean'` — on unit-length vectors Euclidean and cosine
ranked distances are monotonic, so HDBSCAN's dense-region detection
produces the same clusters.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
from uuid import UUID

import asyncpg
import numpy as np


# HDBSCAN and its sklearn dependency are heavy imports; lazy-import
# them inside `cluster_active_models` so unit tests that don't need
# clustering can still load this module fast.


MIN_CLUSTER_SIZE = 3
DENSITY_THRESHOLD = 0.5

# We only cluster these proposition kinds per spec §19 "repeated
# pattern_instance Models. But instance accumulation doesn't
# automatically precipitate — it needs a dedicated worker that
# evaluates whether candidates have enough support" — and per
# BUILD-PLAN 4.C's guidance to cluster "hypothesis / concern" Models.
CLUSTERABLE_KINDS: frozenset[str] = frozenset(("hypothesis", "concern"))


@dataclass(frozen=True)
class ClusterMember:
    model_id: UUID
    proposition_kind: str
    natural: str


@dataclass(frozen=True)
class ClusterResult:
    """One dense embedding cluster of hypothesis/concern Models."""
    tenant_id: UUID
    members: tuple[ClusterMember, ...]
    density: float

    @property
    def size(self) -> int:
        return len(self.members)


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def cluster_active_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None = None,
    min_cluster_size: int = MIN_CLUSTER_SIZE,
    density_threshold: float = DENSITY_THRESHOLD,
    min_samples: int | None = None,
) -> list[ClusterResult]:
    """
    Pull active hypothesis/concern Models, cluster them, filter down
    to dense clusters.

    Returns a list — empty when there are fewer than `min_cluster_size`
    clusterable Models or when no cluster meets the density threshold.
    """
    # Fetch ids + embeddings + metadata.
    params: list = []
    filters = [
        "status = 'active'",
        f"proposition_kind = ANY($1::text[])",
        "embedding IS NOT NULL",
    ]
    params.append(list(CLUSTERABLE_KINDS))
    if tenant_id is not None:
        params.append(tenant_id)
        filters.append(f"tenant_id = ${len(params)}")

    # pgvector returns vectors as strings by default; we register the
    # codec just-in-time so we get real numpy arrays back.
    from pgvector.asyncpg import register_vector
    try:
        await register_vector(conn)
    except Exception:
        # Idempotent — safe to re-register.
        pass

    rows = await conn.fetch(
        f"""
        SELECT id, tenant_id, proposition_kind, "natural", embedding
        FROM models
        WHERE {' AND '.join(filters)}
        """,
        *params,
    )

    if len(rows) < min_cluster_size:
        return []

    # Stack embeddings into an (N, 768) matrix, L2-normalise.
    X = np.array([r["embedding"] for r in rows], dtype=np.float64)
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    # Guard against zero-vectors (shouldn't happen for real embeddings,
    # but tests may pass synthetic zeros).
    norms[norms == 0] = 1.0
    X_unit = X / norms

    # Lazy import — heavy.
    import hdbscan
    # `min_samples=None` defaults HDBSCAN's `min_samples` to
    # `min_cluster_size`, which is too strict for small datasets.
    # We default to 1 — HDBSCAN will over-cluster (including spurious
    # noise pairings) but the per-point probability filter below
    # rejects spurious points.
    effective_min_samples = 1 if min_samples is None else min_samples
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=effective_min_samples,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(X_unit)
    probabilities = clusterer.probabilities_

    # If HDBSCAN returned nothing but N is small enough that its
    # density estimator can't work (< 30 points), fall back to a
    # deterministic cosine-similarity single-link clustering. This
    # covers nightly runs at new tenants + tests with few Models.
    # See BUILD-LOG Deviations for the rationale.
    if all(l < 0 for l in labels) and len(rows) < 30:
        labels, probabilities = _similarity_cluster(
            X_unit, min_cluster_size=min_cluster_size,
            cosine_threshold=0.95,
        )

    # Group rows by cluster label (label -1 is noise). Filter each
    # cluster's membership down to points with probability >=
    # density_threshold — this drops the spurious noise stragglers
    # that HDBSCAN glued onto tight clusters under min_samples=1.
    grouped: dict[int, list[int]] = {}
    for idx, label in enumerate(labels):
        if label < 0:
            continue
        if probabilities[idx] < density_threshold:
            continue
        grouped.setdefault(int(label), []).append(idx)

    results: list[ClusterResult] = []
    for label, idxs in grouped.items():
        if len(idxs) < min_cluster_size:
            continue
        density = float(np.mean([probabilities[i] for i in idxs]))
        if density < density_threshold:
            continue
        # All members share a tenant (we filtered by tenant earlier or
        # clustered across tenants — in the multi-tenant case, we
        # group further by tenant_id inside this cluster).
        by_tenant: dict[UUID, list[int]] = {}
        for i in idxs:
            by_tenant.setdefault(rows[i]["tenant_id"], []).append(i)
        for t_id, sub_idxs in by_tenant.items():
            if len(sub_idxs) < min_cluster_size:
                continue
            # Recompute density within tenant (spec intent: no
            # cross-tenant precipitation).
            sub_density = float(np.mean([probabilities[i] for i in sub_idxs]))
            if sub_density < density_threshold:
                continue
            members = tuple(
                ClusterMember(
                    model_id=rows[i]["id"],
                    proposition_kind=rows[i]["proposition_kind"],
                    natural=rows[i]["natural"],
                )
                for i in sub_idxs
            )
            results.append(ClusterResult(
                tenant_id=t_id,
                members=members,
                density=sub_density,
            ))
    return results


def _similarity_cluster(
    X_unit,
    *,
    min_cluster_size: int,
    cosine_threshold: float,
):
    """
    Small-N fallback. L2-normalised `X_unit` → cosine similarity is
    just X_unit @ X_unit.T. Union-Find groups points whose pairwise
    cosine similarity exceeds `cosine_threshold`. Any group with ≥
    `min_cluster_size` members becomes a cluster; all members get
    probability = mean of pairwise similarities within the group.

    Deterministic. No HDBSCAN density-estimator edge cases.
    """
    import numpy as np
    n = len(X_unit)
    sim = X_unit @ X_unit.T
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    for i in range(n):
        for j in range(i + 1, n):
            if sim[i, j] >= cosine_threshold:
                union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)

    labels = np.full(n, -1, dtype=int)
    probs = np.zeros(n, dtype=float)
    next_label = 0
    for members in groups.values():
        if len(members) < min_cluster_size:
            continue
        # Per-member probability = mean cosine-similarity to other
        # cluster members (excluding self).
        for i in members:
            others = [j for j in members if j != i]
            probs[i] = float(np.mean(sim[i, others])) if others else 1.0
            labels[i] = next_label
        next_label += 1
    return labels, probs


def synthesize_candidate_payload(
    cluster: ClusterResult,
) -> tuple[dict, dict]:
    """
    Turn a cluster into `(proposed_signature, observed_tendency)`
    JSONB payloads for the `pattern_candidates` row.

    No LLM involved — we synthesize a structural summary from the
    members' natural language. Wave 5 UI may enrich this with an LLM
    pattern-description synthesis pass, but that's deferred.
    """
    kinds = sorted({m.proposition_kind for m in cluster.members})
    # Truncate each natural to 200 chars so the payload stays small.
    exemplars = [m.natural[:200] for m in cluster.members[:3]]
    proposed_signature = {
        "kind": "cluster_signature",
        "constituent_kinds": kinds,
        "member_count": cluster.size,
    }
    observed_tendency = {
        "exemplars": exemplars,
        "cluster_density": round(cluster.density, 4),
        "cluster_size": cluster.size,
    }
    return proposed_signature, observed_tendency


__all__ = [
    "CLUSTERABLE_KINDS",
    "MIN_CLUSTER_SIZE",
    "DENSITY_THRESHOLD",
    "ClusterMember",
    "ClusterResult",
    "cluster_active_models",
    "synthesize_candidate_payload",
]
