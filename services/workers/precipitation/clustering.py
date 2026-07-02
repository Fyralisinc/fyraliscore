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

import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import numpy as np


# HDBSCAN and its sklearn dependency are heavy imports; lazy-import
# them inside `cluster_active_models` so unit tests that don't need
# clustering can still load this module fast.


MIN_CLUSTER_SIZE = 3
DENSITY_THRESHOLD = 0.5

# We only cluster these claim roles per spec §19 "repeated
# pattern_instance Models. But instance accumulation doesn't
# automatically precipitate — it needs a dedicated worker that
# evaluates whether candidates have enough support" — and per
# BUILD-PLAN 4.C's guidance to cluster "hypothesis / concern" Models.
CLUSTERABLE_KINDS: frozenset[str] = frozenset(("hypothesis", "concern"))


@dataclass(frozen=True)
class ClusterCounterexample:
    model_id: UUID
    reason: str
    observed_outcome: str | None = None
    shared_terms: tuple[str, ...] = ()
    shared_actor_refs: tuple[UUID, ...] = ()
    shared_entity_refs: tuple[str, ...] = ()
    natural: str = ""


@dataclass(frozen=True)
class ClusterMember:
    model_id: UUID
    proposition_kind: str
    natural: str
    proposition: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    scope_actors: tuple[UUID, ...] = ()
    scope_entities: tuple[str, ...] = ()
    domain_tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClusterResult:
    """One dense embedding cluster of hypothesis/concern Models."""
    tenant_id: UUID
    members: tuple[ClusterMember, ...]
    density: float
    counterexamples: tuple[ClusterCounterexample, ...] = ()

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
        "claim_role = ANY($1::text[])",
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
        SELECT id, tenant_id, proposition_kind, claim_role, proposition,
               "natural", embedding,
               created_at, scope_actors, scope_entities, domain_tags
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
    if all(label < 0 for label in labels) and len(rows) < 30:
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

    all_members = tuple(_member_from_row(row) for row in rows)
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
            members = tuple(all_members[i] for i in sub_idxs)
            results.append(ClusterResult(
                tenant_id=t_id,
                members=members,
                density=sub_density,
            ))
    return attach_cross_cluster_counterexamples(
        results,
        all_members=all_members,
    )


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
    review_features = _review_features(cluster)
    proposed_signature = {
        "kind": "cluster_signature",
        "constituent_kinds": kinds,
        "member_count": cluster.size,
        "review_feature_axes": review_features["feature_axes"],
    }
    observed_tendency = {
        "exemplars": exemplars,
        "cluster_density": round(cluster.density, 4),
        "cluster_size": cluster.size,
        "review_features": review_features,
    }
    return proposed_signature, observed_tendency


def attach_cross_cluster_counterexamples(
    clusters: list[ClusterResult],
    *,
    all_members: tuple[ClusterMember, ...] | None = None,
    max_counterexamples_per_cluster: int = 8,
) -> list[ClusterResult]:
    """Attach bounded non-member counterexamples to candidate clusters."""

    if not clusters:
        return []
    pool = all_members or tuple(
        member for cluster in clusters for member in cluster.members
    )
    return [
        replace(
            cluster,
            counterexamples=_cross_cluster_counterexamples(
                cluster,
                pool=pool,
                max_counterexamples=max_counterexamples_per_cluster,
            ),
        )
        for cluster in clusters
    ]


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]{2,}")
_STOPWORDS = frozenset({
    "and",
    "are",
    "but",
    "for",
    "from",
    "has",
    "have",
    "into",
    "not",
    "our",
    "that",
    "the",
    "this",
    "was",
    "with",
})


def _review_features(cluster: ClusterResult) -> dict[str, Any]:
    shared_terms = _shared_terms(cluster.members)
    shared_actor_refs = _shared_actor_refs(cluster.members)
    shared_entity_refs = _shared_entity_refs(cluster.members)
    shared_outcome_refs = _shared_outcome_refs(cluster.members)
    candidate_counterexamples = _candidate_counterexample_count(cluster.members)
    cross_counterexamples = cluster.counterexamples
    domain_tags = tuple(
        sorted({tag for member in cluster.members for tag in member.domain_tags if tag})
    )
    temporal_span_days = _temporal_span_days(cluster.members)
    feature_axes: list[str] = []
    if shared_terms:
        feature_axes.append("lexical_recurrence")
    if shared_actor_refs:
        feature_axes.append("shared_actors")
    if shared_entity_refs:
        feature_axes.append("shared_entities")
    if shared_outcome_refs:
        feature_axes.append("outcome_recurrence")
    if len(domain_tags) >= 2:
        feature_axes.append("cross_domain")
    if temporal_span_days is not None and temporal_span_days >= 1.0:
        feature_axes.append("temporal_recurrence")
    if candidate_counterexamples > 0:
        feature_axes.append("candidate_counterexamples")
    if cross_counterexamples:
        feature_axes.append("cross_cluster_counterexamples")
    counterexample_count = candidate_counterexamples + len(cross_counterexamples)
    return {
        "feature_axes": feature_axes,
        "evidence_axis_count": len(feature_axes),
        "shared_terms": list(shared_terms),
        "shared_actor_refs": [str(actor_id) for actor_id in shared_actor_refs],
        "shared_entity_refs": list(shared_entity_refs),
        "shared_outcome_refs": list(shared_outcome_refs),
        "candidate_counterexample_count": candidate_counterexamples,
        "cross_cluster_counterexample_count": len(cross_counterexamples),
        "counterexample_count": counterexample_count,
        "cross_cluster_counterexamples": [
            _counterexample_payload(counterexample)
            for counterexample in cross_counterexamples
        ],
        "domain_tags": list(domain_tags[:8]),
        "temporal_span_days": temporal_span_days,
        "constituent_kind_count": len({m.proposition_kind for m in cluster.members}),
        "review_caution": (
            "Weak statistical cluster; promote only if selected evidence proves "
            "a stable, useful, explainable, falsifiable, action-shaping pattern."
        ),
    }


def _cross_cluster_counterexamples(
    cluster: ClusterResult,
    *,
    pool: tuple[ClusterMember, ...],
    max_counterexamples: int,
) -> tuple[ClusterCounterexample, ...]:
    member_ids = {member.model_id for member in cluster.members}
    candidate_terms = set(_shared_terms(cluster.members))
    candidate_actor_refs = set(_cluster_actor_refs(cluster.members))
    candidate_entity_refs = set(_cluster_entity_refs(cluster.members))
    candidate_outcome = _majority_observed_outcome(cluster.members)
    if not candidate_outcome:
        return ()

    scored: list[tuple[float, ClusterCounterexample]] = []
    for member in pool:
        if member.model_id in member_ids:
            continue
        observed_outcome = _outcome_value(member.proposition, "observed_outcome")
        explicit_counterexample = bool(member.proposition.get("counterexample"))
        if not observed_outcome and not explicit_counterexample:
            continue
        if observed_outcome == candidate_outcome and not explicit_counterexample:
            continue
        shared_terms = tuple(
            sorted(candidate_terms.intersection(_member_terms(member)))[:8]
        )
        shared_actor_refs = tuple(
            sorted(
                candidate_actor_refs.intersection(member.scope_actors),
                key=str,
            )[:8]
        )
        shared_entity_refs = tuple(
            sorted(candidate_entity_refs.intersection(member.scope_entities))[:8]
        )
        if not (shared_terms or shared_actor_refs or shared_entity_refs):
            continue
        reason = (
            "explicit_counterexample"
            if explicit_counterexample
            else "shared_shape_conflicting_observed_outcome"
        )
        score = (
            len(shared_terms)
            + 2.0 * len(shared_actor_refs)
            + 2.0 * len(shared_entity_refs)
            + (1.5 if explicit_counterexample else 0.0)
        )
        scored.append(
            (
                score,
                ClusterCounterexample(
                    model_id=member.model_id,
                    reason=reason,
                    observed_outcome=observed_outcome,
                    shared_terms=shared_terms,
                    shared_actor_refs=shared_actor_refs,
                    shared_entity_refs=shared_entity_refs,
                    natural=member.natural[:220],
                ),
            )
        )
    scored.sort(key=lambda item: (-item[0], str(item[1].model_id)))
    return tuple(
        counterexample
        for _score, counterexample in scored[: max(0, int(max_counterexamples))]
    )


def _counterexample_payload(counterexample: ClusterCounterexample) -> dict[str, Any]:
    return {
        "model_id": str(counterexample.model_id),
        "reason": counterexample.reason,
        "observed_outcome": counterexample.observed_outcome,
        "shared_terms": list(counterexample.shared_terms),
        "shared_actor_refs": [str(actor_id) for actor_id in counterexample.shared_actor_refs],
        "shared_entity_refs": list(counterexample.shared_entity_refs),
        "natural": counterexample.natural,
    }


def _shared_terms(members: tuple[ClusterMember, ...]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for member in members:
        terms = {
            token
            for token in _TOKEN_RE.findall((member.natural or "").lower())
            if token not in _STOPWORDS
        }
        for term in terms:
            counts[term] = counts.get(term, 0) + 1
    return tuple(
        term
        for term, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if count >= 2
    )[:12]


def _member_terms(member: ClusterMember) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall((member.natural or "").lower())
        if token not in _STOPWORDS
    }


def _cluster_actor_refs(members: tuple[ClusterMember, ...]) -> tuple[UUID, ...]:
    return tuple(
        sorted(
            {actor_id for member in members for actor_id in member.scope_actors},
            key=str,
        )
    )


def _cluster_entity_refs(members: tuple[ClusterMember, ...]) -> tuple[str, ...]:
    return tuple(
        sorted({entity_ref for member in members for entity_ref in member.scope_entities})
    )


def _shared_actor_refs(members: tuple[ClusterMember, ...]) -> tuple[UUID, ...]:
    counts: dict[UUID, int] = {}
    for member in members:
        for actor_id in set(member.scope_actors):
            counts[actor_id] = counts.get(actor_id, 0) + 1
    return tuple(
        actor_id
        for actor_id, count in sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))
        if count >= 2
    )[:8]


def _shared_entity_refs(members: tuple[ClusterMember, ...]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for member in members:
        for entity_ref in set(member.scope_entities):
            counts[entity_ref] = counts.get(entity_ref, 0) + 1
    return tuple(
        entity_ref
        for entity_ref, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if count >= 2
    )[:8]


def _shared_outcome_refs(members: tuple[ClusterMember, ...]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for member in members:
        for outcome_ref in set(_outcome_refs(member.proposition)):
            counts[outcome_ref] = counts.get(outcome_ref, 0) + 1
    return tuple(
        outcome_ref
        for outcome_ref, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if count >= 2
    )[:8]


def _candidate_counterexample_count(members: tuple[ClusterMember, ...]) -> int:
    observed = [
        outcome
        for member in members
        if (outcome := _outcome_value(member.proposition, "observed_outcome"))
    ]
    if len(set(observed)) <= 1:
        return 0
    counts: dict[str, int] = {}
    for outcome in observed:
        counts[outcome] = counts.get(outcome, 0) + 1
    majority = max(counts.values())
    return max(0, len(observed) - majority)


def _majority_observed_outcome(members: tuple[ClusterMember, ...]) -> str | None:
    observed = [
        outcome
        for member in members
        if (outcome := _outcome_value(member.proposition, "observed_outcome"))
    ]
    if not observed:
        return None
    counts: dict[str, int] = {}
    for outcome in observed:
        counts[outcome] = counts.get(outcome, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _outcome_refs(proposition: dict[str, Any]) -> tuple[str, ...]:
    values = [
        _outcome_value(proposition, "observed_outcome"),
        _outcome_value(proposition, "expected_outcome"),
        _outcome_value(proposition, "outcome"),
    ]
    return tuple(sorted({f"outcome:{value}" for value in values if value}))


def _outcome_value(proposition: dict[str, Any], key: str) -> str | None:
    raw = proposition.get(key)
    if raw is None and isinstance(proposition.get("metadata"), dict):
        raw = proposition["metadata"].get(key)
    if raw is None:
        return None
    text = str(raw).strip().lower().replace(" ", "_")
    return text[:80] or None


def _temporal_span_days(members: tuple[ClusterMember, ...]) -> float | None:
    timestamps = [member.created_at for member in members if member.created_at is not None]
    if len(timestamps) < 2:
        return None
    normalized = [
        ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        for ts in timestamps
    ]
    span = max(normalized) - min(normalized)
    return round(max(0.0, span.total_seconds() / 86400.0), 3)


def _json_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _member_from_row(row: Any) -> ClusterMember:
    return ClusterMember(
        model_id=row["id"],
        proposition_kind=row["claim_role"] or row["proposition_kind"],
        natural=row["natural"],
        proposition=_json_obj(row["proposition"]),
        created_at=row["created_at"],
        scope_actors=tuple(row["scope_actors"] or ()),
        scope_entities=_scope_entity_refs(row["scope_entities"]),
        domain_tags=tuple(str(tag) for tag in row["domain_tags"] or ()),
    )


def _scope_entity_refs(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, list):
        return ()
    refs: list[str] = []
    for item in value:
        if isinstance(item, dict):
            entity_type = str(item.get("type") or item.get("entity_type") or "").strip()
            entity_id = str(item.get("id") or item.get("entity_id") or "").strip()
            if entity_type and entity_id:
                refs.append(f"{entity_type}:{entity_id}")
        elif item:
            refs.append(str(item))
    return tuple(sorted(dict.fromkeys(refs)))


__all__ = [
    "CLUSTERABLE_KINDS",
    "ClusterCounterexample",
    "MIN_CLUSTER_SIZE",
    "DENSITY_THRESHOLD",
    "ClusterMember",
    "ClusterResult",
    "attach_cross_cluster_counterexamples",
    "cluster_active_models",
    "synthesize_candidate_payload",
]
