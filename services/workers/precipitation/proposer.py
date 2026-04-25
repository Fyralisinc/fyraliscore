"""
services/workers/precipitation/proposer.py — candidate proposal + promotion.

Handles two transitions:

1. Cluster → pattern_candidate row → T4 pattern_review trigger.
   Called by the nightly precipitation worker.
2. pattern_candidate + Think T4 accept → Pattern Model + promoted_at.
   Called by Think's deterministic T4 handler on `pattern_review`.

Think T4 rejection path (too speculative): mark rejected_at +
rejection_reason. Called by the same Think handler.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Sequence
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from services.workers.precipitation.clustering import (
    ClusterResult,
    synthesize_candidate_payload,
)


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


async def write_candidates(
    conn: asyncpg.Connection,
    clusters: Sequence[ClusterResult],
) -> list[UUID]:
    """
    Insert one `pattern_candidates` row per cluster. Returns the list
    of inserted ids in input order.

    Idempotency: we dedupe by `constituent_model_ids` — if a candidate
    row already exists (pending OR resolved) whose constituent set is
    identical, we skip insertion and return the existing id.
    """
    if not clusters:
        return []

    out: list[UUID] = []
    for c in clusters:
        member_ids = sorted(m.model_id for m in c.members)
        existing = await conn.fetchrow(
            """
            SELECT id FROM pattern_candidates
            WHERE tenant_id = $1
              AND constituent_model_ids @> $2::uuid[]
              AND cardinality(constituent_model_ids) = cardinality($2::uuid[])
            LIMIT 1
            """,
            c.tenant_id,
            member_ids,
        )
        if existing is not None:
            out.append(existing["id"])
            continue

        sig, tendency = synthesize_candidate_payload(c)
        new_id = uuid7()
        await conn.execute(
            """
            INSERT INTO pattern_candidates (
                id, tenant_id, proposed_signature, observed_tendency,
                constituent_model_ids, cluster_size, density
            ) VALUES (
                $1, $2, $3::jsonb, $4::jsonb, $5::uuid[], $6, $7
            )
            """,
            new_id,
            c.tenant_id,
            _jsonb(sig),
            _jsonb(tendency),
            member_ids,
            c.size,
            float(c.density),
        )
        out.append(new_id)
    return out


async def enqueue_pattern_review_triggers(
    conn: asyncpg.Connection,
    candidate_ids: Sequence[UUID],
) -> list[UUID]:
    """
    For each candidate, insert a row into `think_trigger_queue` with
    trigger_kind='T4', trigger_subkind='pattern_review', payload =
    {"pattern_candidate_id": <uuid>}. Returns the list of trigger-queue
    ids in input order.

    We only enqueue for pending candidates (neither promoted nor
    rejected). A freshly-inserted candidate is always pending.
    """
    if not candidate_ids:
        return []
    out: list[UUID] = []
    for cid in candidate_ids:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, promoted_at, rejected_at
            FROM pattern_candidates
            WHERE id = $1
            """,
            cid,
        )
        if row is None:
            continue
        if row["promoted_at"] is not None or row["rejected_at"] is not None:
            continue
        trig_id = uuid7()
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind, payload
            ) VALUES ($1, $2, 'T4', 'pattern_review', $3::jsonb)
            """,
            trig_id,
            row["tenant_id"],
            _jsonb({"pattern_candidate_id": str(cid)}),
        )
        out.append(trig_id)
    return out


# ---------------------------------------------------------------------
# Promotion / rejection — called by Think T4 `pattern_review` branch
# ---------------------------------------------------------------------


async def promote_pattern_candidate(
    conn: asyncpg.Connection,
    candidate_id: UUID,
    *,
    models_repo,
    born_from_event_id: UUID,
    pattern_confidence: float = 0.7,
) -> UUID:
    """
    Insert a Pattern Model from a pattern_candidates row, flip
    promoted_at + promoted_pattern_model_id, and link constituents
    back via their supporting_model_ids.

    Parameters
    ----------
    conn
        asyncpg.Connection inside a transaction.
    candidate_id
        The pattern_candidates.id to promote.
    models_repo
        A ModelsRepo (pool + optional embedder).
    born_from_event_id
        Observation id that triggered the Think T4 run. Required by
        Model schema.
    pattern_confidence
        The inserted Pattern Model's initial confidence. Defaults to
        0.7 — below the adequate-falsifier threshold so the Pattern
        can be inserted without one (spec §10: "Required if confidence
        > 0.7"). Raising above 0.7 requires supplying a falsifier in
        the payload.

    Returns
    -------
    UUID
        The inserted Pattern Model's id.
    """
    cand = await conn.fetchrow(
        """
        SELECT tenant_id, proposed_signature, observed_tendency,
               constituent_model_ids, promoted_at, rejected_at
        FROM pattern_candidates
        WHERE id = $1
        FOR UPDATE
        """,
        candidate_id,
    )
    if cand is None:
        raise ValidationError(
            f"pattern_candidate {candidate_id} not found",
            candidate_id=str(candidate_id),
        )
    if cand["promoted_at"] is not None:
        raise ValidationError(
            f"pattern_candidate {candidate_id} already promoted",
            candidate_id=str(candidate_id),
        )
    if cand["rejected_at"] is not None:
        raise ValidationError(
            f"pattern_candidate {candidate_id} already rejected",
            candidate_id=str(candidate_id),
        )

    sig = cand["proposed_signature"]
    tendency = cand["observed_tendency"]
    if isinstance(sig, (bytes, bytearray)):
        sig = json.loads(sig.decode())
    if isinstance(sig, str):
        sig = json.loads(sig)
    if isinstance(tendency, (bytes, bytearray)):
        tendency = json.loads(tendency.decode())
    if isinstance(tendency, str):
        tendency = json.loads(tendency)
    tendency_text = tendency.get("exemplars", [""])
    natural_tendency = tendency_text[0] if tendency_text else "pattern"

    # Build the Pattern proposition per services.models.propositions.
    proposition = {
        "kind": "pattern",
        "signature": sig,
        "observed_tendency": natural_tendency,
        "trigger_conditions": {
            "cluster_density": tendency.get("cluster_density", 0),
            "cluster_size": tendency.get("cluster_size", 0),
        },
    }

    from lib.shared.types import ModelCreate

    # Embedding: compute the centroid of the constituent Models'
    # embeddings. The Pattern Model inherits the cluster's semantic
    # location, which is exactly what retrieval wants: the Pattern
    # matches queries that matched any of its constituents.
    centroid = await _compute_centroid_embedding(
        conn, cand["constituent_model_ids"]
    )
    natural = (
        f"Pattern proposal: {sig.get('kind','cluster_signature')} across "
        f"{len(cand['constituent_model_ids'])} related Models"
    )
    payload = ModelCreate(
        tenant_id=cand["tenant_id"],
        born_from_event_id=born_from_event_id,
        proposition=proposition,
        natural=natural,
        embedding=centroid,
        scope_actors=[],
        scope_entities=[],
        scope_temporal={"kind": "open_ended"},
        confidence=pattern_confidence,
        falsifier=None,
        signal_readings=[],
        reading_contestable=True,
        supporting_event_ids=[],
        supporting_model_ids=list(cand["constituent_model_ids"]),
        evidential_weight=0.5,
        visible_to_subjects=True,
        confidence_at_assertion=pattern_confidence,
        activation_coefficient=1.0,
        evaluate_at=None,
        resolution_criteria=None,
        contributing_models=[],
    )
    inserted = await models_repo.insert(payload, conn=conn)

    # Back-link: every constituent gains a supporting_model_ids pointer
    # to the new Pattern (so retrieval walking `supporting_model_ids`
    # sees the Pattern as a reason to pull this Model).
    await conn.execute(
        """
        UPDATE models
        SET supporting_model_ids = supporting_model_ids || ARRAY[$1]::uuid[]
        WHERE id = ANY($2::uuid[])
          AND NOT (supporting_model_ids @> ARRAY[$1]::uuid[])
        """,
        inserted.id,
        list(cand["constituent_model_ids"]),
    )

    await conn.execute(
        """
        UPDATE pattern_candidates
        SET promoted_at = now(),
            promoted_pattern_model_id = $2
        WHERE id = $1
        """,
        candidate_id,
        inserted.id,
    )
    return inserted.id


async def _compute_centroid_embedding(
    conn: asyncpg.Connection,
    model_ids: list[UUID],
) -> list[float]:
    """
    Average + L2-normalise the embeddings of `model_ids`. Used as
    the Pattern Model's embedding so retrieval on the Pattern
    semantically matches any of its constituents.
    """
    from pgvector.asyncpg import register_vector
    try:
        await register_vector(conn)
    except Exception:
        pass
    import numpy as np
    rows = await conn.fetch(
        "SELECT embedding FROM models WHERE id = ANY($1::uuid[]) AND embedding IS NOT NULL",
        list(model_ids),
    )
    if not rows:
        raise ValidationError(
            "no embeddings found for pattern_candidate constituents",
            constituent_ids=[str(m) for m in model_ids],
        )
    X = np.array([r["embedding"] for r in rows], dtype=np.float64)
    centroid = X.mean(axis=0)
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    return centroid.tolist()


async def reject_pattern_candidate(
    conn: asyncpg.Connection,
    candidate_id: UUID,
    *,
    reason: str,
) -> None:
    """Mark a pattern_candidate as rejected. Idempotent."""
    await conn.execute(
        """
        UPDATE pattern_candidates
        SET rejected_at = now(),
            rejection_reason = $2
        WHERE id = $1
          AND rejected_at IS NULL
          AND promoted_at IS NULL
        """,
        candidate_id,
        reason,
    )


__all__ = [
    "write_candidates",
    "enqueue_pattern_review_triggers",
    "promote_pattern_candidate",
    "reject_pattern_candidate",
]
