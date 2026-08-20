"""
services/workers/precipitation/proposer.py — candidate proposal + guarded promotion.

Handles two transitions:

1. Cluster → pattern_candidate row → T4 pattern_review trigger.
   Called by the nightly precipitation worker.
2. pattern_candidate + semantic Think accept → Pattern Model + promoted_at.
   The deterministic T4 handler must not call this just because a cluster
   exists; clustering is weak evidence, not proof.

Think T4 rejection path (too speculative): mark rejected_at +
rejection_reason. Called by the same Think handler.
"""
from __future__ import annotations

import json
from typing import Any, Literal, Sequence
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.domain.triggers import enqueue_trigger

from services.workers.precipitation.clustering import (
    ClusterResult,
    synthesize_candidate_payload,
)


_log = structlog.get_logger(__name__)


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
            SELECT tenant_id, promoted_at, rejected_at,
                   proposed_signature, observed_tendency,
                   constituent_model_ids, cluster_size, density
            FROM pattern_candidates
            WHERE id = $1
            """,
            cid,
        )
        if row is None:
            continue
        if row["promoted_at"] is not None or row["rejected_at"] is not None:
            continue
        trig_id = await enqueue_trigger(
            conn,
            tenant_id=row["tenant_id"],
            trigger_kind="T4",
            trigger_subkind="pattern_review",
            payload={
                "pattern_candidate_id": str(cid),
                "source": "precipitation_cluster",
                "review_mode": "semantic_required",
                "proposed_signature": _json_obj(row["proposed_signature"]),
                "observed_tendency": _json_obj(row["observed_tendency"]),
                "constituent_model_ids": [
                    str(model_id) for model_id in row["constituent_model_ids"]
                ],
                "cluster_size": int(row["cluster_size"]),
                "density": float(row["density"]),
            },
        )
        out.append(trig_id)
    return out


# ---------------------------------------------------------------------
# Promotion / rejection — called only after semantic review accepts/rejects
# ---------------------------------------------------------------------


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

    # Build the Pattern proposition per services.domain.models.propositions.
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

    # S1 dual-write: every constituent gets a typed `instance_of`
    # edge to the new Pattern AND its supporting_model_ids array
    # gains the Pattern id (legacy back-link preserved). Goes through
    # the chokepoint helper so the drift detector stays happy.
    #
    # Direction note: `instance_of` reads "constituent IS AN INSTANCE
    # OF pattern" — so the typed edge is (constituent, pattern,
    # 'instance_of'). The legacy supporting_model_ids on the
    # constituent gets the pattern id appended (matches pre-S1
    # behavior).
    from services.domain.models.repo import _set_model_relations  # local to avoid circular import
    for constituent_id in cand["constituent_model_ids"]:
        await _set_model_relations(
            conn,
            model_id=constituent_id,
            tenant_id=cand["tenant_id"],
            detected_by="precipitation",
            instance_of=[inserted.id],
            created_by_event_id=born_from_event_id,
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
    await _record_pattern_review_feedback_best_effort(
        conn,
        candidate_id=candidate_id,
        outcome="accepted",
        promoted_pattern_model_id=inserted.id,
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
    row = await conn.fetchrow(
        """
        UPDATE pattern_candidates
        SET rejected_at = now(),
            rejection_reason = $2
        WHERE id = $1
          AND rejected_at IS NULL
          AND promoted_at IS NULL
        RETURNING tenant_id
        """,
        candidate_id,
        reason,
    )
    if row is not None:
        await _record_pattern_review_feedback_best_effort(
            conn,
            candidate_id=candidate_id,
            outcome="rejected",
            rejection_reason=reason,
        )


async def _record_pattern_review_feedback_best_effort(
    conn: asyncpg.Connection,
    *,
    candidate_id: UUID,
    outcome: Literal["accepted", "rejected"],
    promoted_pattern_model_id: UUID | None = None,
    rejection_reason: str | None = None,
) -> None:
    """Let SAGE learn from a reviewed candidate without owning the verdict."""
    from services.reasoning.sage.patterns.feedback import (
        record_pattern_review_feedback,
    )

    try:
        report = await record_pattern_review_feedback(
            conn,
            candidate_id=candidate_id,
            outcome=outcome,
            promoted_pattern_model_id=promoted_pattern_model_id,
            rejection_reason=rejection_reason,
        )
    except Exception:
        _log.exception(
            "pattern_review_feedback_failed",
            candidate_id=str(candidate_id),
            outcome=outcome,
        )
        return
    if report.skipped_reason:
        _log.info(
            "pattern_review_feedback_skipped",
            candidate_id=str(candidate_id),
            outcome=outcome,
            skipped_reason=report.skipped_reason,
        )


__all__ = [
    "write_candidates",
    "enqueue_pattern_review_triggers",
    "promote_pattern_candidate",
    "reject_pattern_candidate",
]
