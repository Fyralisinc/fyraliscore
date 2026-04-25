"""Deterministic BeliefQuery builders from ground-truth entities.

The evaluator must ask the same questions every time against the same
ground truth, so query_id + ordering here must be fully deterministic.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from lsob_contracts import BeliefQuery, EntityRef, GroundTruth


def _query_id(kind: str, entity_id: str, timestamp: datetime) -> str:
    # ISO timestamps are already deterministic; strip microseconds for stability.
    ts = timestamp.replace(microsecond=0).isoformat()
    return f"l2::{kind}::{entity_id}::{ts}"


def commitment_queries(gt: GroundTruth) -> list[BeliefQuery]:
    """One BeliefQuery per ground-truth commitment, sorted by id."""
    items = sorted(gt.commitments, key=lambda c: c["id"])
    return [
        BeliefQuery(
            query_id=_query_id("commitment_state", c["id"], gt.timestamp),
            entity_ref=EntityRef(kind="commitment", id=c["id"]),
            timestamp=gt.timestamp,
            proposition_kind="commitment_state",
        )
        for c in items
    ]


def customer_queries(gt: GroundTruth) -> list[BeliefQuery]:
    items = sorted(gt.customers, key=lambda c: c["id"])
    return [
        BeliefQuery(
            query_id=_query_id("customer_health", c["id"], gt.timestamp),
            entity_ref=EntityRef(kind="customer", id=c["id"]),
            timestamp=gt.timestamp,
            proposition_kind="customer_health",
        )
        for c in items
    ]


def pattern_queries(gt: GroundTruth) -> list[BeliefQuery]:
    items = sorted(gt.patterns, key=lambda p: p["id"])
    return [
        BeliefQuery(
            query_id=_query_id("pattern", p["id"], gt.timestamp),
            entity_ref=EntityRef(kind="pattern", id=p["id"]),
            timestamp=gt.timestamp,
            proposition_kind="pattern",
        )
        for p in items
    ]


def prediction_queries(gt: GroundTruth) -> list[BeliefQuery]:
    """Predictions are not real entities; we use a synthetic EntityRef('model').

    The harness-owned prediction registry lives on the SUT itself; this query
    simply asks the SUT for all prediction beliefs at the checkpoint.
    """
    items = sorted(
        gt.predictions_that_will_resolve, key=lambda p: p["prediction_id"]
    )
    return [
        BeliefQuery(
            query_id=_query_id("prediction", p["prediction_id"], gt.timestamp),
            entity_ref=EntityRef(kind="model", id=p["prediction_id"]),
            timestamp=gt.timestamp,
            proposition_kind="prediction",
        )
        for p in items
    ]


def all_queries_for_checkpoint(gt: GroundTruth) -> list[BeliefQuery]:
    """Everything the L2 evaluator asks at one checkpoint, in stable order."""
    out: list[BeliefQuery] = []
    out.extend(commitment_queries(gt))
    out.extend(customer_queries(gt))
    out.extend(pattern_queries(gt))
    out.extend(prediction_queries(gt))
    return out


def iter_checkpoint_entities(
    gts: Iterable[GroundTruth],
) -> list[tuple[GroundTruth, list[dict[str, Any]]]]:
    """Convenience: pair each checkpoint with its commitments, sorted."""
    return [
        (gt, sorted(gt.commitments, key=lambda c: c["id"])) for gt in gts
    ]
