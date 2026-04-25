"""Determinism tests for query_builders."""

from __future__ import annotations

from datetime import datetime, timezone

from lsob_contracts import GroundTruth

from lsob_evaluator_l2 import query_builders as QB


def _make_gt() -> GroundTruth:
    return GroundTruth(
        timestamp=datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        actors=[{"id": "alice"}],
        commitments=[
            {"id": "C-zeta", "owner": "alice", "true_outcome": "open"},
            {"id": "C-alpha", "owner": "alice", "true_outcome": "will_slip"},
        ],
        customers=[
            {"id": "customer-b", "true_health": "healthy"},
            {"id": "customer-a", "true_health": "warning"},
        ],
        patterns=[
            {"id": "P-two", "description": "x"},
            {"id": "P-one", "description": "y"},
        ],
        predictions_that_will_resolve=[
            {
                "prediction_id": "pr-2",
                "proposition": "foo",
                "asserted_confidence": 0.5,
                "resolves_at": "2026-01-15T00:00:00Z",
                "outcome": "true",
            },
            {
                "prediction_id": "pr-1",
                "proposition": "bar",
                "asserted_confidence": 0.4,
                "resolves_at": "2026-01-10T00:00:00Z",
                "outcome": "false",
            },
        ],
    )


def test_commitment_queries_sorted_and_deterministic():
    gt = _make_gt()
    q1 = QB.commitment_queries(gt)
    q2 = QB.commitment_queries(gt)
    ids = [q.entity_ref.id for q in q1]
    assert ids == sorted(ids)
    assert [q.query_id for q in q1] == [q.query_id for q in q2]
    assert all(q.proposition_kind == "commitment_state" for q in q1)


def test_customer_queries_sorted():
    gt = _make_gt()
    qs = QB.customer_queries(gt)
    assert [q.entity_ref.id for q in qs] == sorted(c["id"] for c in gt.customers)


def test_pattern_queries_sorted():
    gt = _make_gt()
    qs = QB.pattern_queries(gt)
    assert [q.entity_ref.id for q in qs] == sorted(p["id"] for p in gt.patterns)


def test_prediction_queries_sorted():
    gt = _make_gt()
    qs = QB.prediction_queries(gt)
    assert [q.entity_ref.id for q in qs] == sorted(
        p["prediction_id"] for p in gt.predictions_that_will_resolve
    )


def test_all_queries_is_union():
    gt = _make_gt()
    assert len(QB.all_queries_for_checkpoint(gt)) == (
        len(QB.commitment_queries(gt))
        + len(QB.customer_queries(gt))
        + len(QB.pattern_queries(gt))
        + len(QB.prediction_queries(gt))
    )


def test_query_ids_stable_across_reorder():
    gt1 = _make_gt()
    gt2 = GroundTruth(
        timestamp=gt1.timestamp,
        actors=gt1.actors,
        commitments=list(reversed(gt1.commitments)),
        customers=gt1.customers,
        patterns=gt1.patterns,
        predictions_that_will_resolve=gt1.predictions_that_will_resolve,
    )
    ids1 = [q.query_id for q in QB.commitment_queries(gt1)]
    ids2 = [q.query_id for q in QB.commitment_queries(gt2)]
    assert ids1 == ids2
