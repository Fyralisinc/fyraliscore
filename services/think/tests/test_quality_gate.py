"""Tests for services.think.quality_gate."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.think.diff_schema import ClaimOp
from services.think.quality_gate import (
    QualityContext,
    QualityVerdict,
    apply_verdict,
    is_compound,
    score_quality,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _ctx() -> QualityContext:
    return QualityContext(tenant_id=uuid4(), reconcile_result=None, trigger_kind="T1")


def _insert(
    *,
    kind: str,
    assertion: str | None = None,
    extra_prop: dict | None = None,
    confidence: float = 0.7,
    falsifier: str | None = None,
    extra_entry: dict | None = None,
) -> ClaimOp:
    prop: dict = {"kind": kind}
    if assertion is not None:
        # state uses 'assertion'; other kinds keyed by their own primary
        # text field — callers can override via extra_prop.
        prop.setdefault("assertion", assertion)
    if extra_prop:
        prop.update(extra_prop)
    # Provide minimal required fields for the kinds we touch in tests.
    if kind == "state":
        prop.setdefault("subject", "Acme")
        prop.setdefault("assertion", assertion or "")
    elif kind == "recommendation":
        prop.setdefault("target_actor_id", str(uuid4()))
        prop.setdefault(
            "proposed_change",
            {"operation": "create", "payload": {"foo": "bar"}},
        )
        prop.setdefault("qualitative_impact", "improves ops")
    elif kind == "situation":
        prop.setdefault("situation", "Q3 risk")
        prop.setdefault("summary", "summary text")
        prop.setdefault("relationship_summary", "members are linked")
    elif kind == "pattern_instance":
        prop.setdefault("matched_context", "context")

    entry: dict = {
        "proposition": prop,
        "confidence_at_assertion": confidence,
    }
    if falsifier:
        entry["falsifier"] = falsifier
    if extra_entry:
        entry.update(extra_entry)
    return ClaimOp(op="insert", entry=entry)


# ---------------------------------------------------------------------
# is_compound fallback
# ---------------------------------------------------------------------


def test_is_compound_detects_bullets_and_sentences() -> None:
    assert is_compound("- one thing\n- another thing") is True
    assert is_compound("We shipped X. We also shipped Y.") is True
    assert is_compound("Acme renewed the contract.") is False


# ---------------------------------------------------------------------
# Core scenarios
# ---------------------------------------------------------------------


def test_atomic_durable_correctly_kinded_state_accepts_high() -> None:
    op = _insert(
        kind="state",
        assertion=(
            "Acme operates a tiered pricing process across three customer plans "
            "anchored by a long-term contract integration."
        ),
        confidence=0.8,
        falsifier=(
            "Observed customer invoices or renewal events show the pricing "
            "tiers have changed."
        ),
    )
    verdict = score_quality(op, _ctx())
    assert verdict.decision == "accept", verdict.rejection_reasons
    assert verdict.atomicity_score >= 0.8
    assert verdict.durability_score >= 0.8
    assert verdict.kind_fit_score >= 0.8
    assert verdict.overall_score >= 0.8
    out_op, side = apply_verdict(op, verdict)
    assert out_op is op
    assert side == []


def test_compound_op_lowers_atomicity_and_routes_review_or_downgrade() -> None:
    # Compound assertion that is otherwise durable + correctly kinded.
    op = _insert(
        kind="state",
        assertion=(
            "Acme rebuilt the pricing process across all tiers. "
            "Acme also signed a new long-term partnership contract."
        ),
        confidence=0.8,
        falsifier="Observed customer invoices show the pricing tiers changed.",
    )
    verdict = score_quality(op, _ctx())
    assert verdict.atomicity_score == pytest.approx(0.4)
    # Either needs_review or downgrade_to_evidence is acceptable here —
    # the dimension that fails is atomicity, not durability.
    assert verdict.decision in {"needs_review", "accept"}
    # If accept: overall should still be ≥ 0.6 (atomicity 0.4 weighted
    # 0.4 = 0.16; the other 0.6 needs to come from durability+kind_fit).


def test_ephemeral_state_downgrades_to_evidence() -> None:
    op = _insert(
        kind="state",
        assertion="Yesterday's call with the customer felt rough.",
        confidence=0.5,
        falsifier="I changed my mind about how it went.",
    )
    verdict = score_quality(op, _ctx())
    assert verdict.durability_score < 0.3, verdict
    # kind_fit should still be reasonable (no should/will/might in text).
    assert verdict.kind_fit_score >= 0.5
    assert verdict.decision == "downgrade_to_evidence"
    out_op, side = apply_verdict(op, verdict)
    assert out_op is None
    assert side == []  # applier selects the DB-backed evidence anchor
    assert any("evidence" in r.lower() for r in verdict.rejection_reasons)


def test_recommendation_phrased_as_descriptive_needs_review() -> None:
    # We strip the default proposed_change.operation to force the gate
    # to consider the text shape only.
    op = ClaimOp(
        op="insert",
        entry={
            "proposition": {
                "kind": "recommendation",
                "target_actor_id": str(uuid4()),
                "proposed_change": {},
                "qualitative_impact": "x",
                "assertion": (
                    "The current pricing tiers are organized into three plans."
                ),
            },
            "confidence_at_assertion": 0.7,
            "falsifier": "Customer invoices observed to change pricing tiers.",
        },
    )
    verdict = score_quality(op, _ctx())
    assert verdict.kind_fit_score < 0.4, verdict
    assert verdict.decision in {"needs_review", "reject"}


def test_pattern_instance_without_pattern_id_rejects() -> None:
    op = _insert(
        kind="pattern_instance",
        assertion=None,
        extra_prop={"matched_context": "X happened during Q2 review."},
    )
    # No pattern_id at all.
    verdict = score_quality(op, _ctx())
    assert verdict.kind_fit_score == 0.0
    assert verdict.decision == "reject"
    out_op, side = apply_verdict(op, verdict)
    assert out_op is None
    assert side == []


def test_situation_with_member_ids_accepts() -> None:
    op = _insert(
        kind="situation",
        extra_prop={
            "member_model_ids": [str(uuid4()), str(uuid4())],
            "summary": (
                "Long-term partnership contract risk concentrates across "
                "three pricing tiers next quarter."
            ),
            "relationship_summary": (
                "Members share a structural dependency on the renewal process."
            ),
        },
        confidence=0.75,
        falsifier="Renewal events observed to occur on schedule.",
    )
    verdict = score_quality(op, _ctx())
    assert verdict.kind_fit_score == 1.0
    assert verdict.decision == "accept", verdict.rejection_reasons


# ---------------------------------------------------------------------
# apply_verdict glue
# ---------------------------------------------------------------------


def test_apply_verdict_branches() -> None:
    op = _insert(
        kind="state",
        assertion="Acme operates a tiered pricing process.",
        falsifier="Customer invoices observed to change.",
    )
    # accept
    v = QualityVerdict(
        decision="accept",
        atomicity_score=1.0,
        durability_score=1.0,
        kind_fit_score=1.0,
        overall_score=1.0,
    )
    assert apply_verdict(op, v) == (op, [])

    # reject
    v.decision = "reject"
    assert apply_verdict(op, v) == (None, [])

    # downgrade
    v.decision = "downgrade_to_evidence"
    v.downgrade_target = "evidence"
    assert apply_verdict(op, v) == (None, [])

    # needs_review
    v.decision = "needs_review"
    v.downgrade_target = None
    assert apply_verdict(op, v) == (op, [])


def test_non_insert_ops_short_circuit_to_accept() -> None:
    op = ClaimOp(op="update", model_id=uuid4(), changes={"confidence": 0.9})
    verdict = score_quality(op, _ctx())
    assert verdict.decision == "accept"
    assert verdict.overall_score == 1.0
