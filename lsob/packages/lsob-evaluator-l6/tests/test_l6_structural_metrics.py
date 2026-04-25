"""Unit tests for Phase 6a structural metrics."""

from __future__ import annotations

from datetime import datetime, timezone

from lsob_contracts import ActOp, ClaimOp, DiffOp

from lsob_evaluator_l6.metrics import (
    confidence_alignment_rate,
    falsifier_adequacy_rate,
    is_over_split,
    is_under_split,
    over_splitting_rate,
    state_transition_accuracy,
    under_splitting_rate,
)

_NOW = datetime(2026, 1, 15, tzinfo=timezone.utc)


def _diff(*, claim_ops=None, act_ops=None) -> DiffOp:
    return DiffOp(
        diff_id="d",
        produced_at=_NOW,
        claim_ops=list(claim_ops or []),
        act_ops=list(act_ops or []),
    )


def _claim(
    claim_id: str,
    *,
    kind: str = "risk_assessment",
    conf: float = 0.5,
    falsifier: str | None = None,
    entities: list[str] | None = None,
) -> ClaimOp:
    return ClaimOp(
        claim_id=claim_id,
        proposition=f"prop-{claim_id}",
        proposition_kind=kind,
        asserted_confidence=conf,
        falsifier=falsifier,
        entities=entities or ["commitment:C-1"],
    )


def _act(entity_ref: str, to_state: str, from_state: str | None = "on_track") -> ActOp:
    return ActOp(entity_ref=entity_ref, from_state=from_state, to_state=to_state)


def test_state_transition_accuracy_half_match():
    ref = _diff(
        act_ops=[
            _act("commitment:A", "at_risk"),
            _act("commitment:B", "at_risk"),
        ]
    )
    # SUT: one correct, one wrong to_state.
    sut = _diff(
        act_ops=[
            _act("commitment:A", "at_risk"),
            _act("commitment:B", "on_track"),
        ]
    )
    assert state_transition_accuracy(sut, ref) == 0.5


def test_state_transition_accuracy_all_match():
    ref = _diff(act_ops=[_act("commitment:A", "at_risk")])
    sut = _diff(act_ops=[_act("commitment:A", "at_risk")])
    assert state_transition_accuracy(sut, ref) == 1.0


def test_state_transition_accuracy_empty_sides():
    empty = _diff()
    assert state_transition_accuracy(empty, empty) == 1.0
    ref = _diff(act_ops=[_act("commitment:A", "at_risk")])
    assert state_transition_accuracy(empty, ref) == 0.0


def test_confidence_alignment_within_tolerance():
    ref = _diff(
        claim_ops=[
            _claim("r1", conf=0.80, entities=["commitment:C-1"]),
            _claim("r2", conf=0.30, entities=["actor:alice"], kind="capacity"),
        ]
    )
    sut = _diff(
        claim_ops=[
            _claim("s1", conf=0.75, entities=["commitment:C-1"]),  # within 0.15
            _claim("s2", conf=0.10, entities=["actor:alice"], kind="capacity"),  # outside
        ]
    )
    assert confidence_alignment_rate(sut, ref) == 0.5


def test_confidence_alignment_requires_entity_overlap():
    ref = _diff(
        claim_ops=[_claim("r1", conf=0.5, entities=["commitment:A"])]
    )
    # Same kind but different entity — no match, no alignment credit.
    sut = _diff(
        claim_ops=[_claim("s1", conf=0.5, entities=["commitment:B"])]
    )
    assert confidence_alignment_rate(sut, ref) == 0.0


def test_falsifier_adequacy_counts_only_high_confidence():
    diff = _diff(
        claim_ops=[
            _claim("c1", conf=0.85, falsifier="x"),
            _claim("c2", conf=0.90, falsifier=None),
            _claim("c3", conf=0.40, falsifier=None),  # low-confidence, ignored
        ]
    )
    # 1 of 2 high-confidence claims has a non-empty falsifier.
    assert falsifier_adequacy_rate(diff) == 0.5


def test_falsifier_adequacy_vacuous_without_high_confidence():
    diff = _diff(
        claim_ops=[
            _claim("c1", conf=0.40),
            _claim("c2", conf=0.50),
        ]
    )
    assert falsifier_adequacy_rate(diff) == 1.0


def test_over_split_flag_and_rate():
    ref = _diff(
        claim_ops=[_claim(f"r{i}", entities=["commitment:A"]) for i in range(2)]
    )
    sut_over = _diff(
        claim_ops=[_claim(f"s{i}", entities=["commitment:A"]) for i in range(6)]
    )
    sut_ok = _diff(
        claim_ops=[_claim(f"s{i}", entities=["commitment:A"]) for i in range(3)]
    )
    assert is_over_split(sut_over, ref) is True
    assert is_over_split(sut_ok, ref) is False
    assert over_splitting_rate([(sut_over, ref), (sut_ok, ref)]) == 0.5


def test_under_split_flag_and_rate():
    ref = _diff(
        claim_ops=[_claim(f"r{i}", entities=["commitment:A"]) for i in range(3)]
    )
    sut_under = _diff(claim_ops=[_claim("s1", entities=["commitment:A"])])
    sut_ok = _diff(
        claim_ops=[_claim(f"s{i}", entities=["commitment:A"]) for i in range(3)]
    )
    assert is_under_split(sut_under, ref) is True
    assert is_under_split(sut_ok, ref) is False
    assert under_splitting_rate([(sut_under, ref), (sut_ok, ref)]) == 0.5
