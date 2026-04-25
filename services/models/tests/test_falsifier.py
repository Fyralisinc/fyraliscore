"""
services/models/tests/test_falsifier.py — spec §10 adequacy rules.

All unit tests — no DB. Cover every one of the five falsifier kinds
and their adequacy predicates plus the "no falsifier / unknown kind /
non-dict" paths.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.models.falsifier import (
    LEGAL_FALSIFIER_KINDS,
    is_adequate_falsifier,
)


def test_none_falsifier_inadequate() -> None:
    ok, reason = is_adequate_falsifier(None)
    assert not ok and "no falsifier" in reason


def test_non_dict_falsifier_inadequate() -> None:
    ok, reason = is_adequate_falsifier("a string")  # type: ignore[arg-type]
    assert not ok and "dict" in reason


def test_missing_kind_inadequate() -> None:
    ok, reason = is_adequate_falsifier({"pattern": "..."})
    assert not ok and "kind" in reason


def test_unknown_kind_inadequate() -> None:
    ok, reason = is_adequate_falsifier({"kind": "magic"})
    assert not ok and "unknown" in reason.lower()


def test_legal_kinds_matches_spec() -> None:
    """§10: five falsifier kinds; spec authoritative over BUILD-PLAN paraphrase."""
    assert LEGAL_FALSIFIER_KINDS == frozenset(
        {
            "observation_pattern",
            "commitment_outcome",
            "prediction_deadline",
            "resource_threshold",
            "explicit_contestation",
        }
    )


# ---------------------------------------------------------------------
# observation_pattern
# ---------------------------------------------------------------------


def test_observation_pattern_adequate() -> None:
    ok, reason = is_adequate_falsifier(
        {
            "kind": "observation_pattern",
            "pattern": "any Observation from authoritative source stating X",
            "within_window": "any 4-week period",
        }
    )
    assert ok, reason


def test_observation_pattern_short_pattern_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "observation_pattern", "pattern": "short", "within_window": "4w"}
    )
    assert not ok and "vague" in reason


def test_observation_pattern_missing_window_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "observation_pattern", "pattern": "a" * 40}
    )
    assert not ok and "window" in reason


# ---------------------------------------------------------------------
# commitment_outcome
# ---------------------------------------------------------------------


def test_commitment_outcome_adequate() -> None:
    ok, reason = is_adequate_falsifier(
        {
            "kind": "commitment_outcome",
            "commitment_ref": "c-187",
            "contradicting_state": ["Closed", "DoneVerified with >30% overtime"],
        }
    )
    assert ok, reason


def test_commitment_outcome_missing_ref_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "commitment_outcome", "contradicting_state": ["Closed"]}
    )
    assert not ok and "commitment reference" in reason


def test_commitment_outcome_empty_state_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "commitment_outcome", "commitment_ref": "c-1", "contradicting_state": []}
    )
    assert not ok and "contradicting state" in reason


# ---------------------------------------------------------------------
# prediction_deadline
# ---------------------------------------------------------------------


def test_prediction_deadline_adequate_future() -> None:
    future = (datetime.now(tz=timezone.utc) + timedelta(days=14)).isoformat()
    ok, reason = is_adequate_falsifier(
        {
            "kind": "prediction_deadline",
            "evaluate_at": future,
            "check": "Commitment c-187 in state DoneVerified",
        }
    )
    assert ok, reason


def test_prediction_deadline_past_inadequate() -> None:
    past = (datetime.now(tz=timezone.utc) - timedelta(days=1)).isoformat()
    ok, reason = is_adequate_falsifier(
        {"kind": "prediction_deadline", "evaluate_at": past, "check": "X"}
    )
    assert not ok and "past" in reason


def test_prediction_deadline_missing_check_inadequate() -> None:
    future = (datetime.now(tz=timezone.utc) + timedelta(days=14)).isoformat()
    ok, reason = is_adequate_falsifier(
        {"kind": "prediction_deadline", "evaluate_at": future}
    )
    assert not ok and "check" in reason


def test_prediction_deadline_missing_evaluate_at_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "prediction_deadline", "check": "X"}
    )
    assert not ok and "evaluate_at" in reason


# ---------------------------------------------------------------------
# resource_threshold
# ---------------------------------------------------------------------


def test_resource_threshold_adequate() -> None:
    ok, reason = is_adequate_falsifier(
        {
            "kind": "resource_threshold",
            "resource_ref": "r-eng-capacity",
            "threshold": "available_capacity < 0.20",
        }
    )
    assert ok, reason


def test_resource_threshold_missing_ref_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "resource_threshold", "threshold": "x < 1"}
    )
    assert not ok and "resource reference" in reason


def test_resource_threshold_missing_threshold_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "resource_threshold", "resource_ref": "r-1"}
    )
    assert not ok and "threshold" in reason


# ---------------------------------------------------------------------
# explicit_contestation
# ---------------------------------------------------------------------


def test_explicit_contestation_adequate() -> None:
    ok, reason = is_adequate_falsifier(
        {
            "kind": "explicit_contestation",
            "contesting_actors": ["alice", "bob"],
            "within_days": 90,
        }
    )
    assert ok, reason


def test_explicit_contestation_empty_actors_inadequate() -> None:
    ok, reason = is_adequate_falsifier(
        {"kind": "explicit_contestation", "contesting_actors": []}
    )
    assert not ok and "contesting actors" in reason


def test_explicit_contestation_missing_actors_inadequate() -> None:
    ok, reason = is_adequate_falsifier({"kind": "explicit_contestation"})
    assert not ok and "contesting actors" in reason


def test_prediction_deadline_respects_injected_now() -> None:
    """Deterministic `now` injection lets us test the boundary condition."""
    fake_now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ok_future, _ = is_adequate_falsifier(
        {
            "kind": "prediction_deadline",
            "evaluate_at": "2027-01-01T00:00:00Z",
            "check": "X",
        },
        now=fake_now,
    )
    ok_past, reason = is_adequate_falsifier(
        {
            "kind": "prediction_deadline",
            "evaluate_at": "2025-01-01T00:00:00Z",
            "check": "X",
        },
        now=fake_now,
    )
    assert ok_future
    assert not ok_past
