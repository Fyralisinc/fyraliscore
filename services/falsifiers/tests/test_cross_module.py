"""
services/falsifiers/tests/test_cross_module.py — Wave 4-C re-export regression.

Verifies that every downstream consumer (Wave 4-A deadline resolver,
Wave 4-B anomaly processor, Wave 3-B Think validator, Wave 4-C
contestability + precipitation worker) can import the adequacy check
from `services.falsifiers` and gets byte-identical behaviour to the
authoritative `services.models.falsifier` implementation.

Wave 1-C's tests in services/models/tests/test_falsifier.py cover the
rule semantics in depth. This file intentionally does NOT duplicate
those — instead it exercises the cross-module surface.

Five per-kind adequate / inadequate cases (one per falsifier kind),
plus two identity checks.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import services.models.falsifier as authoritative
import services.falsifiers as reexport


def test_reexport_points_at_authoritative_implementation():
    """The re-exported function is the SAME function object — not a
    wrapper, not a copy — so rule changes in the authoritative module
    propagate instantly."""
    assert reexport.is_adequate is authoritative.is_adequate_falsifier
    assert reexport.is_adequate_async is authoritative.is_adequate_falsifier_async
    assert reexport.LEGAL_FALSIFIER_KINDS is authoritative.LEGAL_FALSIFIER_KINDS


def test_observation_pattern_adequate_via_reexport():
    f = {
        "kind": "observation_pattern",
        "pattern": "any Slack message from Alice containing 'ship'",
        "within_window": "P7D",
    }
    ok, reason = reexport.is_adequate(f)
    assert ok is True
    assert reason is None


def test_observation_pattern_inadequate_via_reexport():
    # Too-short pattern (< 20 chars per spec §10).
    f = {"kind": "observation_pattern", "pattern": "too short",
         "within_window": "P1D"}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason == "pattern too vague"


def test_commitment_outcome_adequate_via_reexport():
    f = {
        "kind": "commitment_outcome",
        "commitment_ref": "c-187",
        "contradicting_state": ["Closed"],
    }
    ok, reason = reexport.is_adequate(f)
    assert ok is True
    assert reason is None


def test_commitment_outcome_inadequate_via_reexport():
    # Missing contradicting_state.
    f = {"kind": "commitment_outcome", "commitment_ref": "c-187"}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason == "no contradicting state"


def test_prediction_deadline_adequate_via_reexport():
    future = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    f = {
        "kind": "prediction_deadline",
        "evaluate_at": future,
        "check": "commitment c-187 in state DoneVerified",
    }
    ok, reason = reexport.is_adequate(f)
    assert ok is True
    assert reason is None


def test_prediction_deadline_inadequate_in_past():
    past = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    f = {"kind": "prediction_deadline", "evaluate_at": past, "check": "x"}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason == "evaluate_at in past"


def test_resource_threshold_adequate_via_reexport():
    f = {
        "kind": "resource_threshold",
        "resource_ref": "r-eng-capacity",
        "threshold": "available_capacity < 0.20",
    }
    ok, reason = reexport.is_adequate(f)
    assert ok is True
    assert reason is None


def test_resource_threshold_inadequate_no_threshold():
    f = {"kind": "resource_threshold", "resource_ref": "r-eng"}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason == "no threshold"


def test_explicit_contestation_adequate_via_reexport():
    f = {"kind": "explicit_contestation", "contesting_actors": ["alice"]}
    ok, reason = reexport.is_adequate(f)
    assert ok is True
    assert reason is None


def test_explicit_contestation_inadequate_empty_list():
    f = {"kind": "explicit_contestation", "contesting_actors": []}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason == "no contesting actors"


def test_unknown_kind_rejected_via_reexport():
    f = {"kind": "unknown_kind", "pattern": "doesn't matter"}
    ok, reason = reexport.is_adequate(f)
    assert ok is False
    assert reason is not None and "unknown falsifier kind" in reason


def test_none_falsifier_rejected():
    ok, reason = reexport.is_adequate(None)
    assert ok is False
    assert reason == "no falsifier specified"


def test_legal_kinds_match_spec():
    assert reexport.LEGAL_FALSIFIER_KINDS == frozenset((
        "observation_pattern",
        "commitment_outcome",
        "prediction_deadline",
        "resource_threshold",
        "explicit_contestation",
    ))
