"""Unit tests for services.calibration.classify_card.

Verifies the claim-class derivation rules and the unclassifiable
fallthrough to None.
"""
from __future__ import annotations

import pytest

from services.calibration import classify_card


def test_classify_renewal_risk_from_revenue_at_risk_string():
    focus = {"resource": {"kind": "customer", "revenue_at_risk": "$487K"}}
    assert classify_card(focus) == "renewal_risk"


def test_classify_renewal_risk_from_revenue_at_risk_usd():
    focus = {"resource": {"kind": "customer", "revenue_at_risk_usd": 100_000}}
    assert classify_card(focus) == "renewal_risk"


def test_classify_expansion_likelihood_healthy_customer():
    focus = {
        "resource": {
            "kind": "customer",
            "health": "healthy",
            "utilization_state": "committed",
        }
    }
    assert classify_card(focus) == "expansion_likelihood"


def test_classify_expansion_likelihood_relational_customer():
    focus = {
        "resource": {
            "kind": "relational",
            "health": "healthy",
            "utilization_state": "available",
        }
    }
    assert classify_card(focus) == "expansion_likelihood"


def test_classify_expansion_skipped_when_revenue_at_risk_present():
    # Renewal risk wins over expansion when both apply.
    focus = {
        "resource": {
            "kind": "customer",
            "health": "healthy",
            "revenue_at_risk": "$1M",
        }
    }
    assert classify_card(focus) == "renewal_risk"


def test_classify_delivery_estimate_from_due_at():
    focus = {"commitment": {"due_at": "2026-06-01T00:00:00+00:00"}}
    assert classify_card(focus) == "delivery_estimate"


def test_classify_delivery_estimate_from_days_to_due():
    focus = {"commitment": {"days_to_due": 7}}
    assert classify_card(focus) == "delivery_estimate"


def test_classify_belief_movement():
    focus = {"model": {"confidence": 0.62, "prior_confidence": 0.85}}
    assert classify_card(focus) == "belief_movement"


def test_classify_belief_no_movement_returns_none():
    # No drift = no belief_movement.
    focus = {"model": {"confidence": 0.7, "prior_confidence": 0.7}}
    assert classify_card(focus) is None


def test_classify_returns_none_for_empty_focus():
    assert classify_card({}) is None
    assert classify_card(None) is None
    assert classify_card("not-a-dict") is None


def test_classify_prefers_renewal_over_delivery_when_both_apply():
    # Order matters: renewal_risk fires first.
    focus = {
        "resource": {"kind": "customer", "revenue_at_risk": "$300K"},
        "commitment": {"days_to_due": 14},
    }
    assert classify_card(focus) == "renewal_risk"
