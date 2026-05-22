"""Unit tests for services.today.stake.

Covers `parse_revenue_at_risk` (every suffix, commas, bare ints, junk)
and `derive_stake` (rule order, fallbacks, malformed input).
"""
from __future__ import annotations

import pytest

from services.today.stake import derive_stake, parse_revenue_at_risk


# ---------------------------------------------------------------------
# parse_revenue_at_risk — happy paths
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Common rendering shapes (the rendering contract pre-formats
        # these with `$` + suffix).
        ("$487K", 487_000),
        ("$1.2M", 1_200_000),
        ("$2B", 2_000_000_000),
        ("$500,000", 500_000),
        # Lowercase suffixes
        ("$10k", 10_000),
        ("$3.5m", 3_500_000),
        ("$1b", 1_000_000_000),
        # No suffix, no dollar
        ("1500", 1_500),
        ("$1,234.50", 1_234),
        ("$0", 0),
        # Decimals with K
        ("$1.5K", 1_500),
        ("0.5M", 500_000),
        # Whitespace tolerance
        ("  $750K  ", 750_000),
        # Numeric inputs pass through
        (1500, 1_500),
        (1500.7, 1_500),
        (0, 0),
    ],
)
def test_parse_revenue_at_risk_happy(raw, expected):
    assert parse_revenue_at_risk(raw) == expected


# ---------------------------------------------------------------------
# parse_revenue_at_risk — None / failures
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "abc",
        "$",
        "$K",
        "$1.2.3M",          # malformed decimal
        "$-100K",           # negative
        "100Q",             # unknown suffix
        "100 M extra",      # trailing junk
        "100,00,000",       # malformed grouping (would still match? no — strict comma grouping)
        -100,               # negative numeric
        [],                 # wrong type
        {"x": 1},
        object(),
    ],
)
def test_parse_revenue_at_risk_returns_none(raw):
    assert parse_revenue_at_risk(raw) is None


def test_parse_revenue_at_risk_huge_int_does_not_overflow():
    # int(float * 1B) on extreme values would round; just confirm no
    # crash on a credibly-large number.
    assert parse_revenue_at_risk("999B") == 999_000_000_000


# ---------------------------------------------------------------------
# derive_stake — Rule 1: resource revenue_at_risk
# ---------------------------------------------------------------------


def test_derive_stake_customer_resource_usd():
    focus = {"resource": {"kind": "customer", "revenue_at_risk": "$487K"}}
    assert derive_stake(focus) == {"unit": "usd", "value": 487_000}


def test_derive_stake_relational_resource_usd():
    # GRT snapshot uses kind=='relational' for customer rows.
    focus = {"resource": {"kind": "relational", "revenue_at_risk": "$1.2M"}}
    assert derive_stake(focus) == {"unit": "usd", "value": 1_200_000}


def test_derive_stake_resource_falls_through_on_bad_string():
    # Resource present but rev string doesn't parse: fall through to
    # next rule (or None if no other rule fires).
    focus = {"resource": {"kind": "customer", "revenue_at_risk": "garbage"}}
    assert derive_stake(focus) is None


def test_derive_stake_resource_numeric_revenue_at_risk_usd():
    # The snapshot-side field is numeric, not pre-formatted.
    focus = {"resource": {"kind": "customer", "revenue_at_risk_usd": 750_000}}
    assert derive_stake(focus) == {"unit": "usd", "value": 750_000}


def test_derive_stake_resource_zero_revenue_at_risk():
    # Zero is a valid stake value — keep it (not None).
    focus = {"resource": {"kind": "customer", "revenue_at_risk": "$0"}}
    assert derive_stake(focus) == {"unit": "usd", "value": 0}


# ---------------------------------------------------------------------
# derive_stake — Rule 2: commitment pressure
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "pressure,value",
    [
        ("low", 1),
        ("medium", 2),
        ("med", 2),
        ("high", 3),
        ("HIGH", 3),     # case-insensitive
        ("  Low  ", 1),  # whitespace
    ],
)
def test_derive_stake_commitment_pressure(pressure, value):
    focus = {"commitment": {"pressure": pressure}}
    assert derive_stake(focus) == {"unit": "risk", "value": value}


def test_derive_stake_commitment_pressure_missing():
    focus = {"commitment": {"pressure": None}}
    assert derive_stake(focus) is None


def test_derive_stake_commitment_pressure_unknown():
    focus = {"commitment": {"pressure": "extreme"}}
    assert derive_stake(focus) is None


# ---------------------------------------------------------------------
# derive_stake — rule order
# ---------------------------------------------------------------------


def test_derive_stake_resource_beats_commitment_pressure():
    focus = {
        "resource": {"kind": "customer", "revenue_at_risk": "$200K"},
        "commitment": {"pressure": "high"},
    }
    assert derive_stake(focus) == {"unit": "usd", "value": 200_000}


def test_derive_stake_fallback_to_commitment_when_resource_unparseable():
    focus = {
        "resource": {"kind": "customer", "revenue_at_risk": "not-a-number"},
        "commitment": {"pressure": "medium"},
    }
    assert derive_stake(focus) == {"unit": "risk", "value": 2}


# ---------------------------------------------------------------------
# derive_stake — empty / malformed
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "focus",
    [
        None,
        {},
        {"resource": None},
        {"commitment": None},
        {"resource": "not-a-dict"},
        {"commitment": "not-a-dict"},
        {"model": {"confidence": 0.8}},     # no resource or commitment
        "totally wrong type",
        42,
    ],
)
def test_derive_stake_returns_none_for_unusable_input(focus):
    assert derive_stake(focus) is None
