"""Pure-function property tests for the missing-transition detector
helpers.

Kept separate from `test_detectors.py` so this file runs without a live
Postgres — the integration tests there carry a module-level `integration`
mark that gates them on `DATABASE_URL`. These property tests are the
"stepped-up" bar: hypothesis-driven invariants over the detector's
math, exercised at 200+ examples per property.
"""
from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from services.reasoning.dynamics.detectors import (
    _VOLATILE_AUDIT_FIELDS,
    _coerce_state_jsonb,
    _material_diff,
    _missing_transition_strength,
)


# ---------------------------------------------------------------------
# _material_diff invariants
# ---------------------------------------------------------------------


@pytest.mark.parametrize("volatile_field", sorted(_VOLATILE_AUDIT_FIELDS))
def test_material_diff_ignores_volatile_field(volatile_field: str) -> None:
    """Volatile fields must NEVER be reported as differing — they churn
    under normal reconsolidation activity and would cause false
    positives."""
    prev = {volatile_field: "old", "status": "active"}
    nxt = {volatile_field: "new", "status": "active"}
    assert _material_diff(prev, nxt) == ()


_key_strategy = st.text(min_size=1, max_size=20).filter(
    lambda s: s not in _VOLATILE_AUDIT_FIELDS
)

_value_strategy = st.one_of(
    st.text(max_size=20),
    st.integers(min_value=-(10 ** 6), max_value=10 ** 6),
    st.booleans(),
    st.none(),
)


@settings(
    max_examples=200,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    a=st.dictionaries(_key_strategy, _value_strategy, max_size=8),
    b=st.dictionaries(_key_strategy, _value_strategy, max_size=8),
)
def test_material_diff_symmetric_on_key_set(a: dict, b: dict) -> None:
    """The SET of differing keys is order-invariant: diff(a,b) and
    diff(b,a) must agree on which keys changed (the per-key value
    ordering may differ but the membership is symmetric).

    This is load-bearing for the imputer's claim that "the discontinuity
    itself is symmetric" — the user-visible signal must not depend on
    which audit event happens to be older."""
    da = set(_material_diff(a, b))
    db = set(_material_diff(b, a))
    assert da == db


@settings(max_examples=200)
@given(
    a=st.dictionaries(_key_strategy, _value_strategy, max_size=8),
)
def test_material_diff_self_is_empty(a: dict) -> None:
    """A snapshot compared to itself has no diff — the trivial invariant
    everyone forgets to test."""
    assert _material_diff(a, a) == ()


@settings(max_examples=200)
@given(
    a=st.dictionaries(_key_strategy, _value_strategy, min_size=1, max_size=4),
    extra_key=_key_strategy,
    extra_value=_value_strategy,
)
def test_material_diff_grows_with_keys(
    a: dict, extra_key: str, extra_value
) -> None:
    """Adding a non-volatile key to b that isn't in a (and isn't equal
    to a's default `None`) must register as a diff."""
    if extra_key in a:
        return  # property doesn't apply when extra_key overwrites
    b = {**a, extra_key: extra_value}
    diff = _material_diff(a, b)
    if extra_value is None and a.get(extra_key) is None:
        # Both treat the field as missing/None; not a real diff.
        assert extra_key not in diff
    else:
        assert extra_key in diff


# ---------------------------------------------------------------------
# _missing_transition_strength invariants
# ---------------------------------------------------------------------


@settings(max_examples=200)
@given(
    n_fields=st.integers(min_value=1, max_value=20),
    gap_seconds=st.floats(
        min_value=0.0, max_value=60.0 * 60.0 * 24.0 * 365.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_strength_in_unit_interval(n_fields: int, gap_seconds: float) -> None:
    """Strength is a [0, 1]-valued scalar regardless of input.

    Downstream consumers (DynamicSignal.to_dict, ReasoningFrame ranking)
    rely on this — an out-of-bounds strength would corrupt rank ordering
    in the action list."""
    differing = tuple(f"f{i}" for i in range(n_fields))
    s = _missing_transition_strength(differing, gap_seconds)
    assert 0.0 <= s <= 1.0


@settings(max_examples=200)
@given(
    a_fields=st.integers(min_value=1, max_value=5),
    extra=st.integers(min_value=0, max_value=15),
    gap_seconds=st.floats(
        min_value=0.0, max_value=60.0 * 60.0 * 24.0 * 7.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_strength_monotonic_in_field_count(
    a_fields: int, extra: int, gap_seconds: float,
) -> None:
    """Wider divergence (more differing fields) must not weaken the
    signal. This is the calibration ordering invariant for the field-
    count axis."""
    a = tuple(f"f{i}" for i in range(a_fields))
    b = tuple(f"f{i}" for i in range(a_fields + extra))
    sa = _missing_transition_strength(a, gap_seconds)
    sb = _missing_transition_strength(b, gap_seconds)
    assert sb >= sa


@settings(max_examples=200)
@given(
    n_fields=st.integers(min_value=1, max_value=8),
    small_gap=st.floats(
        min_value=0.0, max_value=60.0 * 60.0,
        allow_nan=False, allow_infinity=False,
    ),
    extra_gap=st.floats(
        min_value=60.0 * 60.0 * 24.0 * 14.0,
        max_value=60.0 * 60.0 * 24.0 * 30.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_strength_decays_with_gap(
    n_fields: int, small_gap: float, extra_gap: float,
) -> None:
    """A larger gap must not produce a stronger signal than a smaller
    gap (with the same diff). This is the calibration ordering invariant
    for the temporal axis."""
    differing = tuple(f"f{i}" for i in range(n_fields))
    tight = _missing_transition_strength(differing, small_gap)
    huge = _missing_transition_strength(differing, small_gap + extra_gap)
    assert tight >= huge


def test_strength_zero_fields_returns_floor() -> None:
    """Pathological input: no differing fields. Even if the call site
    forgot to pre-filter, the function shouldn't crash."""
    s = _missing_transition_strength((), 0.0)
    assert 0.0 <= s <= 1.0


def test_strength_negative_gap_treated_as_zero() -> None:
    """If the audit chain ordering somehow inverted, a negative gap
    shouldn't blow up the math — the floor kicks in."""
    s_neg = _missing_transition_strength(("status",), -3600.0)
    s_zero = _missing_transition_strength(("status",), 0.0)
    # Negative gaps are clamped to 0 inside the strength function so the
    # output should match.
    assert s_neg == s_zero


# ---------------------------------------------------------------------
# _coerce_state_jsonb invariants
# ---------------------------------------------------------------------


@settings(max_examples=200)
@given(
    val=st.one_of(
        st.none(),
        st.text(max_size=200),
        st.integers(),
        st.booleans(),
        st.dictionaries(st.text(min_size=1, max_size=8), st.integers(), max_size=5),
        st.lists(st.integers(), max_size=5),
        st.binary(max_size=200),
    ),
)
def test_coerce_state_jsonb_always_returns_dict(val) -> None:
    """JSONB coercion must never raise — the substrate may feed us
    bytes, str, dict, or even pathological values, and downstream
    dict.get() calls must remain safe."""
    out = _coerce_state_jsonb(val)
    assert isinstance(out, dict)


def test_coerce_state_jsonb_passthrough_dict() -> None:
    assert _coerce_state_jsonb({"a": 1}) == {"a": 1}


def test_coerce_state_jsonb_decodes_json_string() -> None:
    assert _coerce_state_jsonb('{"a": 1}') == {"a": 1}


def test_coerce_state_jsonb_decodes_jsonb_bytes() -> None:
    assert _coerce_state_jsonb(b'{"a": 1}') == {"a": 1}


def test_coerce_state_jsonb_handles_malformed_json() -> None:
    assert _coerce_state_jsonb('{not-json') == {}


def test_coerce_state_jsonb_rejects_non_object_root() -> None:
    """JSON arrays / scalars at the root are not valid Model state and
    must coerce to an empty dict rather than leaking through."""
    assert _coerce_state_jsonb('[1, 2, 3]') == {}
    assert _coerce_state_jsonb('"plain string"') == {}
    assert _coerce_state_jsonb('42') == {}
