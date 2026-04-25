"""Tests for lib/shared/trust.py — seven trust tiers + comparisons."""
from __future__ import annotations

import json

import pytest
from hypothesis import given, strategies as st

from lib.shared.trust import TrustTier, max_tier, min_tier, ordered


EXPECTED_ORDER = [
    "authoritative",
    "attested_agent",
    "authoritative_external",
    "reputable",
    "inferential",
    "inferential_external",
    "unvetted",
]


def test_seven_tiers_present():
    values = [t.value for t in TrustTier]
    assert sorted(values) == sorted(EXPECTED_ORDER)
    assert len(values) == 7


def test_ordered_matches_spec():
    assert [t.value for t in ordered()] == EXPECTED_ORDER


def test_rank_monotonic():
    ranks = [t.rank for t in ordered()]
    assert ranks == list(range(7))


def test_string_value_roundtrip():
    for t in TrustTier:
        assert TrustTier(t.value) is t


def test_invalid_string_raises():
    with pytest.raises(ValueError):
        TrustTier("bogus_tier")


@pytest.mark.parametrize(
    "more,less",
    [
        (TrustTier.authoritative, TrustTier.attested_agent),
        (TrustTier.authoritative, TrustTier.unvetted),
        (TrustTier.attested_agent, TrustTier.reputable),
        (TrustTier.reputable, TrustTier.inferential_external),
        (TrustTier.inferential, TrustTier.unvetted),
    ],
)
def test_strict_greater_more_trustworthy(more: TrustTier, less: TrustTier):
    assert more > less
    assert not (less > more)
    assert more >= less
    assert less < more


def test_equal_is_not_greater():
    assert not (TrustTier.reputable > TrustTier.reputable)
    assert TrustTier.reputable >= TrustTier.reputable
    assert TrustTier.reputable <= TrustTier.reputable


def test_is_at_least_accepts_string():
    assert TrustTier.authoritative.is_at_least("reputable")
    assert not TrustTier.unvetted.is_at_least("reputable")


def test_is_at_least_equal():
    assert TrustTier.reputable.is_at_least(TrustTier.reputable)


def test_is_at_least_transitivity():
    for a in TrustTier:
        for b in TrustTier:
            for c in TrustTier:
                if a.is_at_least(b) and b.is_at_least(c):
                    assert a.is_at_least(c)


def test_max_tier_picks_most_trustworthy():
    assert max_tier(TrustTier.reputable, TrustTier.authoritative, TrustTier.unvetted) \
        is TrustTier.authoritative


def test_max_tier_accepts_strings():
    assert max_tier("reputable", "authoritative", "unvetted") is TrustTier.authoritative


def test_max_tier_rejects_empty():
    with pytest.raises(ValueError):
        max_tier()


def test_min_tier_picks_least_trustworthy():
    assert min_tier(TrustTier.reputable, TrustTier.authoritative, TrustTier.unvetted) \
        is TrustTier.unvetted


def test_min_tier_rejects_empty():
    with pytest.raises(ValueError):
        min_tier()


def test_json_roundtrip():
    for t in TrustTier:
        s = json.dumps(t.value)
        assert TrustTier(json.loads(s)) is t


def test_is_at_least_rejects_unknown_string():
    """An arbitrary string passed to is_at_least fails fast (ValueError)."""
    with pytest.raises(ValueError):
        TrustTier.authoritative.is_at_least("not_a_tier")


def test_sorting_by_trust_rank():
    shuffled = [TrustTier.unvetted, TrustTier.authoritative, TrustTier.reputable]
    # Python's sort with our __lt__ puts less-trustworthy first.
    # Sorting ascending -> least-trustworthy first.
    s = sorted(shuffled)
    assert s == [TrustTier.unvetted, TrustTier.reputable, TrustTier.authoritative]
    # Reversed -> most-trustworthy first.
    s = sorted(shuffled, reverse=True)
    assert s == [TrustTier.authoritative, TrustTier.reputable, TrustTier.unvetted]


@given(st.sampled_from(list(TrustTier)), st.sampled_from(list(TrustTier)))
def test_trichotomy_property(a: TrustTier, b: TrustTier):
    """Exactly one of a<b, a==b, a>b must hold."""
    lt = a < b
    eq = a == b
    gt = a > b
    assert [lt, eq, gt].count(True) == 1


def test_all_tiers_iterable():
    count = 0
    for t in TrustTier:
        assert isinstance(t.value, str)
        count += 1
    assert count == 7
