"""
lib/shared/trust.py — the seven trust tiers per spec §1.

Ordering (most reliable to least):

  1. authoritative             — internal systems-of-record (CRM, Git)
  2. attested_agent            — Nexus-attested AI agent
  3. authoritative_external    — SEC, FDA, regulatory APIs
  4. reputable                 — established news / analysts
  5. inferential               — internal conversational, user-created
  6. inferential_external      — aggregated external reporting
  7. unvetted                  — social media, forums

Comparison helpers preserve this semantic ordering: `>=` means
"at least as trustworthy as". Trust-tier enforcement points (e.g.
`Commitment.doneverified` requires `authoritative` resolved_by)
call `tier.is_at_least(required)`.
"""
from __future__ import annotations

from enum import Enum


class TrustTier(str, Enum):
    """
    String-valued enum so values round-trip through JSON / asyncpg
    without coercion. Ordering is explicitly defined by rank (lower
    rank = more trustworthy) and overrides `str`'s lexicographic
    comparisons so that sorted() and comparison operators reflect
    trust semantics.
    """

    authoritative = "authoritative"
    attested_agent = "attested_agent"
    authoritative_external = "authoritative_external"
    reputable = "reputable"
    inferential = "inferential"
    inferential_external = "inferential_external"
    unvetted = "unvetted"

    @property
    def rank(self) -> int:
        """Lower rank = more trustworthy."""
        return _RANK[self]

    def is_at_least(self, other: "TrustTier | str") -> bool:
        """
        Return True if `self` is at least as trustworthy as `other`.
        Accepts a TrustTier or the string form.
        """
        other_tier = TrustTier(other) if isinstance(other, str) else other
        return self.rank <= other_tier.rank

    # Ordering semantics: `a > b` == "a is strictly more trustworthy
    # than b". Lower rank == more trustworthy, so `a > b` iff
    # `a.rank < b.rank`.
    #
    # We override all six comparisons explicitly because `str` already
    # provides __lt__/__le__/__gt__/__ge__ and @total_ordering would
    # leave those in place. Equality stays identity-based (Enum default).

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TrustTier):
            return NotImplemented
        return self.rank > other.rank

    def __le__(self, other: object) -> bool:
        if not isinstance(other, TrustTier):
            return NotImplemented
        return self.rank >= other.rank

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, TrustTier):
            return NotImplemented
        return self.rank < other.rank

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, TrustTier):
            return NotImplemented
        return self.rank <= other.rank


# Canonical order, populated once at import time.
_ORDER: tuple[TrustTier, ...] = (
    TrustTier.authoritative,
    TrustTier.attested_agent,
    TrustTier.authoritative_external,
    TrustTier.reputable,
    TrustTier.inferential,
    TrustTier.inferential_external,
    TrustTier.unvetted,
)

_RANK: dict[TrustTier, int] = {tier: i for i, tier in enumerate(_ORDER)}


def max_tier(*tiers: TrustTier | str) -> TrustTier:
    """Return the most-trustworthy tier in the argument list."""
    if not tiers:
        raise ValueError("max_tier requires at least one argument")
    coerced = [TrustTier(t) if isinstance(t, str) else t for t in tiers]
    return min(coerced, key=lambda t: t.rank)


def min_tier(*tiers: TrustTier | str) -> TrustTier:
    """Return the least-trustworthy tier in the argument list."""
    if not tiers:
        raise ValueError("min_tier requires at least one argument")
    coerced = [TrustTier(t) if isinstance(t, str) else t for t in tiers]
    return max(coerced, key=lambda t: t.rank)


def ordered() -> tuple[TrustTier, ...]:
    """Return the canonical ordering (authoritative first)."""
    return _ORDER


__all__ = ["TrustTier", "max_tier", "min_tier", "ordered"]
