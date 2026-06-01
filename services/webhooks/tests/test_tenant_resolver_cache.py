"""Unit tests for the in-process TTL LRU cache in
services/webhooks/tenant_resolver.py.

Covers FR-009 (cache structure), FR-010 (invalidation), FR-011 (cache
behavior under stress — the integration variant in
test_tenant_resolver_lookup.py covers the cache-backend-unavailable
fallback in the resolver itself).

Pure unit tests with an injected clock. No DB.
"""
from __future__ import annotations

import pytest

from services.webhooks.tenant_resolver import (
    CacheHit,
    CacheNegative,
    InstallationCache,
)


def _hit() -> CacheHit:
    """Build a value-irrelevant CacheHit fixture."""
    from uuid import UUID

    return CacheHit(
        tenant_id=UUID("11111111-1111-7111-8111-111111111111"),
        installation_row_id=UUID("22222222-2222-7222-8222-222222222222"),
        secret_ref="secret-ref-fixture",
    )


# ---------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------

def test_get_returns_put_value() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    value = _hit()
    cache.put(("slack", "T123"), value, now=0.0)
    assert cache.get(("slack", "T123"), now=0.0) is value


def test_get_returns_none_on_missing_key() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    assert cache.get(("slack", "T_UNKNOWN"), now=0.0) is None


# ---------------------------------------------------------------------
# TTL expiry
# ---------------------------------------------------------------------

def test_cache_expires_after_ttl() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=5.0)
    cache.put(("slack", "T123"), _hit(), now=0.0)
    # Just before expiry, still present.
    assert cache.get(("slack", "T123"), now=4.99) is not None
    # At expiry boundary (expires_at == now), counts as expired.
    assert cache.get(("slack", "T123"), now=5.0) is None


def test_cache_purges_expired_entry_on_read() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=1.0)
    cache.put(("slack", "T123"), _hit(), now=0.0)
    cache.get(("slack", "T123"), now=2.0)  # triggers eviction
    assert cache.size() == 0


# ---------------------------------------------------------------------
# LRU eviction
# ---------------------------------------------------------------------

def test_evicts_lru_when_full() -> None:
    cache = InstallationCache(max_entries=3, ttl_seconds=10.0)
    cache.put(("slack", "a"), _hit(), now=0.0)
    cache.put(("slack", "b"), _hit(), now=0.0)
    cache.put(("slack", "c"), _hit(), now=0.0)
    # Touch 'a' to make 'b' the LRU.
    cache.get(("slack", "a"), now=0.0)
    # Insert a 4th — should evict 'b'.
    cache.put(("slack", "d"), _hit(), now=0.0)
    assert cache.get(("slack", "a"), now=0.0) is not None
    assert cache.get(("slack", "b"), now=0.0) is None
    assert cache.get(("slack", "c"), now=0.0) is not None
    assert cache.get(("slack", "d"), now=0.0) is not None
    assert cache.size() == 3


def test_put_updates_existing_key_without_eviction() -> None:
    cache = InstallationCache(max_entries=2, ttl_seconds=10.0)
    cache.put(("slack", "a"), _hit(), now=0.0)
    cache.put(("slack", "b"), _hit(), now=0.0)
    # Re-put 'a' with a fresh expiry; size stays at 2.
    cache.put(("slack", "a"), _hit(), now=5.0)
    assert cache.size() == 2
    assert cache.get(("slack", "a"), now=5.0) is not None


# ---------------------------------------------------------------------
# Invalidation
# ---------------------------------------------------------------------

def test_invalidate_removes_entry() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    cache.put(("slack", "a"), _hit(), now=0.0)
    cache.invalidate(("slack", "a"))
    assert cache.get(("slack", "a"), now=0.0) is None


def test_invalidate_missing_key_is_noop() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    cache.invalidate(("slack", "ghost"))  # should not raise


# ---------------------------------------------------------------------
# Negative caching
# ---------------------------------------------------------------------

def test_negative_entry_round_trips() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    cache.put(("slack", "unknown"), CacheNegative(), now=0.0)
    cached = cache.get(("slack", "unknown"), now=0.0)
    assert isinstance(cached, CacheNegative)


def test_invalidate_clears_negative_entry() -> None:
    cache = InstallationCache(max_entries=8, ttl_seconds=10.0)
    cache.put(("slack", "unknown"), CacheNegative(), now=0.0)
    cache.invalidate(("slack", "unknown"))
    assert cache.get(("slack", "unknown"), now=0.0) is None


# ---------------------------------------------------------------------
# Constructor sanity
# ---------------------------------------------------------------------

@pytest.mark.parametrize("max_entries", [0, -1])
def test_rejects_non_positive_max_entries(max_entries: int) -> None:
    with pytest.raises(ValueError):
        InstallationCache(max_entries=max_entries, ttl_seconds=1.0)


@pytest.mark.parametrize("ttl_seconds", [0.0, -1.0])
def test_rejects_non_positive_ttl(ttl_seconds: float) -> None:
    with pytest.raises(ValueError):
        InstallationCache(max_entries=4, ttl_seconds=ttl_seconds)
