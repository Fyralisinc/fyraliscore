"""IN-13 unit tests for `services/integrations/github/replay_cache.py`.

Covers tasks T080–T083 from specs/IN-13-github-integration/tasks.md.
No DB required.
"""
from __future__ import annotations

import pytest

from services.integrations.github.replay_cache import (
    ReplayCache,
    make_replay_cache,
)


def test_first_call_misses_second_hits() -> None:
    """T080: first seen() returns False (miss + insert); second within
    TTL returns True (hit)."""
    cache = ReplayCache(max_entries=8, ttl_seconds=10.0)
    assert cache.seen("inst-1", "delivery-A", now=0.0) is False
    assert cache.seen("inst-1", "delivery-A", now=1.0) is True


def test_ttl_expiry_releases_key() -> None:
    """T081: after TTL elapses, the same key is treated as a fresh miss."""
    cache = ReplayCache(max_entries=8, ttl_seconds=5.0)
    assert cache.seen("i", "d", now=0.0) is False
    # Still within TTL.
    assert cache.seen("i", "d", now=4.9) is True
    # Past TTL.
    assert cache.seen("i", "d", now=6.0) is False


def test_lru_eviction_over_capacity() -> None:
    """T082: when over capacity, oldest entries are evicted (LRU)."""
    cache = ReplayCache(max_entries=2, ttl_seconds=100.0)
    cache.seen("i", "a", now=1.0)
    cache.seen("i", "b", now=2.0)
    cache.seen("i", "c", now=3.0)  # evicts "a"
    assert cache.size == 2
    # "a" is now a miss again.
    assert cache.seen("i", "a", now=4.0) is False


def test_missing_delivery_id_bypasses_cache() -> None:
    """T083: missing delivery_id → bypass (returns False without inserting),
    increments bypass_count."""
    cache = ReplayCache()
    assert cache.seen("i", None, now=0.0) is False
    assert cache.seen(None, "d", now=0.0) is False
    assert cache.seen("", "d", now=0.0) is False
    assert cache.bypass_count == 3


def test_make_replay_cache_factory_returns_instance() -> None:
    cache = make_replay_cache(max_entries=16, ttl_seconds=30.0)
    assert isinstance(cache, ReplayCache)
    assert cache.size == 0


def test_invalid_init_args_raise() -> None:
    with pytest.raises(ValueError):
        ReplayCache(max_entries=0)
    with pytest.raises(ValueError):
        ReplayCache(ttl_seconds=0)
