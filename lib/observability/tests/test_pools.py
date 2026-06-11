"""Tests for lib/observability/pools.py — pool gauges + acquire-wait timing.

The pools collector and the acquire-wait histogram both live on the
process-global default registry, so the fixture resets both stores and
all assertions target specific labeled lines (other subsystems may have
registered families in the same process).
"""
from __future__ import annotations

import pytest

from lib.observability import pools
from lib.observability.metrics import render_default, reset_default_for_tests
from lib.observability.pools import (
    observe_acquire_wait,
    register_pool,
    unregister_pool,
)


class FakePool:
    """Duck-typed asyncpg.Pool stand-in (the four stat accessors)."""

    def __init__(self, size: int = 5, idle: int = 2,
                 min_size: int = 1, max_size: int = 10) -> None:
        self._size = size
        self._idle = idle
        self._min = min_size
        self._max = max_size

    def get_size(self) -> int:
        return self._size

    def get_idle_size(self) -> int:
        return self._idle

    def get_min_size(self) -> int:
        return self._min

    def get_max_size(self) -> int:
        return self._max


class BrokenPool:
    """Pool whose stats raise (mid-close) — the collector must skip it."""

    def get_size(self) -> int:
        raise RuntimeError("pool is closing")

    def get_idle_size(self) -> int:  # pragma: no cover - get_size raises first
        raise RuntimeError("pool is closing")

    def get_min_size(self) -> int:  # pragma: no cover
        raise RuntimeError("pool is closing")

    def get_max_size(self) -> int:  # pragma: no cover
        raise RuntimeError("pool is closing")


@pytest.fixture(autouse=True)
def _isolate():
    reset_default_for_tests()
    pools._reset_for_tests()
    yield
    reset_default_for_tests()
    pools._reset_for_tests()


class TestPoolGauges:
    def test_registered_pool_renders_db_pool_gauges(self) -> None:
        register_pool("x", FakePool(size=5, idle=2, min_size=1, max_size=10))
        text = render_default()
        assert 'db_pool_in_use{pool="x"} 3' in text
        assert 'db_pool_size{pool="x"} 5' in text
        assert 'db_pool_idle{pool="x"} 2' in text
        assert 'db_pool_min{pool="x"} 1' in text
        assert 'db_pool_max{pool="x"} 10' in text
        assert "# TYPE db_pool_in_use gauge" in text

    def test_unregistered_pool_disappears(self) -> None:
        register_pool("gone", FakePool())
        assert 'db_pool_size{pool="gone"}' in render_default()
        unregister_pool("gone")
        assert 'db_pool_size{pool="gone"}' not in render_default()

    def test_broken_pool_skipped_healthy_pool_still_rendered(self) -> None:
        register_pool("ok", FakePool(size=4, idle=4))
        register_pool("broken", BrokenPool())
        text = render_default()  # must not raise
        assert 'db_pool_in_use{pool="ok"} 0' in text
        assert 'pool="broken"' not in text


class TestAcquireWait:
    def test_observe_acquire_wait_feeds_histogram(self) -> None:
        observe_acquire_wait("x", 0.002)
        text = render_default()
        assert 'db_pool_acquire_wait_seconds_count{pool="x"} 1' in text
        # 0.002 lands in the 0.005 cumulative bucket
        assert 'db_pool_acquire_wait_seconds_bucket{pool="x",le="0.005"} 1' in text
        assert 'db_pool_acquire_wait_seconds_bucket{pool="x",le="+Inf"} 1' in text
