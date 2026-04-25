"""Tests for services/query/prefetch.py.

Covers:
  - prefetch writes every chip to cache
  - subsequent ask with the same query_id hits cache (near-instant)
  - failures in one chip don't cancel others
  - concurrency cap is respected
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from uuid import uuid4

import pytest

from services.query import strategies as strat_pkg
from services.query.adapters import InMemoryCacheAdapter
from services.query.core import AnswerQueryRequest, QueryHandler
from services.query.prefetch import (
    PrefetchChip,
    QueryPrefetcher,
    prefetch_query_grid,
)
from services.query.tests._helpers import (
    FakeRenderingAdapter,
    FakeStrategy,
    ScriptedClassifier,
    fake_conn_provider,
)


TENANT = uuid4()


@pytest.fixture
def fake_strategies(monkeypatch):
    replacements = {
        cat: FakeStrategy(category=cat)
        for cat in strat_pkg.STRATEGIES.keys()
    }
    monkeypatch.setattr(strat_pkg, "STRATEGIES", replacements, raising=True)
    from services.query import strategies as strategies_mod
    monkeypatch.setattr(strategies_mod, "STRATEGIES", replacements, raising=True)
    yield replacements


async def test_prefetch_warms_cache_for_all_chips(fake_strategies):
    cache = InMemoryCacheAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=cache,
    )
    chips = [
        PrefetchChip(query_id="q1", query_text="show me customers"),
        PrefetchChip(query_id="q2", query_text="why is Acme at risk"),
        PrefetchChip(query_id="q3", query_text="draft a reply to Marcus"),
    ]
    report = await prefetch_query_grid(handler, TENANT, chips)
    assert report.succeeded == 3
    assert report.failed == 0
    for c in chips:
        row = await cache.get(TENANT, f"query_prefetch:{c.query_id}")
        assert row is not None
        assert row["content"]["query_echo"] == c.query_text


async def test_prefetch_then_ask_is_fast(fake_strategies):
    """Non-prefetched responses go through the render latency; prefetch
    hits short-circuit. We don't measure wall time rigorously in CI —
    we verify the render adapter wasn't called twice."""
    renderer = FakeRenderingAdapter(latency_ms=50)
    cache = InMemoryCacheAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=cache,
    )
    chip = PrefetchChip(query_id="qP", query_text="why is Acme at risk?")
    await prefetch_query_grid(handler, TENANT, [chip])
    assert len(renderer.calls) == 1  # one render call during prefetch

    # Simulate the UI tapping the chip; handler.try_serve_from_prefetch
    # is what the API layer calls before running the full pipeline.
    t0 = time.perf_counter()
    cached = await handler.try_serve_from_prefetch(TENANT, "qP")
    latency_ms = (time.perf_counter() - t0) * 1000
    assert cached is not None
    assert len(renderer.calls) == 1  # no extra render call
    # Prefetched latency target: <500ms. In-memory adapter + no render
    # should be comfortably under this.
    assert latency_ms < 500


async def test_prefetch_chip_failure_does_not_cancel_others(fake_strategies):
    """If one chip raises, others still land in cache."""
    renderer = FakeRenderingAdapter()
    cache = InMemoryCacheAdapter()

    # Wrap the handler so the second chip raises.
    real_handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=cache,
    )

    class _FlakyHandler:
        async def answer_query(self, req):
            if req.query == "bad":
                raise RuntimeError("intentional")
            return await real_handler.answer_query(req)

    prefetcher = QueryPrefetcher(_FlakyHandler(), max_concurrency=2)  # type: ignore[arg-type]
    chips = [
        PrefetchChip(query_id="good1", query_text="good"),
        PrefetchChip(query_id="bad1", query_text="bad"),
        PrefetchChip(query_id="good2", query_text="good"),
    ]
    report = await prefetcher.prefetch(TENANT, chips)
    assert report.total == 3
    assert report.succeeded == 2
    assert report.failed == 1
    # good1 + good2 cached; bad1 not.
    assert await cache.get(TENANT, "query_prefetch:good1") is not None
    assert await cache.get(TENANT, "query_prefetch:good2") is not None
    assert await cache.get(TENANT, "query_prefetch:bad1") is None


async def test_prefetch_empty_chip_list_is_noop(fake_strategies):
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
    )
    report = await prefetch_query_grid(handler, TENANT, [])
    assert report.total == 0
    assert report.succeeded == 0
    assert report.failed == 0


async def test_prefetch_concurrency_cap(fake_strategies):
    """With max_concurrency=1, chips run sequentially."""
    renderer = FakeRenderingAdapter(latency_ms=20)
    cache = InMemoryCacheAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=renderer,
        cache_adapter=cache,
    )
    prefetcher = QueryPrefetcher(handler, max_concurrency=1)
    chips = [PrefetchChip(query_id=f"q{i}", query_text="x") for i in range(3)]
    t0 = time.perf_counter()
    await prefetcher.prefetch(TENANT, chips)
    elapsed = (time.perf_counter() - t0) * 1000
    # 3 * 20ms sequentially = ~60ms (with some slack).
    assert elapsed >= 45
