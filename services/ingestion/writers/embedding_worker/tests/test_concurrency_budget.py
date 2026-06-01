"""Source-isolation Phase 7: the embedding worker honours its per-source
Ollama concurrency budget (EMBEDDING_MAX_CONCURRENCY / config.max_concurrency).

Mocks the Kafka/health boundary and patches `embed_and_update` with a tracker
that records the maximum number of concurrent in-flight embeds, so the test
runs in the unit lane (no broker, no DB, no Ollama).
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import orjson
import pytest

from services.ingestion.embedding.models import EmbeddingEnvelope
from services.ingestion.writers.embedding_worker import embedding_worker as ew


def _msg(tenant_id: Any) -> MagicMock:
    env = EmbeddingEnvelope(
        tenant_id=tenant_id,
        source="slack",
        observation_id=uuid4(),
        enqueued_at=dt.datetime(2026, 5, 17, tzinfo=dt.timezone.utc),
    )
    m = MagicMock()
    m.value = orjson.dumps(env.model_dump(mode="json"))
    m.topic, m.partition, m.offset = "ingestion.embedding.slack", 0, 0
    return m


async def _run_with_budget(monkeypatch: pytest.MonkeyPatch, *, budget: int, n: int) -> int:
    """Drive run_embedding_worker over one batch of `n` messages with
    max_concurrency=`budget`; return the observed max concurrent embeds.
    """
    inflight = 0
    max_inflight = 0

    async def _tracking_embed(*, env, pool, embedder, dlq_producer) -> str:  # noqa: ANN001
        nonlocal inflight, max_inflight
        inflight += 1
        max_inflight = max(max_inflight, inflight)
        try:
            await asyncio.sleep(0.02)  # hold the slot so overlap is observable
            return "embedded"
        finally:
            inflight -= 1

    monkeypatch.setattr(ew, "embed_and_update", _tracking_embed)

    # Kafka consumer: one batch of n messages, then the stop_after break.
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.subscribe = MagicMock()
    consumer.getmany = AsyncMock(return_value={("tp", 0): [_msg(uuid4()) for _ in range(n)]})
    monkeypatch.setattr(ew, "AIOKafkaConsumer", lambda *a, **k: consumer)

    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    monkeypatch.setattr(ew, "IdempotentProducer", lambda *a, **k: producer)

    monkeypatch.setattr(ew, "start_health_server", lambda **k: None)

    async def _noop_ticker(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(ew, "run_heartbeat_ticker", _noop_ticker)

    cfg = ew.EmbeddingWorkerConfig(
        source="slack", max_concurrency=budget, stop_after=n,
    )
    embedder = MagicMock()  # unused — embed_and_update is patched
    result = await ew.run_embedding_worker(cfg, MagicMock(), embedder=embedder)
    assert result["consumed"] == n
    assert result["embedded"] == n
    return max_inflight


async def test_budget_one_is_strictly_sequential(monkeypatch: pytest.MonkeyPatch) -> None:
    max_inflight = await _run_with_budget(monkeypatch, budget=1, n=6)
    assert max_inflight == 1, f"budget=1 must serialise; saw {max_inflight} concurrent"


async def test_budget_caps_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    # 8 messages, budget 3 → never more than 3 embeds in flight, but it DID
    # overlap (more than 1), proving concurrency is real.
    max_inflight = await _run_with_budget(monkeypatch, budget=3, n=8)
    assert max_inflight <= 3, f"budget=3 exceeded; saw {max_inflight}"
    assert max_inflight >= 2, f"expected real overlap; saw {max_inflight}"
