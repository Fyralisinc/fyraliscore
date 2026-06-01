"""Source-isolation Phase 3: the normalizer subscribes to the right topic(s)
and joins the right consumer group depending on ``config.source``.

These mock the Kafka/S3/health boundary so the wiring is exercised without a
live broker — `next_or_stop` returns None so the loop exits immediately after
subscribe().
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.ingest.ingestion.kafka import topics
from services.ingest.ingestion.normalizer import worker as worker_module


class _FakeConsumer:
    """Records the group_id it was constructed with and the topics passed to
    subscribe(); everything else is a no-op stub.
    """

    last_group_id: str | None = None
    last_subscribe: list[str] | None = None

    def __init__(self, *_, group_id: str | None = None, **__) -> None:
        _FakeConsumer.last_group_id = group_id

    def subscribe(self, topics_list: list[str], **_: Any) -> None:
        _FakeConsumer.last_subscribe = list(topics_list)

    async def start(self) -> None:  # noqa: D401
        return None

    async def stop(self) -> None:
        return None

    async def commit(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _patch_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_module, "AIOKafkaConsumer", _FakeConsumer)

    fake_producer = MagicMock()
    fake_producer.start = AsyncMock()
    fake_producer.stop = AsyncMock()
    monkeypatch.setattr(
        worker_module, "IdempotentProducer", lambda *_a, **_k: fake_producer
    )

    fake_s3 = MagicMock()
    fake_s3.connect = AsyncMock()
    fake_s3.close = AsyncMock()
    monkeypatch.setattr(worker_module, "S3Client", lambda *_a, **_k: fake_s3)

    # Exit the consume loop immediately (no messages).
    monkeypatch.setattr(worker_module, "next_or_stop", AsyncMock(return_value=None))
    monkeypatch.setattr(worker_module, "start_health_server", lambda **_k: None)

    async def _noop_ticker(*_a: Any, **_k: Any) -> None:
        return None

    monkeypatch.setattr(worker_module, "run_heartbeat_ticker", _noop_ticker)

    _FakeConsumer.last_group_id = None
    _FakeConsumer.last_subscribe = None


async def test_isolated_source_subscribes_to_one_topic_and_per_source_group() -> None:
    config = worker_module.WorkerConfig(source="slack")
    await worker_module.run_worker(config)

    assert _FakeConsumer.last_subscribe == ["ingestion.raw.slack"]
    assert _FakeConsumer.last_group_id == "normalizer.slack"


async def test_all_sources_fallback_subscribes_to_every_raw_topic() -> None:
    config = worker_module.WorkerConfig(source=None)
    await worker_module.run_worker(config)

    assert _FakeConsumer.last_subscribe == topics.topics_for_stage("raw")
    assert len(_FakeConsumer.last_subscribe) == len(topics.INGESTION_SOURCES)
    # All-sources worker keeps the historical bare group id.
    assert _FakeConsumer.last_group_id == "normalizer"


async def test_unknown_source_fails_fast() -> None:
    config = worker_module.WorkerConfig(source="myspace")
    with pytest.raises(ValueError, match="unknown ingestion source"):
        await worker_module.run_worker(config)
