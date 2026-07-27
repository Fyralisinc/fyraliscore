"""Source-isolation Phase 6: the normalizer's concurrent loop overlaps S3
GETs across tenants while preserving per-tenant ordering.

Mocks the Kafka/health boundary and uses a concurrency-tracking S3 stub, so
it runs in the unit lane (no broker). With max_concurrency=N over M tenants
(distinct Kafka keys), more than one S3 GET is in flight at once (overlap is
real) but never more than N (bounded).
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import orjson
import pytest

from services.ingest.ingestion.normalizer import worker as worker_module
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope


def _good_payload() -> dict[str, Any]:
    return {
        "event": {
            "type": "message",
            "channel": "C00good",
            "user": "U00good",
            "text": "hello",
            "ts": "1747483200.001000",
            "team": "T01ACME",
        },
    }


class _TrackingS3:
    """In-memory S3 whose get() sleeps (so overlap is observable) and records
    the maximum number of concurrent in-flight gets."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self.inflight = 0
        self.max_inflight = 0

    def put(self, key: str, body: bytes) -> None:
        self._store[key] = body

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get(self, key: str) -> bytes:
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            await asyncio.sleep(0.02)
            return self._store[key]
        finally:
            self.inflight -= 1


def _envelope_msg(tenant_id: UUID, s3: _TrackingS3, i: int) -> MagicMock:
    body = orjson.dumps(_good_payload())
    h = f"{i:040x}"  # 40-hex content hash (lowercase) — passes invariants
    ingested_at = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0)
    key = f"dev/slack/{tenant_id}/{ingested_at:%Y-%m}/{h[:2]}/{h}.json"
    s3.put(key, body)
    env = RawEnvelope(
        source="slack",
        tenant_id=tenant_id,
        raw_s3_key=key,
        content_hash=h,
        ingested_at=ingested_at,
        ingress_kind="webhook",
    )
    m = MagicMock()
    m.value = orjson.dumps(env.model_dump(mode="json"))
    m.key = str(tenant_id).encode("utf-8")
    m.topic, m.partition, m.offset, m.timestamp = "ingestion.raw.slack", 0, i, 0
    return m


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    worker_module.reset_metrics()


async def _run_concurrent(
    monkeypatch: pytest.MonkeyPatch, *, budget: int, tenants: int, per_tenant: int
) -> tuple[dict[str, int], int]:
    s3 = _TrackingS3()
    messages = []
    idx = 0
    for _ in range(tenants):
        tid = uuid4()
        for _ in range(per_tenant):
            messages.append(_envelope_msg(tid, s3, idx))
            idx += 1
    n = len(messages)

    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.subscribe = MagicMock()
    consumer.getmany = AsyncMock(return_value={("tp", 0): messages})
    monkeypatch.setattr(worker_module, "AIOKafkaConsumer", lambda *a, **k: consumer)

    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.produce = AsyncMock()
    producer.flush = AsyncMock(return_value=0)
    monkeypatch.setattr(worker_module, "IdempotentProducer", lambda *a, **k: producer)
    monkeypatch.setattr(worker_module, "S3Client", lambda *a, **k: s3)
    monkeypatch.setattr(worker_module, "start_health_server", lambda **k: None)

    async def _noop_ticker(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(worker_module, "run_heartbeat_ticker", _noop_ticker)

    cfg = worker_module.WorkerConfig(
        source="slack", max_concurrency=budget, stop_after=n,
    )
    result = await worker_module.run_worker(cfg)
    return result, s3.max_inflight


async def test_concurrent_overlaps_across_tenants(monkeypatch: pytest.MonkeyPatch) -> None:
    result, max_inflight = await _run_concurrent(
        monkeypatch, budget=4, tenants=4, per_tenant=2,
    )
    assert result["consumed"] == 8
    assert result["produced"] == 8
    # Real overlap (more than one tenant's S3 GET in flight) but bounded.
    assert 2 <= max_inflight <= 4, max_inflight


async def test_budget_one_path_is_serial_equivalent(monkeypatch: pytest.MonkeyPatch) -> None:
    # max_concurrency=1 uses the serial loop; this drives it via the mocked
    # next_or_stop path instead of getmany.
    s3 = _TrackingS3()
    msgs = [_envelope_msg(uuid4(), s3, i) for i in range(3)]

    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.subscribe = MagicMock()
    monkeypatch.setattr(worker_module, "AIOKafkaConsumer", lambda *a, **k: consumer)

    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.produce = AsyncMock()
    producer.flush = AsyncMock(return_value=0)
    monkeypatch.setattr(worker_module, "IdempotentProducer", lambda *a, **k: producer)
    monkeypatch.setattr(worker_module, "S3Client", lambda *a, **k: s3)
    monkeypatch.setattr(worker_module, "start_health_server", lambda **k: None)

    async def _noop_ticker(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(worker_module, "run_heartbeat_ticker", _noop_ticker)

    # Serial loop consumes via next_or_stop: feed msgs then None.
    feed = iter(msgs)

    async def _next_or_stop(_c: Any, _ev: Any) -> Any:
        return next(feed, None)

    monkeypatch.setattr(worker_module, "next_or_stop", _next_or_stop)

    cfg = worker_module.WorkerConfig(source="slack", max_concurrency=1)
    result = await worker_module.run_worker(cfg)
    assert result["consumed"] == 3
    assert result["produced"] == 3
    assert s3.max_inflight == 1  # strictly serial
