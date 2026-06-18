"""F3 — observation_writer durable poison-cap → DLQ tests.

A DETERMINISTIC poison message (a code bug that fails identically every time)
would, before F3, be retried in-process up to `_TRANSIENT_MAX_ATTEMPTS` then
re-raised — crashing the writer. The supervisor restarts it, Kafka redelivers
the same UNcommitted offset, and the partition head-of-line-blocks forever,
stalling any backfill sharing the per-tenant key.

F3 adds a durable (restart-surviving) give-up counter keyed by
`(topic, partition, offset)` in `writer_poison_attempts`. After
`_POISON_MAX_DURABLE_ATTEMPTS` cross-restart give-ups the message is parked to
the DLQ and the call RETURNS (so the caller commits) instead of re-raising, so
the partition advances. Counter writes are best-effort: a counter-store outage
degrades to the legacy re-raise — we never DLQ a message because the
bookkeeping failed.

These tests drive `_handle_message_with_retry` directly with a fake Kafka
message and a monkeypatched `_handle_message`, so no broker is needed. The
DB-backed cases use `fresh_db` (which applies migration 0137).
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import asyncpg
import orjson
import pytest

from services.ingest.ingestion.writers import observation_writer as writer_module


pytestmark = [pytest.mark.timeout(120)]


@pytest.fixture(autouse=True)
def _reset_writer_state() -> None:
    writer_module.reset_metrics()


class _CaptureProducer:
    """IdempotentProducer stand-in; captures DLQ publishes."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        return None

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        return None

    async def produce(
        self, topic: str, value: bytes, *, key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))


class _Boom(RuntimeError):
    """A deterministic transient/unknown error (stands in for a code bug)."""


class _FakeMsg:
    """Minimal AIOKafka ConsumerRecord stand-in."""

    def __init__(
        self,
        *,
        topic: str = "ingestion.normalized.slack",
        partition: int = 0,
        offset: int = 0,
        source: str = "slack",
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        # Valid-enough envelope bytes so publish_dlq can extract tenant+source
        # and actually publish (it skips when those are absent).
        self.value = orjson.dumps(
            {"tenant_id": str(uuid4()), "source": source, "content_text": "x"}
        )
        self.key = b"tenant"


def _patch_caps(
    monkeypatch: pytest.MonkeyPatch, *, durable_cap: int, transient_max: int = 1,
) -> None:
    monkeypatch.setattr(writer_module, "_POISON_MAX_DURABLE_ATTEMPTS", durable_cap)
    monkeypatch.setattr(writer_module, "_TRANSIENT_MAX_ATTEMPTS", transient_max)
    monkeypatch.setattr(writer_module, "_TRANSIENT_BACKOFF_BASE_S", 0.0)


def _always_raise() -> Any:
    async def _boom(*_a: Any, **_kw: Any) -> None:
        raise _Boom("deterministic poison")

    return _boom


async def test_poison_dlq_after_durable_cap(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At the cap, the message is DLQ-parked and the call RETURNS (so the
    caller commits and the partition advances) instead of re-raising. The
    durable counter row is cleared."""
    _patch_caps(monkeypatch, durable_cap=1)
    monkeypatch.setattr(writer_module, "_handle_message", _always_raise())
    msg = _FakeMsg(offset=42)
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=fresh_db)

    # Must NOT raise — poison is parked, not propagated.
    await writer_module._handle_message_with_retry(
        msg, config=config, dlq_producer=capture,
        embedding_producer=capture, stop_event=asyncio.Event(),
    )

    assert len(capture.published) == 1, "poison should be published to the DLQ"
    topic, _value, _key = capture.published[0]
    assert topic.startswith("ingestion.dlq"), topic
    metrics = writer_module.get_metrics()
    assert metrics["writer.poison_dlq"] == 1
    assert metrics["writer.dlq_publish.success"] == 1
    # Counter row cleared after parking.
    n = await fresh_db.fetchval(
        'SELECT count(*) FROM writer_poison_attempts WHERE "offset" = $1', 42,
    )
    assert n == 0


async def test_poison_below_cap_still_reraises(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Below the cap, behaviour is unchanged: the error re-raises (so the
    supervisor restarts and Kafka redelivers), nothing is DLQ'd, and the
    durable counter increments."""
    _patch_caps(monkeypatch, durable_cap=50)
    monkeypatch.setattr(writer_module, "_handle_message", _always_raise())
    msg = _FakeMsg(offset=7)
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=fresh_db)

    with pytest.raises(_Boom):
        await writer_module._handle_message_with_retry(
            msg, config=config, dlq_producer=capture,
            embedding_producer=capture, stop_event=asyncio.Event(),
        )

    assert capture.published == []
    assert writer_module.get_metrics()["writer.poison_dlq"] == 0
    attempts = await fresh_db.fetchval(
        'SELECT attempts FROM writer_poison_attempts WHERE "offset" = $1', 7,
    )
    assert attempts == 1


async def test_poison_counter_outage_degrades_to_reraise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no pool (counter store unavailable), the cap is inert: the writer
    re-raises rather than DLQ-ing — we never drop on a bookkeeping failure."""
    _patch_caps(monkeypatch, durable_cap=1)
    monkeypatch.setattr(writer_module, "_handle_message", _always_raise())
    msg = _FakeMsg(offset=1)
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=None)

    with pytest.raises(_Boom):
        await writer_module._handle_message_with_retry(
            msg, config=config, dlq_producer=capture,
            embedding_producer=capture, stop_event=asyncio.Event(),
        )

    assert capture.published == []
    assert writer_module.get_metrics()["writer.poison_dlq"] == 0


async def test_poison_giveup_on_shutdown_is_not_counted(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A give-up triggered by shutdown (stop_event set) is NOT poison: it
    re-raises so the loop exits cleanly and does NOT touch the durable
    counter (the offset replays on restart)."""
    _patch_caps(monkeypatch, durable_cap=1)
    monkeypatch.setattr(writer_module, "_handle_message", _always_raise())
    msg = _FakeMsg(offset=5)
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=fresh_db)
    stop = asyncio.Event()
    stop.set()

    with pytest.raises(_Boom):
        await writer_module._handle_message_with_retry(
            msg, config=config, dlq_producer=capture,
            embedding_producer=capture, stop_event=stop,
        )

    assert capture.published == []
    assert writer_module.get_metrics()["writer.poison_dlq"] == 0
    n = await fresh_db.fetchval(
        'SELECT count(*) FROM writer_poison_attempts WHERE "offset" = $1', 5,
    )
    assert n == 0, "shutdown give-up must not write a poison counter row"


async def test_poison_counter_cleared_on_success(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A message that fails an in-process attempt then SUCCEEDS clears its
    durable counter, so a transient blip doesn't accrue toward the cap."""
    _patch_caps(monkeypatch, durable_cap=50, transient_max=5)
    msg = _FakeMsg(offset=99)
    # Pre-seed a stale counter row for this coordinate.
    await fresh_db.execute(
        'INSERT INTO writer_poison_attempts (topic, partition, "offset", attempts)'
        " VALUES ($1, $2, $3, 3)",
        msg.topic, msg.partition, msg.offset,
    )

    calls = {"n": 0}

    async def _fail_then_succeed(*_a: Any, **_kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Boom("blip")
        return None

    monkeypatch.setattr(writer_module, "_handle_message", _fail_then_succeed)
    capture = _CaptureProducer()
    config = writer_module.WriterConfig(pool=fresh_db)

    await writer_module._handle_message_with_retry(
        msg, config=config, dlq_producer=capture,
        embedding_producer=capture, stop_event=asyncio.Event(),
    )

    n = await fresh_db.fetchval(
        'SELECT count(*) FROM writer_poison_attempts WHERE "offset" = $1', 99,
    )
    assert n == 0, "counter row should be cleared after eventual success"
