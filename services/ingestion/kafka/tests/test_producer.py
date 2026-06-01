"""Unit tests for services.ingestion.kafka.producer (M2.1).

Verifies the ProducerConfig → librdkafka dict mapping. The async
wrapper around confluent_kafka.Producer is exercised by the M2.4
end-to-end shadow test against a real (testcontainers/dev-stack)
broker; per-method unit tests of `produce()` / `flush()` would
require patching the C-level Producer constructor in a way that is
brittle and offers little signal.
"""
from __future__ import annotations

import pytest

from services.ingestion.kafka import IdempotentProducer, ProducerConfig


def test_producer_config_idempotence_defaults_match_lld() -> None:
    """Per LLD §5.2 + M2.1 work order: `enable.idempotence=true`,
    `acks='all'`, `max.in.flight.requests.per.connection=5`,
    `compression.type='zstd'`. Tightened to exact equality (per M2.1
    review): catches "we forgot to enable idempotence" regressions
    AND the off-by-one variants (e.g. accidentally setting
    `max.in.flight=1` for a stricter ordering guarantee at the cost
    of throughput, or dropping `acks='all'` to `acks='1'`).
    """
    cfg = ProducerConfig()
    d = cfg.to_confluent_dict()
    assert d["enable.idempotence"] is True
    assert d["acks"] == "all"
    assert d["max.in.flight.requests.per.connection"] == 5
    assert d["compression.type"] == "zstd"


def test_producer_config_override_via_extra() -> None:
    cfg = ProducerConfig(extra={"socket.timeout.ms": 30000})
    d = cfg.to_confluent_dict()
    assert d["socket.timeout.ms"] == 30000
    # Stock keys still present.
    assert d["enable.idempotence"] is True


def test_producer_config_no_transactional_id() -> None:
    """Per LLD §5.2 amendment (M1 closeout): `transactional_id`
    requires begin/commit_transaction calls we don't make.
    `enable_idempotence=True` alone is the correct setting; the
    config must NOT set `transactional.id`.
    """
    d = ProducerConfig().to_confluent_dict()
    assert "transactional.id" not in d


async def test_idempotent_producer_starts_idempotently() -> None:
    """Two start() calls without an intervening stop are a no-op on
    the second. Verifies the singleton-style contract.

    This test instantiates IdempotentProducer but does NOT actually
    connect to a broker — `start()` only calls confluent_kafka's
    Producer constructor which is purely a librdkafka handle setup.
    The producer's background poll thread starts but doesn't try to
    connect until produce() is called. Safe for unit-test scope.
    """
    p = IdempotentProducer(ProducerConfig(bootstrap_servers="localhost:9092"))
    assert p.is_started is False
    await p.start()
    assert p.is_started is True
    # Idempotent — second start() is a no-op.
    await p.start()
    assert p.is_started is True
    # Cleanup. flush with a tight timeout since we never produced
    # anything; the queue is empty.
    await p.stop(timeout_seconds=1.0)
    assert p.is_started is False
