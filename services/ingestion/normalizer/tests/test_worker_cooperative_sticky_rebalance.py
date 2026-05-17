"""M2.3 — Cooperative-sticky rebalance against a real Kafka broker.

Per M2 work-order §M2.3: the normalizer consumer pool uses
cooperative-sticky assignment so the pool can scale (add/remove
workers) without losing messages or producing duplicates during
the rebalance.

This test is INTENTIONALLY not mocked. Mocking the rebalance defeats
the point — the value of the test is that aiokafka + librdkafka +
the cooperative-sticky strategy actually behave as advertised against
a real broker. Per the M2.3 work-order: "testcontainers, not mocks."

The test:
  1. Spins a fresh Kafka via testcontainers.
  2. Creates `ingestion.raw` with 4 partitions so two workers can
     share the load.
  3. Pre-publishes 40 raw envelopes.
  4. Starts worker 1; lets it consume a few messages.
  5. Starts worker 2 (same consumer group); waits for rebalance.
  6. Both workers run to completion (stop_after each).
  7. Asserts every envelope was consumed exactly once across both
     workers — no losses, no duplicates.

If testcontainers / Docker is genuinely unavailable, the test
SKIPS cleanly via the `requires_docker` marker. Per M2 work-order:
"If testcontainers Kafka is unworkable in the test environment,
surface as a finding — do NOT substitute a mock."
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import orjson
import pytest

# Testcontainers + Docker are dev-only; skip if not available so the
# test suite still passes in environments without Docker (CI nodes
# that haven't enabled dind, etc.).
try:
    import docker as _docker_module  # type: ignore[import-not-found]
    from testcontainers.kafka import KafkaContainer  # type: ignore[import-not-found]
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


pytestmark = [
    pytest.mark.requires_docker,
    pytest.mark.skipif(
        not _HAS_TESTCONTAINERS,
        reason="testcontainers / docker SDK unavailable",
    ),
    pytest.mark.timeout(120),  # broker boot + consumer-group churn
]


def _docker_available() -> bool:
    if not _HAS_TESTCONTAINERS:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------


class _InMemoryS3:
    """Tiny in-memory replacement for S3Client. The rebalance test
    cares about Kafka behaviour, not S3 — using moto here would
    add a second container without adding any signal."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self._store[key] = body

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get(self, key: str) -> bytes:
        return self._store[key]


class _CaptureProducer:
    """Replaces IdempotentProducer; records what the worker would
    have published to `ingestion.normalized` so the test can assert
    once-and-only-once across both workers.

    Thread-safe access via asyncio.Lock — both workers run in the
    same event loop in this test (different asyncio tasks).
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        return None

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        return None

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        **_kw: Any,
    ) -> None:
        async with self._lock:
            self.published.append((topic, value, key))


def _slack_payload(i: int) -> dict:
    return {
        "event": {
            "type": "message",
            "channel": f"C{i:05d}",
            "user": f"U{i:05d}",
            "text": f"msg #{i}",
            "ts": f"17474832{i:04d}.001000",
            "team": "T01ACME",
        },
    }


def _envelope_bytes_for(i: int, *, s3: _InMemoryS3) -> bytes:
    from services.ingestion.raw_tier.envelope import RawEnvelope

    tenant = uuid4()
    payload = _slack_payload(i)
    raw_body = orjson.dumps(payload)
    content_hash = f"{i:040x}"
    s3_key = f"dev/slack/{tenant}/2026-05/{content_hash[:2]}/{content_hash}.json"
    s3.put(s3_key, raw_body)
    envelope = RawEnvelope(
        source="slack",
        tenant_id=tenant,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc),
        ingress_kind="webhook",
        ingress_metadata={"i": i},
    )
    return orjson.dumps(envelope.model_dump(mode="json"))


# ---------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------


@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_cooperative_sticky_two_workers_no_loss_no_duplicates(
    monkeypatch,
):
    """End-to-end cooperative-sticky rebalance against a real broker.

    Why this test exists: at the heart of the M2 design is the claim
    that the normalizer pool can scale by adding processes to the
    same consumer group. The cooperative-sticky strategy means new
    workers steal partitions incrementally without a stop-the-world
    pause. Mocking the rebalance protocol would test our mock, not
    the protocol.

    The test publishes 40 envelopes across 4 partitions, starts
    worker A (consumes a few), then starts worker B in the SAME
    group, then lets both drain. Every envelope must be consumed
    exactly once across the pool.
    """
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import Producer as RawProducer

    # ---- Boot the broker ----
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()

        # Create the topic with 4 partitions so the rebalance can
        # actually redistribute. A 1-partition topic would not
        # exercise the strategy at all.
        admin = AdminClient({"bootstrap.servers": bootstrap})
        topic_fut = admin.create_topics([
            NewTopic("ingestion.raw", num_partitions=4, replication_factor=1),
        ])
        for fut in topic_fut.values():
            fut.result(timeout=30)

        # Pre-publish 40 envelopes (10 per partition on average).
        s3 = _InMemoryS3()
        envelopes = [_envelope_bytes_for(i, s3=s3) for i in range(40)]
        raw_producer = RawProducer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "zstd",
        })
        for i, env_bytes in enumerate(envelopes):
            raw_producer.produce(
                "ingestion.raw",
                value=env_bytes,
                key=f"key-{i}".encode("utf-8"),
            )
        raw_producer.flush(timeout=30)

        # ---- Patch the worker's S3 + producer dependencies ----
        # The worker constructs both internally from config; we
        # replace the classes at module-import time so it picks up
        # the in-memory variants.
        from services.ingestion.normalizer import worker as worker_module

        capture_producer = _CaptureProducer()

        # Worker constructs S3Client + IdempotentProducer + AIOKafkaConsumer
        # in run_worker. We patch the constructors at module level.
        monkeypatch.setattr(
            worker_module, "S3Client", lambda *a, **kw: s3,
        )
        monkeypatch.setattr(
            worker_module, "IdempotentProducer", lambda *a, **kw: capture_producer,
        )

        worker_module.reset_metrics()

        from services.ingestion.normalizer.worker import (
            WorkerConfig,
            run_worker,
        )

        async def _run_worker_a():
            # Worker A handles the first wave; stops after 15 messages.
            cfg = WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="normalizer-rebalance-test",
                stop_after=15,
            )
            return await run_worker(cfg)

        async def _run_worker_b():
            # Worker B starts a few seconds later (after A has
            # claimed partitions) and drains the rest.
            await asyncio.sleep(3.0)
            cfg = WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="normalizer-rebalance-test",
                stop_after=25,
            )
            return await run_worker(cfg)

        # Run both workers concurrently.
        a_result, b_result = await asyncio.gather(
            _run_worker_a(),
            _run_worker_b(),
        )

        # ---- Assertions ----
        # 1. Combined produced count == 40 (every envelope normalized).
        total_produced = a_result["produced"] + b_result["produced"]
        assert total_produced == 40, (
            f"expected 40 produced across both workers, got "
            f"{total_produced} (A={a_result}, B={b_result})"
        )

        # 2. Each worker did real work (not one starved).
        assert a_result["consumed"] >= 1, a_result
        assert b_result["consumed"] >= 1, b_result

        # 3. No duplicates — exactly 40 unique payloads on
        # `ingestion.normalized`.
        seen_keys = [k for (_, _, k) in capture_producer.published]
        assert len(seen_keys) == 40, len(seen_keys)
        # The normalized envelope's key is the upstream tenant_id;
        # each envelope had a fresh UUID so all 40 keys distinct.
        assert len(set(seen_keys)) == 40, (
            f"duplicates detected — got {len(set(seen_keys))} unique "
            f"keys out of {len(seen_keys)} published"
        )

        # 4. Topic was published to is ingestion.normalized.
        topics = {t for (t, _, _) in capture_producer.published}
        assert topics == {"ingestion.normalized"}, topics
