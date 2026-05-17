"""M3.1 — Normalizer worker DLQ-publish integration tests.

Real Kafka (testcontainers) per the M3.1 work order. Two tests:

  1. test_normalizer_parse_failure_publishes_dlq_envelope
     Send a malformed envelope, run the normalizer, assert exactly
     one message on `ingestion.dlq` with the expected fields.

  2. test_normalizer_dlq_publish_failure_does_not_crash_worker
     [LOAD-BEARING] Inject a Kafka publish failure on the DLQ topic
     by monkey-patching the producer. Assert the worker continues
     consuming the next message normally and bumps the
     `normalizer.dlq_publish.failure` metric.

The load-bearing case is the failure mode that would otherwise
manifest as: "DLQ topic was briefly unreachable → normalizer
crashed → entire shadow pipeline stalled on a partition."
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import orjson
import pytest

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
    pytest.mark.timeout(120),
]


def _docker_available() -> bool:
    if not _HAS_TESTCONTAINERS:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


class _InMemoryS3:
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


def _make_topics(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for fut in admin.create_topics([
        NewTopic("ingestion.raw", num_partitions=1, replication_factor=1),
        NewTopic("ingestion.dlq", num_partitions=1, replication_factor=1),
    ]).values():
        fut.result(timeout=30)


def _publish_raw(bootstrap: str, msgs: list[bytes]) -> None:
    from confluent_kafka import Producer as RawProducer
    p = RawProducer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
    })
    for b in msgs:
        p.produce("ingestion.raw", value=b, key=b"k")
    p.flush(timeout=30)


def _drain_dlq(bootstrap: str, expected: int, timeout_s: float = 30.0) -> list[dict]:
    """Read up to `expected` messages from ingestion.dlq, decode JSON,
    return the parsed dicts."""
    from confluent_kafka import Consumer as RawConsumer
    c = RawConsumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"dlq-drain-{uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe(["ingestion.dlq"])
    out: list[dict] = []
    deadline = asyncio.get_event_loop().time() + timeout_s
    while len(out) < expected:
        msg = c.poll(1.0)
        if msg is None:
            if asyncio.get_event_loop().time() > deadline:
                break
            continue
        if msg.error():
            continue
        out.append(orjson.loads(msg.value()))
    c.close()
    return out


# =====================================================================
# 1. Parse failure → DLQ publish with correct fields.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_normalizer_parse_failure_publishes_dlq_envelope(monkeypatch):
    """Send a malformed RawEnvelope (missing required fields). The
    normalizer's parse_failure path must publish ONE DLQ envelope
    with failure_kind='normalizer.parse_failure' and the original
    tenant_id / source / raw_s3_key (best-effort-extracted)."""
    from services.ingestion.normalizer import worker as worker_module

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _make_topics(bootstrap)

        tenant = uuid4()
        # Best-effort-extractable: has tenant_id + source + raw_s3_key,
        # missing required content_hash + ingested_at + ingress_kind.
        partial = orjson.dumps({
            "envelope_version": 1,
            "source": "slack",
            "tenant_id": str(tenant),
            "raw_s3_key": f"dev/slack/{tenant}/2026-05/aa/" + "a" * 40 + ".json",
            # MISSING: content_hash, ingested_at, ingress_kind.
        })
        _publish_raw(bootstrap, [partial])

        s3 = _InMemoryS3()
        monkeypatch.setattr(worker_module, "S3Client", lambda *a, **kw: s3)
        worker_module.reset_metrics()

        result = await worker_module.run_worker(
            worker_module.WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-int-test-1",
                stop_after=1,
            )
        )
        assert result["consumed"] == 1
        assert result["produced"] == 0

        # ===== Assertions =====
        m = worker_module.get_metrics()
        assert m["normalizer.parse_failure"] == 1
        assert m["normalizer.dlq_publish.success"] == 1, m

        dlq_msgs = _drain_dlq(bootstrap, expected=1, timeout_s=15.0)
        assert len(dlq_msgs) == 1
        env = dlq_msgs[0]
        assert env["envelope_version"] == 1
        assert env["failure_kind"] == "normalizer.parse_failure"
        assert env["source"] == "slack"
        assert env["tenant_id"] == str(tenant)
        assert env["raw_s3_key"].endswith(".json")
        assert "ValidationError" in env["error_summary"] or "validation" in env["error_summary"].lower()


# =====================================================================
# 2. DLQ Kafka publish failure → worker continues. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_normalizer_dlq_publish_failure_does_not_crash_worker(
    monkeypatch,
):
    """LOAD-BEARING (M3.1): if the DLQ Kafka publish itself fails,
    the worker MUST continue consuming. Without this property, a
    transient Kafka outage on the DLQ topic alone would deadline-
    loop the entire normalizer pipeline.

    Setup:
      - 3 messages on ingestion.raw, all malformed (parse failures).
      - The producer's `produce` is monkey-patched: every call to
        topic="ingestion.dlq" raises a simulated KafkaError. Calls to
        "ingestion.normalized" pass through normally.
      - Worker consumes all 3 with stop_after=3.

    Assertions:
      - All 3 messages consumed (worker did not stall).
      - 3 dlq_publish.failure metric increments.
      - parse_failure metric == 3.
      - 0 successful DLQ publishes (the mock blocked them).
    """
    from services.ingestion.kafka.producer import IdempotentProducer
    from services.ingestion.normalizer import worker as worker_module

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        _make_topics(bootstrap)

        # Pre-publish 3 malformed raw envelopes — each has tenant_id +
        # source so DLQ extraction succeeds; missing other fields so
        # Pydantic validation fails.
        tenants = [uuid4() for _ in range(3)]
        bad_msgs = [
            orjson.dumps({
                "source": "slack",
                "tenant_id": str(t),
                "raw_s3_key": f"dev/slack/{t}/2026-05/aa/" + "a" * 40 + ".json",
            })
            for t in tenants
        ]
        _publish_raw(bootstrap, bad_msgs)

        # Tripwire: hook the producer's produce method so DLQ
        # publishes raise. We monkey-patch the IdempotentProducer
        # class so any instance the worker constructs gets the
        # patched method.
        original_produce = IdempotentProducer.produce
        dlq_publish_attempts = {"n": 0}

        async def flaky_produce(
            self, topic: str, value: bytes, *, key: bytes | None = None,
            **kwargs,
        ) -> None:
            if topic == "ingestion.dlq":
                dlq_publish_attempts["n"] += 1
                raise RuntimeError("simulated Kafka DLQ outage")
            return await original_produce(
                self, topic=topic, value=value, key=key, **kwargs,
            )

        monkeypatch.setattr(IdempotentProducer, "produce", flaky_produce)

        s3 = _InMemoryS3()
        monkeypatch.setattr(worker_module, "S3Client", lambda *a, **kw: s3)
        worker_module.reset_metrics()

        result = await worker_module.run_worker(
            worker_module.WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="dlq-int-test-2",
                stop_after=3,
            )
        )

        # ===== LOAD-BEARING ASSERTIONS — worker continued =====
        # All three messages were consumed. If the worker had crashed
        # on the first DLQ publish failure, this would be < 3, OR the
        # test would have timed out (caught by pytest-timeout).
        assert result["consumed"] == 3, (
            f"worker stalled after DLQ publish failure — consumed "
            f"only {result['consumed']}/3"
        )
        assert result["produced"] == 0  # All were parse failures
        # The producer's produce was called for DLQ 3 times (one per
        # bad message), and every call raised.
        assert dlq_publish_attempts["n"] == 3, dlq_publish_attempts

        m = worker_module.get_metrics()
        # parse_failure incremented for each bad message.
        assert m["normalizer.parse_failure"] == 3, m
        # DLQ publishes all failed (the monkey-patch raised every time).
        assert m["normalizer.dlq_publish.failure"] == 3, m
        # No DLQ publishes succeeded.
        assert m["normalizer.dlq_publish.success"] == 0, m
