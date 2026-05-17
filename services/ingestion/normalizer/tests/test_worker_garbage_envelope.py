"""M2.4 — "don't get stuck on garbage" property test.

Per M2 work-order §M2.4:

    "assert_envelope_invariants raises EnvelopeInvariantError which
    is logged + metric'd but NOT propagated. The reasoning: a
    malformed envelope is parse-failure-class; the Kafka message
    must be acknowledged so the consumer doesn't loop forever
    retrying garbage. Produce a malformed envelope, assert the
    worker logs, increments the metric, and continues consuming
    the next message."

This test uses a REAL Kafka broker (testcontainers) because the
"continues consuming the next message" property fundamentally
depends on the consumer-group offset commit happening — which only
makes sense against a real broker, not a mock.

Sequence:
  1. Boot Kafka via testcontainers.
  2. Publish FOUR messages to ingestion.raw:
     (a) byte garbage (not JSON)
     (b) JSON object that isn't a valid RawEnvelope (Pydantic
         rejects)
     (c) valid RawEnvelope shape but FAILS invariants (bad
         content_hash format)
     (d) a valid envelope referencing a real S3 body
  3. Run one worker with stop_after=4.
  4. Assert:
     - the worker consumed all 4 (no infinite loop).
     - 3 envelopes hit parse_failure / invariant_failure metrics.
     - exactly 1 normalized envelope was produced.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

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


class _CaptureProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        return None

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        return None

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0

    async def produce(self, topic: str, value: bytes, *,
                      key: bytes | None = None, **_kw: Any) -> None:
        self.published.append((topic, value, key))


@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_garbage_envelope_does_not_stall_consumer(monkeypatch):
    """LOAD-BEARING (M2.4): the worker must commit + advance past
    every malformed message, NOT spin on it. If this test deadlocks
    or fails with worker_consumed < 4, the prime directive is broken
    and a single bad message will stop M2's data plane in production.
    """
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import Producer as RawProducer

    from services.ingestion.normalizer import worker as worker_module
    from services.ingestion.raw_tier.envelope import RawEnvelope

    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()
        admin = AdminClient({"bootstrap.servers": bootstrap})
        for fut in admin.create_topics([
            NewTopic("ingestion.raw", num_partitions=1, replication_factor=1),
        ]).values():
            fut.result(timeout=30)

        # ---- Publish 4 messages in order: 3 garbage + 1 valid ----
        raw_producer = RawProducer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "zstd",
        })

        # (a) Byte garbage — not JSON at all.
        raw_producer.produce(
            "ingestion.raw", value=b"\x00\xff\xfe not json garbage", key=b"a",
        )

        # (b) Valid JSON, INVALID RawEnvelope (missing required fields).
        raw_producer.produce(
            "ingestion.raw",
            value=orjson.dumps({"source": "slack", "missing": "fields"}),
            key=b"b",
        )

        # (c) Valid RawEnvelope shape, FAILS invariants. content_hash
        # is the right length and matches the s3_key prefix but is
        # UPPERCASE — the invariant requires lowercase hex.
        bad_tenant = uuid4()
        bad_hash = "A" * 40  # uppercase — fails _CONTENT_HASH_RE
        bad_env = RawEnvelope(
            source="slack",
            tenant_id=bad_tenant,
            raw_s3_key=f"dev/slack/{bad_tenant}/2026-05/{bad_hash[:2].lower()}/{bad_hash.lower()}.json",
            content_hash=bad_hash,
            ingested_at=dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc),
            ingress_kind="webhook",
        )
        raw_producer.produce(
            "ingestion.raw",
            value=orjson.dumps(bad_env.model_dump(mode="json")),
            key=b"c",
        )

        # (d) The VALID envelope — payload referenced in S3.
        s3 = _InMemoryS3()
        good_tenant = uuid4()
        good_payload = {
            "event": {
                "type": "message",
                "channel": "C00good",
                "user": "U00good",
                "text": "this one should land",
                "ts": "1747483200.001000",
                "team": "T01ACME",
            },
        }
        good_body = orjson.dumps(good_payload)
        good_hash = "d" * 40
        good_key = f"dev/slack/{good_tenant}/2026-05/{good_hash[:2]}/{good_hash}.json"
        s3.put(good_key, good_body)
        good_env = RawEnvelope(
            source="slack",
            tenant_id=good_tenant,
            raw_s3_key=good_key,
            content_hash=good_hash,
            ingested_at=dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc),
            ingress_kind="webhook",
        )
        raw_producer.produce(
            "ingestion.raw",
            value=orjson.dumps(good_env.model_dump(mode="json")),
            key=b"d",
        )

        raw_producer.flush(timeout=30)

        # ---- Patch worker S3 + producer ----
        capture = _CaptureProducer()
        monkeypatch.setattr(worker_module, "S3Client", lambda *a, **kw: s3)
        monkeypatch.setattr(
            worker_module, "IdempotentProducer", lambda *a, **kw: capture,
        )

        worker_module.reset_metrics()

        # Run worker. stop_after=4 forces the worker to consume ALL
        # four messages (or hang — which is the failure mode this
        # test catches via the pytest.mark.timeout(120) marker).
        result = await worker_module.run_worker(
            worker_module.WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="normalizer-garbage-test",
                stop_after=4,
            )
        )

        # ===== LOAD-BEARING ASSERTIONS =====

        # 1. The worker consumed ALL FOUR messages. If garbage had
        # stalled the consumer, this would be < 4 (or the test would
        # have timed out via pytest-timeout — which would also be a
        # failure mode this test catches).
        assert result["consumed"] == 4, (
            f"worker stalled — consumed only {result['consumed']}/4 "
            f"messages. Garbage poisoned the consumer."
        )

        # 2. Only ONE normalized envelope was published — the valid
        # one. Three malformed messages produced nothing on the
        # downstream NORMALIZED topic. M3.1 added DLQ publish for
        # invariant-failure messages with full envelope fields, so
        # message (c)'s DLQ publish ALSO appears in `capture.published`.
        assert result["produced"] == 1, (
            f"expected 1 produced (only the valid envelope), got "
            f"{result['produced']}"
        )
        # Topics broken down:
        #   - ingestion.normalized × 1 (message d, valid envelope)
        #   - ingestion.dlq        × 1 (message c, invariant failure
        #                               — full envelope so best-effort
        #                               extract succeeds)
        # Messages (a) (byte garbage) and (b) (no tenant_id) skip the
        # DLQ publish because best-effort extraction can't pull a
        # valid (tenant_id, source) pair.
        topic_counts = {t: 0 for t in {"ingestion.normalized", "ingestion.dlq"}}
        for (topic, _, _) in capture.published:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
        assert topic_counts["ingestion.normalized"] == 1, topic_counts
        assert topic_counts["ingestion.dlq"] == 1, topic_counts
        assert len(capture.published) == 2

        # 3. Failure metrics incremented correctly.
        m = worker_module.get_metrics()
        # (a) was a JSON parse failure → parse_failure
        # (b) was a Pydantic validation failure → parse_failure
        # (c) was an invariant failure → invariant_failure AND parse_failure
        assert m["normalizer.invariant_failure"] >= 1, m
        # Each failure bumps parse_failure; (c) bumps it once via the
        # invariant_failure handler.
        assert m["normalizer.parse_failure"] == 3, m
        assert m["normalizer.messages_consumed"] == 4
        assert m["normalizer.messages_produced"] == 1
        # M3.1 — DLQ publish telemetry. (c) succeeds; (a) and (b) skip
        # because best-effort extract can't recover the required
        # (tenant_id, source) pair.
        assert m["normalizer.dlq_publish.success"] == 1, m
        assert m["normalizer.dlq_publish.skipped"] == 2, m
