"""M2.4 — End-to-end shadow-path zero-divergence test.

THIS IS M2's LOAD-BEARING TEST.

Per-sub-block load-bearers proved each piece in isolation:
  - M2.1: test_shadow_path_failure_does_not_break_inline (router)
  - M2.2: test_pubsub_shadow_failure_does_not_break_fetch
          test_gateway_shadow_failure_does_not_break_dispatch
  - M2.3: test_worker_no_db_access_under_load (Path B)

THIS TEST proves M2 AS A WHOLE works: 100 synthetic webhooks driven
through BOTH the inline path AND the shadow path, asserting that
the shadow pipeline (S3 + raw Kafka + normalizer + normalized Kafka
+ writer's shadow log) produces a record set for each tenant that
is byte-identical-in-external-id-set to the inline observations
table.

The pipeline under test:

    100 Slack payloads
         │
         ├─→ ingest()                  → 100 observations rows (inline; Path A)
         │
         └─→ shadow_write_raw()        → 100 S3 objects
                                       → 100 ingestion.raw messages
                                          │
                                          ├─→ normalizer worker     (Path B)
                                          │   └→ 100 ingestion.normalized messages
                                          │
                                          └─→ observation_writer    (Path B)
                                              └→ 100 ShadowWriteEvents

Asserted at the end:
  (A) Counts: 100/100/100/100/100/100 across all six counters.
  (B) Set equality on external_ids:
        { obs.external_id for obs in observations table }
        ==
        { event.external_id for event in shadow_log }

Why SET equality (not list / multiset): counts can match while
specific records diverge — a webhook arriving twice within the test
window dedups on the inline path's UNIQUE (source_channel,
external_id) index but the shadow path counts both. Set equality
catches that asymmetry. Per the M2.4 review.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any
from uuid import UUID

import asyncpg
import orjson
import pytest

try:
    import docker as _docker_module  # type: ignore[import-not-found]
    from testcontainers.kafka import KafkaContainer  # type: ignore[import-not-found]
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_docker,
    pytest.mark.skipif(
        not _HAS_TESTCONTAINERS,
        reason="testcontainers / docker SDK unavailable",
    ),
    pytest.mark.timeout(180),
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
# In-process S3 — the bodies travel between shadow_write_raw and the
# normalizer worker. Real S3 would just add latency + a second
# container; the correctness property under test is the data flow,
# not S3's own internals (those are covered by M1.4 tests).
# ---------------------------------------------------------------------


class _InMemoryS3:
    """Implements the S3Client surface area used by shadow_write_raw
    (put_if_absent) and the normalizer worker (get / connect / close).
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self._store.setdefault(key, body)

    async def get(self, key: str) -> bytes:
        return self._store[key]

    def __len__(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------
# Payload builder — 100 distinct Slack messages.
# ---------------------------------------------------------------------


def _payload(i: int) -> dict:
    return {
        "event": {
            "type": "message",
            "channel": f"C{i:05d}",
            "user": f"U{i:05d}",
            "text": f"e2e message #{i}",
            # Slack ts: 10-digit unix epoch seconds + microseconds.
            # Channel varies per i so the (channel, ts) external_id
            # stays unique even with a constant ts.
            "ts": "1747483200.001000",
            "team": "T0E2EE2E",
        },
    }


def _expected_external_id(payload: dict) -> str:
    """Mirrors what services/ingestion/handlers/slack.py emits."""
    ev = payload["event"]
    return f"{ev['channel']}:{ev['ts']}"


# ---------------------------------------------------------------------
# THE TEST
# ---------------------------------------------------------------------


@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_e2e_shadow_100_webhooks_zero_divergence(
    fresh_db: asyncpg.Pool, monkeypatch,
):
    from confluent_kafka.admin import AdminClient, NewTopic

    from lib.shared.ids import uuid7
    from services.ingestion.core import ingest
    from services.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingestion.normalizer import worker as normalizer_module
    from services.ingestion.shadow_write import shadow_write_raw
    from services.ingestion.writers import (
        observation_writer as writer_module,
    )

    # ---- 1. Boot Kafka via testcontainers ----------------------------
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        bootstrap = kafka.get_bootstrap_server()

        admin = AdminClient({"bootstrap.servers": bootstrap})
        for fut in admin.create_topics([
            NewTopic("ingestion.raw", num_partitions=4, replication_factor=1),
            NewTopic("ingestion.normalized", num_partitions=4, replication_factor=1),
        ]).values():
            fut.result(timeout=30)

        # ---- 2. Seed tenant -----------------------------------------
        tenant_id = uuid7()
        await fresh_db.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id, f"e2e-test-{tenant_id.hex[:8]}",
        )

        # ---- 3. Drive 100 webhooks through BOTH paths ---------------
        payloads = [_payload(i) for i in range(100)]

        # 3a. Inline path: ingest() → observations row.
        for payload in payloads:
            await ingest(
                "slack:message",
                payload,
                pool=fresh_db,
                tenant_id=tenant_id,
                enqueue_trigger=False,  # M2: skip the think trigger
            )

        # 3b. Shadow path: shadow_write_raw() → S3 + Kafka.
        s3 = _InMemoryS3()
        shadow_producer = IdempotentProducer(
            ProducerConfig(
                bootstrap_servers=bootstrap,
                client_id="e2e-shadow-producer",
            )
        )
        await shadow_producer.start()
        try:
            for payload in payloads:
                await shadow_write_raw(
                    tenant_id=tenant_id,
                    source="slack",
                    ingress_kind="webhook",
                    raw_body=orjson.dumps(payload),
                    s3_client=s3,
                    kafka_producer=shadow_producer,
                )
        finally:
            await shadow_producer.stop()

        # Sanity: all 100 bodies in S3, no dedup collisions (payloads
        # are unique → content_hashes are unique).
        assert len(s3) == 100, f"S3 dedup collision: {len(s3)} unique bodies"

        # ---- 4. Run normalizer (stop_after=100) ---------------------
        # Patch S3Client at the worker module so it reads from our
        # in-process store. The IdempotentProducer it constructs
        # internally talks to real Kafka.
        monkeypatch.setattr(
            normalizer_module, "S3Client", lambda *a, **kw: s3,
        )
        normalizer_module.reset_metrics()

        norm_result = await normalizer_module.run_worker(
            normalizer_module.WorkerConfig(
                bootstrap_servers=bootstrap,
                consumer_group="normalizer-e2e",
                stop_after=100,
            )
        )

        # ---- 5. Run writer (stop_after=100) -------------------------
        writer_module.reset_metrics()
        writer_module.reset_shadow_log()
        write_result = await writer_module.run_writer(
            writer_module.WriterConfig(
                bootstrap_servers=bootstrap,
                consumer_group="observation-writer-e2e",
                stop_after=100,
            )
        )

        # ====================================================================
        # ASSERTIONS — zero divergence across all six counters + set equality.
        # ====================================================================

        # (A) COUNTS — six counters, all 100.
        # 1. 100 inline observations.
        inline_count = await fresh_db.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1",
            tenant_id,
        )
        assert inline_count == 100, f"inline observations: {inline_count}"

        # 2. 100 S3 objects.
        assert len(s3) == 100, f"S3 objects: {len(s3)}"

        # 3. 100 ingestion.raw messages → normalizer consumed all.
        assert norm_result["consumed"] == 100, (
            f"normalizer consumed: {norm_result['consumed']}"
        )

        # 4. 100 ingestion.normalized messages → normalizer produced
        # all (none dropped to parse_failure / invariant_failure).
        assert norm_result["produced"] == 100, (
            f"normalizer produced: {norm_result['produced']}"
        )
        m_norm = normalizer_module.get_metrics()
        assert m_norm["normalizer.parse_failure"] == 0, m_norm
        assert m_norm["normalizer.invariant_failure"] == 0, m_norm

        # 5. 100 writer consumed.
        assert write_result["consumed"] == 100, (
            f"writer consumed: {write_result['consumed']}"
        )

        # 6. 100 shadow-write events recorded.
        shadow_log = writer_module.get_shadow_log()
        assert len(shadow_log) == 100, f"shadow events: {len(shadow_log)}"
        m_writer = writer_module.get_metrics()
        assert m_writer["writer.parse_failure"] == 0, m_writer

        # (B) SET EQUALITY on external_ids — the load-bearing claim.
        # Catches the failure mode where counts match but specific
        # records diverge (e.g. dedup asymmetry between paths).
        inline_external_ids = {
            r["external_id"] for r in await fresh_db.fetch(
                "SELECT external_id FROM observations WHERE tenant_id = $1",
                tenant_id,
            )
        }
        shadow_external_ids = {e.external_id for e in shadow_log}
        expected_external_ids = {
            _expected_external_id(p) for p in payloads
        }

        assert inline_external_ids == expected_external_ids, (
            f"inline path missing/extra external_ids:\n"
            f"  missing: {expected_external_ids - inline_external_ids}\n"
            f"  extra:   {inline_external_ids - expected_external_ids}"
        )
        assert shadow_external_ids == expected_external_ids, (
            f"shadow path missing/extra external_ids:\n"
            f"  missing: {expected_external_ids - shadow_external_ids}\n"
            f"  extra:   {shadow_external_ids - expected_external_ids}"
        )
        # The bottom-line ZERO-DIVERGENCE assertion.
        assert inline_external_ids == shadow_external_ids, (
            f"DIVERGENCE between inline and shadow paths:\n"
            f"  inline only: {inline_external_ids - shadow_external_ids}\n"
            f"  shadow only: {shadow_external_ids - inline_external_ids}"
        )

        # (C) Per-record content_hash present on the shadow side —
        # the writer's record set includes the hashes the M3 batched-
        # INSERT will use for idempotency. Cardinality is the load-
        # bearing property; values are inspected for shape.
        shadow_hashes = {e.content_hash for e in shadow_log}
        assert len(shadow_hashes) == 100  # all distinct
        for h in shadow_hashes:
            assert len(h) == 40 and all(c in "0123456789abcdef" for c in h)
