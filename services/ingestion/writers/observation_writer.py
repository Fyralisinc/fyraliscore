"""services/ingestion/writers/observation_writer.py — M2.4 no-op writer.

Per M2 work-order §M2.4 and LLD §5.2.

Consumes `ingestion.normalized` from M2.3's normalizer pool. In M2,
this writer is INTENTIONALLY A NO-OP — it logs each
NormalizedEnvelope and records a `ShadowWriteEvent` in an in-process
list. It does NOT INSERT into `observations`.

Why no-op in M2:
  - The inline path (`services.ingestion.core.ingest`) is the
    source of truth during the 48-hour zero-divergence soak.
  - Writing observations from the shadow path would mean two paths
    racing to claim the same `(source_channel, external_id)` slot
    via the unique index. Either path could legitimately win the
    insert; we want to PROVE the shadow path produces equivalent
    rows BEFORE we let it write.
  - The E2E test (`test_e2e_shadow.py`) asserts set-equality
    between inline-observation external_ids and shadow_log
    external_ids. Zero divergence = the shadow pipeline is
    correctness-equivalent and M3 can flip the writer to batched
    INSERT.

============================================================
PATH B CONTRACT
============================================================
This module MUST NOT import asyncpg or any DB-touching module.
The shadow log is in-process; M3's batched-INSERT version is a
DIFFERENT module that imports asyncpg and lives in Path A.

The static + runtime Path B proofs for the normalizer
(`test_worker_no_db_access.py`) apply structurally here too: the
writer imports the same Pydantic envelope model + aiokafka, and
nothing else.

DLQ note: M2.4 writer parses NormalizedEnvelope; a Pydantic
ValidationError on the wire format logs + bumps `parse_failure` +
COMMITS the offset (same prime directive as the normalizer's
EnvelopeInvariantError handling). M3 adds DLQ insert when the DB
pool is in scope.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer

from services.ingestion.dlq.publish import publish_dlq
from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingestion.normalizer.models import NormalizedEnvelope


log = logging.getLogger(__name__)


_NORMALIZED_TOPIC = "ingestion.normalized"
_WRITER_GROUP = "observation-writer"


# In-process metrics. M3 swaps to OTel Prometheus.
_metrics: dict[str, float] = {
    "writer.messages_consumed": 0.0,
    "writer.shadow_write_events": 0.0,
    "writer.parse_failure": 0.0,
    # M3.1 — DLQ publish metrics.
    "writer.dlq_publish.success": 0.0,
    "writer.dlq_publish.failure": 0.0,
    "writer.dlq_publish.skipped": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


@dataclass(frozen=True)
class ShadowWriteEvent:
    """One record the writer would INSERT in M3. The set of
    ShadowWriteEvents on a given test window must (per the E2E
    contract) have external_ids equal to the set of inline
    observations.external_id values for that window.
    """

    tenant_id: str
    source: str
    ingress_kind: str
    source_channel: str
    external_id: str | None
    content_hash: str
    raw_s3_key: str
    occurred_at: dt.datetime
    normalized_at: dt.datetime


# Module-global so test setup can `reset_shadow_log()` between cases.
# Concurrent appends are serialised through `_shadow_log_lock`.
_shadow_log: list[ShadowWriteEvent] = []
_shadow_log_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    """Lazily create the lock on the first event-loop touch.
    `asyncio.Lock()` constructed at module-import time would bind
    to whichever loop happened to be current then (often none).
    """
    global _shadow_log_lock
    if _shadow_log_lock is None:
        _shadow_log_lock = asyncio.Lock()
    return _shadow_log_lock


def get_shadow_log() -> list[ShadowWriteEvent]:
    """Snapshot copy of the shadow log. Test-friendly; production
    M3 swaps this entire module for one that writes to Postgres."""
    return list(_shadow_log)


def reset_shadow_log() -> None:
    _shadow_log.clear()


async def _record_event(env: NormalizedEnvelope) -> None:
    event = ShadowWriteEvent(
        tenant_id=str(env.tenant_id),
        source=env.source,
        ingress_kind=env.ingress_kind,
        source_channel=env.source_channel,
        external_id=env.external_id,
        content_hash=env.content_hash,
        raw_s3_key=env.raw_s3_key,
        occurred_at=env.occurred_at,
        normalized_at=env.normalized_at,
    )
    async with _get_lock():
        _shadow_log.append(event)
    _bump("writer.shadow_write_events")
    log.info(
        "writer.shadow_write_event",
        extra={
            "tenant_id": event.tenant_id,
            "source": event.source,
            "source_channel": event.source_channel,
            "external_id": event.external_id,
            # content_hash truncated — the full 40-char hex is
            # noise in operator logs; prefix is enough to grep.
            "content_hash_prefix": event.content_hash[:16],
        },
    )


@dataclass
class WriterConfig:
    """Configuration for one writer process."""

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _WRITER_GROUP
    # Stop after N events (test mode). Production = None.
    stop_after: int | None = None
    # M3.1 — producer config for DLQ publishes. Defaults are
    # LLD §5.2 (idempotent, zstd, acks=all).
    dlq_producer_config: ProducerConfig | None = None


async def run_writer(config: WriterConfig) -> dict[str, int]:
    """Writer's main loop. Returns a stats dict for tests.

    Path B preserved (NO Postgres pool here). M3.1 adds a Kafka
    producer for DLQ publishes — still Path B; the failure
    persistence is done by the separate DLQ writer process which IS
    Path A. This split is intentional (PRIME DIRECTIVE: failure-
    handling on the hot path must not introduce DB latency).
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    # M3.1 — producer for DLQ publishes.
    dlq_producer_cfg = config.dlq_producer_config or ProducerConfig(
        bootstrap_servers=config.bootstrap_servers,
        client_id=f"observation-writer-dlq-{id(config)}",
    )
    dlq_producer = IdempotentProducer(dlq_producer_cfg)

    await dlq_producer.start()
    await consumer.start()
    consumer.subscribe([_NORMALIZED_TOPIC])

    consumed = 0
    try:
        async for msg in consumer:
            consumed += 1
            _bump("writer.messages_consumed")
            try:
                env = NormalizedEnvelope.model_validate(
                    json.loads(msg.value)
                )
                await _record_event(env)
            except Exception as exc:  # noqa: BLE001
                _bump("writer.parse_failure")
                log.warning(
                    "writer.parse_failed",
                    extra={
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:200],
                    },
                )
                # M3.1 — publish to ingestion.dlq with
                # failure_kind="writer.invariant_failure". The
                # writer's invariant in M2 is "the message must
                # parse as a NormalizedEnvelope"; future M3+
                # writers may add invariant checks for downstream
                # writability (e.g. content_text non-empty for
                # the embedding worker). PRIME DIRECTIVE: a DLQ
                # publish failure here MUST NOT crash the worker.
                await publish_dlq(
                    producer=dlq_producer,
                    failure_kind="writer.invariant_failure",
                    error_summary=(
                        f"{type(exc).__name__}: {str(exc)[:200]}"
                    ),
                    msg_bytes=msg.value,
                    on_success=lambda: _bump("writer.dlq_publish.success"),
                    on_failure=lambda: _bump("writer.dlq_publish.failure"),
                    on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
                )
            await consumer.commit()
            if (
                config.stop_after is not None
                and consumed >= config.stop_after
            ):
                break
    finally:
        await consumer.stop()
        await dlq_producer.stop()

    return {"consumed": consumed}


def main() -> None:
    """Synchronous CLI entry."""
    logging.basicConfig(
        level=os.environ.get("WRITER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = WriterConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
    )
    asyncio.run(run_writer(config))


__all__ = [
    "ShadowWriteEvent",
    "WriterConfig",
    "get_metrics",
    "get_shadow_log",
    "main",
    "reset_metrics",
    "reset_shadow_log",
    "run_writer",
]
