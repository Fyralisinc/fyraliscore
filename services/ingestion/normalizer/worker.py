"""services/ingestion/normalizer/worker.py — per-process normalizer.

Per ingestion LLD §5.2 and M2 work-order §M2.3.

The worker's job, in three steps:

    1. Consume an envelope from `ingestion.raw` (Kafka).
    2. Fetch the raw body from S3 via `envelope.raw_s3_key`.
    3. Dispatch the body through the existing handler registry to
       obtain an `ObservationDraft`, wrap it in a `NormalizedEnvelope`,
       and publish to `ingestion.normalized`.

============================================================
CRITICAL — PATH B INVARIANT
============================================================
This module MUST NOT import:
    - asyncpg (or any asyncpg.*)
    - lib.shared.tenant_context
    - services.ingestion.core (which imports asyncpg)
    - services.observations.repo (asyncpg)
    - any module that transitively pulls those in.

The normalizer's contract is pure: consume raw → fetch body → run
handler → publish normalized. No database.

Two complementary proofs in
`services/ingestion/normalizer/tests/test_worker_no_db_access.py`:
  - Static: import graph from this module shows no DB modules.
  - Runtime: asyncpg's user-facing API is tripwired during a
    synthetic load of N envelopes; the tripwire must NEVER fire.

If you add a feature here that needs the database, you are off
Path B and should escalate to the M3 design conversation. Do not
silently add an asyncpg import.

DLQ note: parse failures log + bump `parse_failure` metric but do
NOT write to `ingestion_failures`. The DLQ writer requires a DB
pool which lands in M3 (per M2 work-order "What is NOT done" §M2.3).

============================================================
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)

from services.ingestion.handlers import HandlerNotFound, get_handler
from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.raw_tier.envelope import RawEnvelope
from services.ingestion.raw_tier.s3 import S3Client


log = logging.getLogger(__name__)


_RAW_TOPIC = "ingestion.raw"
_NORMALIZED_TOPIC = "ingestion.normalized"
_CONSUMER_GROUP = "normalizer"


# ---- In-process metrics ----
# Per M2 work-order metric list (M3+ swaps to OTel Prometheus).
_metrics: dict[str, float] = {
    "normalizer.messages_consumed": 0.0,
    "normalizer.messages_produced": 0.0,
    "normalizer.parse_failure": 0.0,
    "normalizer.unsupported_combination": 0.0,
    "normalizer.transform_duration_ms_sum": 0.0,
    "normalizer.transform_duration_ms_count": 0.0,
    "normalizer.consumer_lag_seconds_last": 0.0,
}


def get_metrics() -> dict[str, float]:
    """Snapshot of in-process counters. Test-friendly."""
    return dict(_metrics)


def reset_metrics() -> None:
    """Clear all counters. Test-only."""
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


@dataclass
class WorkerConfig:
    """Configuration for one normalizer worker process."""

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _CONSUMER_GROUP
    # S3 raw-tier connection.
    s3_endpoint_url: str | None = None  # None → real AWS
    s3_bucket: str = "fyralis-raw"
    s3_region_name: str = "auto"
    # Stop after N envelopes (test mode). Production sets to None.
    stop_after: int | None = None
    # Idempotent producer; LLD §5.2 defaults if omitted.
    producer_config: ProducerConfig | None = None
    # Sticky partition assignment is aiokafka's nearest analogue to
    # the LLD §5.2 "cooperative-sticky" contract: rebalances move
    # partitions incrementally rather than stop-the-world, and the
    # strategy minimises reassignment during membership changes.
    # (aiokafka 0.14.x exposes Sticky; not a separate CooperativeSticky
    # class.) Tests can override.
    partition_assignment_strategy: tuple = (StickyPartitionAssignor,)


async def run_worker(config: WorkerConfig) -> dict[str, int]:
    """One worker's main loop. Returns a stats dict (`consumed`,
    `produced`) — used by tests; production discards.

    Exit conditions:
      - `config.stop_after` envelopes consumed (test mode).
      - SIGTERM / SIGINT received.
      - Unhandled exception bubbles up; supervisor restarts.

    This function NEVER touches asyncpg / a Postgres pool. Path B.
    """
    consumer = AIOKafkaConsumer(
        _RAW_TOPIC,
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        partition_assignment_strategy=config.partition_assignment_strategy,
        max_poll_interval_ms=300_000,
    )
    producer_cfg = config.producer_config or ProducerConfig(
        bootstrap_servers=config.bootstrap_servers,
        client_id=f"normalizer-{os.getpid()}",
    )
    producer = IdempotentProducer(producer_cfg)
    s3 = S3Client(
        config.s3_bucket,
        endpoint_url=config.s3_endpoint_url,
        region_name=config.s3_region_name,
    )

    await producer.start()
    await consumer.start()
    await s3.connect()

    consumed = 0
    produced = 0
    stop = False

    def _handle_signal(*_args: Any) -> None:
        nonlocal stop
        stop = True

    # signal.signal() only works on the main thread; in worker
    # processes started by the supervisor this IS the main thread.
    # In test harnesses we may run off-main-thread, so guard.
    try:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    except (ValueError, OSError):
        pass

    try:
        async for msg in consumer:
            if stop:
                break

            consumed += 1
            _bump("normalizer.messages_consumed")

            if msg.timestamp:
                lag_s = max(
                    0.0, (time.time() * 1000 - msg.timestamp) / 1000.0
                )
                _metrics["normalizer.consumer_lag_seconds_last"] = lag_s

            t0 = time.monotonic()
            try:
                produced_one = await _normalize_one(msg.value, s3, producer)
                if produced_one:
                    produced += 1
                    _bump("normalizer.messages_produced")
            except Exception as exc:  # noqa: BLE001 — record + skip
                _bump("normalizer.parse_failure")
                log.warning(
                    "normalizer.transform_failed",
                    extra={
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:200],
                    },
                )
                # NOTE: M2.3 does NOT write to ingestion_failures.
                # The DLQ writer needs a Postgres pool which is Path A
                # territory; lands in M3. Per M2 work-order
                # "What is NOT done" §M2.3.
            finally:
                duration_ms = (time.monotonic() - t0) * 1000.0
                _bump("normalizer.transform_duration_ms_sum", duration_ms)
                _bump("normalizer.transform_duration_ms_count")

            # Commit AFTER processing — at-least-once semantics. M3
            # may layer txn-commit on top; M2 is the simplest correct
            # shape.
            await consumer.commit()

            if (
                config.stop_after is not None
                and consumed >= config.stop_after
            ):
                break
    finally:
        await consumer.stop()
        await producer.stop()
        await s3.close()

    return {"consumed": consumed, "produced": produced}


async def _normalize_one(
    envelope_bytes: bytes,
    s3: S3Client,
    producer: IdempotentProducer,
) -> bool:
    """Process one raw envelope. Returns True if a normalized envelope
    was produced; False if the (source, ingress_kind) was unsupported
    (out-of-scope for M2). Raises on any other error; the caller
    catches + records `parse_failure`.

    Pure transform — no database. Path B.
    """
    envelope = RawEnvelope.model_validate(orjson.loads(envelope_bytes))

    channel = resolve_channel(envelope.source, envelope.ingress_kind)
    if channel is None:
        _bump("normalizer.unsupported_combination")
        log.info(
            "normalizer.unsupported_combination",
            extra={
                "source": envelope.source,
                "ingress_kind": envelope.ingress_kind,
                "reason": "no_handler_in_m2_scope",
                "raw_s3_key": envelope.raw_s3_key,
            },
        )
        return False

    # Fetch the raw body from S3 (the only network call in this hot
    # path besides Kafka).
    raw_body = await s3.get(envelope.raw_s3_key)
    payload = orjson.loads(raw_body)

    # Dispatch — the handler is a pure (payload, headers) → draft
    # function. headers={} because the verified-at-ingress info is
    # already in `envelope.ingress_metadata`; M3+ may pass a subset
    # through if a handler needs it.
    handler = get_handler(channel)
    draft = await handler(payload, {})

    normalized = NormalizedEnvelope(
        envelope_version=1,
        source=envelope.source,
        ingress_kind=envelope.ingress_kind,
        tenant_id=envelope.tenant_id,
        raw_s3_key=envelope.raw_s3_key,
        content_hash=envelope.content_hash,
        raw_ingested_at=envelope.ingested_at,
        source_channel=draft.source_channel,
        content_text=draft.content_text,
        content=draft.content,
        occurred_at=draft.occurred_at,
        trust_tier=draft.trust_tier,
        kind=draft.kind,
        source_actor_ref=draft.source_actor_ref,
        external_id=draft.external_id,
        entities_hint=draft.entities_hint,
        normalized_at=dt.datetime.now(tz=dt.timezone.utc),
        ingress_metadata=envelope.ingress_metadata,
        idem_hints=envelope.idem_hints,
    )
    await producer.produce(
        topic=_NORMALIZED_TOPIC,
        value=orjson.dumps(normalized.model_dump(mode="json")),
        key=str(envelope.tenant_id).encode("utf-8"),
    )
    return True


def main() -> None:
    """Synchronous CLI entry — wraps run_worker in asyncio.run.

    Reads connection details from env (KAFKA_BOOTSTRAP_SERVERS,
    S3_ENDPOINT_URL, S3_RAW_BUCKET, S3_REGION_NAME). Used by:
      - the supervisor (spawned child processes).
      - `python -m services.ingestion.normalizer --single-worker`
        for local debugging.
    """
    logging.basicConfig(
        level=os.environ.get("NORMALIZER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = WorkerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        s3_region_name=os.environ.get("S3_REGION_NAME", "auto"),
    )
    asyncio.run(run_worker(config))


# Re-export the channel resolver as a module attribute so the
# Path B static proof can introspect it without importing the
# private module. Convenience-only; not a public API.
__all__ = [
    "WorkerConfig",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_worker",
]
