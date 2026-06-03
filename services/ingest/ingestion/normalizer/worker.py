"""services/ingest/ingestion/normalizer/worker.py — per-process normalizer.

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
    - services.ingest.ingestion.core (which imports asyncpg)
    - services.domain.observations.repo (asyncpg)
    - any module that transitively pulls those in.

The normalizer's contract is pure: consume raw → fetch body → run
handler → publish normalized. No database.

Two complementary proofs in
`services/ingest/ingestion/normalizer/tests/test_worker_no_db_access.py`:
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
import time
from dataclasses import dataclass
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)

from services.ingest.ingestion.dlq.publish import publish_dlq
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.kafka.topics import (
    consumer_group,
    subscribe_topics,
    topic_for,
)
from services.ingest.ingestion.kafka.shutdown import install_shutdown_event, next_or_stop
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.normalizer.invariants import (
    EnvelopeInvariantError,
    assert_envelope_invariants,
)
from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
from services.ingest.ingestion.raw_tier.s3 import S3Client


log = logging.getLogger(__name__)


_RAW_TOPIC = "ingestion.raw"
_NORMALIZED_TOPIC = "ingestion.normalized"
_DLQ_TOPIC = "ingestion.dlq"
_CONSUMER_GROUP = "normalizer"


# ---- In-process metrics ----
# Per M2 work-order metric list (M3+ swaps to OTel Prometheus).
_metrics: dict[str, float] = {
    "normalizer.messages_consumed": 0.0,
    "normalizer.messages_produced": 0.0,
    "normalizer.parse_failure": 0.0,
    "normalizer.invariant_failure": 0.0,
    "normalizer.unsupported_combination": 0.0,
    "normalizer.transform_duration_ms_sum": 0.0,
    "normalizer.transform_duration_ms_count": 0.0,
    "normalizer.consumer_lag_seconds_last": 0.0,
    # M3.1 — DLQ publish metrics. Failures here MUST NOT crash the
    # worker (PRIME DIRECTIVE preserved); they're tracked for ops
    # to detect a broken DLQ path.
    "normalizer.dlq_publish.success": 0.0,
    "normalizer.dlq_publish.failure": 0.0,
    "normalizer.dlq_publish.skipped":  0.0,
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
    # Source isolation: when set (e.g. "slack"), this worker subscribes
    # ONLY to ingestion.raw.<source> and joins consumer group
    # "<consumer_group>.<source>" so its lag/offsets are independent of
    # every other source. When None, it subscribes to ALL per-source raw
    # topics under the bare group (dev / single-process fallback).
    source: str | None = None
    # S3 raw-tier connection.
    s3_endpoint_url: str | None = None  # None → real AWS
    s3_bucket: str = "fyralis-raw"
    s3_region_name: str = "auto"
    # Stop after N envelopes (test mode). Production sets to None.
    stop_after: int | None = None
    # Source isolation / intra-source fairness: how many tenants' message
    # groups to process concurrently within a batch. Default 1 = the
    # historical strictly-serial loop (one message, await S3 GET, produce,
    # commit). >1 switches to a batched loop that overlaps S3 GETs across
    # tenants while preserving per-tenant ordering, so one tenant's slow
    # S3 fetch no longer head-of-line blocks another tenant on the same
    # source lane. See docs/ingestion/source-isolation.md.
    max_concurrency: int = 1
    # getmany poll timeout for the concurrent loop (ignored when
    # max_concurrency == 1).
    poll_timeout_ms: int = 500
    # Idempotent producer; LLD §5.2 defaults if omitted.
    producer_config: ProducerConfig | None = None
    # Sticky partition assignment is aiokafka's nearest analogue to
    # the LLD §5.2 "cooperative-sticky" contract: rebalances move
    # partitions incrementally rather than stop-the-world, and the
    # strategy minimises reassignment during membership changes.
    # (aiokafka 0.14.x exposes Sticky; not a separate CooperativeSticky
    # class.) Tests can override.
    partition_assignment_strategy: tuple = (StickyPartitionAssignor,)
    # Optional rebalance listener. The cooperative-sticky rebalance
    # test passes a recorder so it can assert rebalance events
    # actually fired during the workload (not just that the workload
    # finished). Production leaves this None — aiokafka logs at INFO.
    rebalance_listener: ConsumerRebalanceListener | None = None


async def run_worker(config: WorkerConfig) -> dict[str, int]:
    """One worker's main loop. Returns a stats dict (`consumed`,
    `produced`) — used by tests; production discards.

    Exit conditions:
      - `config.stop_after` envelopes consumed (test mode).
      - SIGTERM / SIGINT received.
      - Unhandled exception bubbles up; supervisor restarts.

    This function NEVER touches asyncpg / a Postgres pool. Path B.
    """
    # Construct WITHOUT topic so we can call subscribe(...) below
    # with an optional listener. Constructor-subscription doesn't
    # support listeners (the listener arg lives on subscribe()).
    raw_topics = subscribe_topics("raw", config.source)
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=consumer_group(config.consumer_group, config.source),
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
    # Subscribe AFTER start(); listener (if any) records rebalance
    # events for the cooperative-sticky test.
    if config.rebalance_listener is not None:
        consumer.subscribe(raw_topics, listener=config.rebalance_listener)
    else:
        consumer.subscribe(raw_topics)
    await s3.connect()

    consumed = 0
    produced = 0

    # Ticket #45: a SIGTERM/SIGINT sets this event and the racing
    # `next_or_stop` returns None, breaking the loop into its normal
    # teardown (rc=0) instead of dying mid-poll (rc=-15/-9).
    stop_event = install_shutdown_event()

    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))

    try:
        if config.max_concurrency <= 1:
            # ---- Serial path (historical, default) ----
            # One message at a time: await S3 GET, normalize, produce,
            # commit. Behaviourally identical to the pre-concurrency loop.
            while True:
                msg = await next_or_stop(consumer, stop_event)
                if msg is None:
                    break

                consumed += 1
                _bump("normalizer.messages_consumed")
                _record_lag(msg)

                if await _process_message(msg, s3, producer):
                    produced += 1
                    _bump("normalizer.messages_produced")

                # Commit AFTER processing — at-least-once semantics.
                await consumer.commit()

                if (
                    config.stop_after is not None
                    and consumed >= config.stop_after
                ):
                    break
        else:
            # ---- Concurrent path (max_concurrency > 1) ----
            # Pull a batch, group by tenant (the Kafka message key IS the
            # tenant_id), process tenant groups CONCURRENTLY (their S3 GETs
            # overlap) but messages within a tenant SERIALLY (preserves
            # per-tenant ordering). Commit the whole batch after all groups
            # complete — at-least-once preserved; reprocessing produces
            # duplicate normalized messages that the observation-writer's
            # unique index dedups. Concurrency bounded by max_concurrency.
            sem = asyncio.Semaphore(config.max_concurrency)

            async def _process_group(group: list[Any]) -> int:
                produced_in_group = 0
                async with sem:
                    for m in group:
                        if await _process_message(m, s3, producer):
                            produced_in_group += 1
                return produced_in_group

            while not stop_event.is_set():
                batches = await consumer.getmany(
                    timeout_ms=config.poll_timeout_ms,
                )
                messages: list[Any] = []
                for partition_msgs in batches.values():
                    messages.extend(partition_msgs)
                if not messages:
                    if (
                        config.stop_after is not None
                        and consumed >= config.stop_after
                    ):
                        break
                    continue

                consumed += len(messages)
                _bump("normalizer.messages_consumed", float(len(messages)))
                for m in messages:
                    _record_lag(m)

                # Group by tenant (message key). Insertion order within a
                # key preserves the partition's delivery order for that
                # tenant.
                groups: dict[bytes, list[Any]] = {}
                for m in messages:
                    groups.setdefault(m.key or b"", []).append(m)

                counts = await asyncio.gather(
                    *(_process_group(g) for g in groups.values())
                )
                total_produced = sum(counts)
                produced += total_produced
                if total_produced:
                    _bump("normalizer.messages_produced", float(total_produced))

                await consumer.commit()

                if (
                    config.stop_after is not None
                    and consumed >= config.stop_after
                ):
                    break
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        await consumer.stop()
        await producer.stop()
        await s3.close()

    return {"consumed": consumed, "produced": produced}


def _record_lag(msg: Any) -> None:
    """Update the consumer-lag gauge from a message timestamp."""
    if msg.timestamp:
        lag_s = max(0.0, (time.time() * 1000 - msg.timestamp) / 1000.0)
        _metrics["normalizer.consumer_lag_seconds_last"] = lag_s


async def _process_message(
    msg: Any,
    s3: S3Client,
    producer: IdempotentProducer,
) -> bool:
    """Normalize one raw message and publish to `ingestion.normalized.<source>`.

    Returns True iff a normalized envelope was produced. On parse/invariant
    failure, publishes a best-effort DLQ envelope and returns False. NEVER
    raises (PRIME DIRECTIVE): a single bad message must not stall or crash
    the consumer. Bumps transform-duration + failure metrics. Shared by the
    serial and concurrent loops in `run_worker`.
    """
    t0 = time.monotonic()
    last_envelope: RawEnvelope | None = None
    last_msg_bytes = msg.value
    produced = False
    try:
        # _normalize_one_with_envelope parses the envelope FIRST so the
        # invariant-failure branch has it available for the DLQ publish.
        envelope_or_none, produced = await _normalize_one_with_envelope(
            msg.value, s3, producer,
        )
        last_envelope = envelope_or_none
    except EnvelopeInvariantError as exc:
        # M2.4 PRIME DIRECTIVE: invariant failures are parse-failure-class.
        # Log + metric + DLQ + CONTINUE. Never propagate.
        _bump("normalizer.invariant_failure")
        _bump("normalizer.parse_failure")
        log.warning(
            "normalizer.invariant_failure",
            extra={
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "error": str(exc)[:200],
            },
        )
        _env = getattr(exc, "envelope", None) or last_envelope
        await publish_dlq(
            producer=producer,
            failure_kind="normalizer.invariant_failure",
            error_summary=str(exc)[:500],
            tenant_id=(_env.tenant_id if _env is not None else None),
            source=(_env.source if _env is not None else None),
            raw_s3_key=(_env.raw_s3_key if _env is not None else None),
            msg_bytes=last_msg_bytes,
            on_success=lambda: _bump("normalizer.dlq_publish.success"),
            on_failure=lambda: _bump("normalizer.dlq_publish.failure"),
            on_skipped=lambda: _bump("normalizer.dlq_publish.skipped"),
        )
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
        # Best-effort DLQ publish. Byte garbage with no extractable
        # (tenant_id, source) is skipped (would violate the
        # ingestion_failures CHECK constraint anyway).
        await publish_dlq(
            producer=producer,
            failure_kind="normalizer.parse_failure",
            error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
            tenant_id=(
                last_envelope.tenant_id if last_envelope is not None else None
            ),
            source=(
                last_envelope.source if last_envelope is not None else None
            ),
            raw_s3_key=(
                last_envelope.raw_s3_key if last_envelope is not None else None
            ),
            msg_bytes=last_msg_bytes,
            on_success=lambda: _bump("normalizer.dlq_publish.success"),
            on_failure=lambda: _bump("normalizer.dlq_publish.failure"),
            on_skipped=lambda: _bump("normalizer.dlq_publish.skipped"),
        )
    finally:
        duration_ms = (time.monotonic() - t0) * 1000.0
        _bump("normalizer.transform_duration_ms_sum", duration_ms)
        _bump("normalizer.transform_duration_ms_count")
    return produced


async def _normalize_one(
    envelope_bytes: bytes,
    s3: S3Client,
    producer: IdempotentProducer,
) -> bool:
    """Backwards-compatible wrapper around `_normalize_one_with_envelope`.

    Kept for tests that depend on the single-return shape — internally
    delegates to the two-tuple variant which the outer loop uses.
    """
    _envelope, produced = await _normalize_one_with_envelope(
        envelope_bytes, s3, producer,
    )
    return produced


async def _normalize_one_with_envelope(
    envelope_bytes: bytes,
    s3: S3Client,
    producer: IdempotentProducer,
) -> tuple[RawEnvelope | None, bool]:
    """Process one raw envelope. Returns (envelope, produced):

      - envelope: the parsed RawEnvelope, IF parse succeeded (so the
        outer loop's DLQ publish on invariant failure has the full
        fields). Never raises with `envelope` populated unless the
        invariant check failed.
      - produced: True if a normalized envelope was published; False
        if the (source, ingress_kind) was unsupported.

    Raises on any other error; the caller catches + records
    `parse_failure` AND publishes a best-effort DLQ envelope.

    Pure transform — no database. Path B.
    """
    envelope = RawEnvelope.model_validate(orjson.loads(envelope_bytes))

    # M2.4 — post-validation cross-field invariants. Raises
    # EnvelopeInvariantError (ValueError subclass) which the outer
    # loop catches, logs, metrics, and commits (PRIME DIRECTIVE).
    # M3.1 — on raise, the outer loop also publishes a DLQ envelope.
    # We attach the parsed envelope to the exception so the helper
    # can construct the DLQ envelope without re-parsing.
    try:
        assert_envelope_invariants(envelope)
    except EnvelopeInvariantError as exc:
        exc.envelope = envelope  # type: ignore[attr-defined]
        raise

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
        return envelope, False

    # Fetch the raw body from S3 (the only network call in this hot
    # path besides Kafka).
    raw_body = await s3.get(envelope.raw_s3_key)
    payload = orjson.loads(raw_body)

    # M6.7 (A27.3) — the backfill producer (shard_fetch) wraps the
    # handler body in a blob `{record, shard_context, webhook_metadata}`
    # so it can carry the webhook-equivalent headers a handler needs
    # (e.g. X-GitHub-Event) without a webhook signature. Unwrap it here:
    # the handler then sees the SAME (body, headers) shape webhook
    # routing would provide, so it derives the SAME external_id (parity,
    # HLD §02 L278). The live webhook/gateway/pubsub paths publish the
    # bare body with no wrapper, so they keep headers={}.
    headers: dict[str, str] = {}
    if envelope.ingress_kind == "backfill" and isinstance(payload, dict):
        headers = payload.get("webhook_metadata") or {}
        payload = payload.get("record", payload)
    elif envelope.source == "github":
        # Live-via-Kafka github (ingress_kind="webhook"): the handler keys
        # the event on the `X-GitHub-Event` header, NOT the body. The
        # webhook-router cutover (and any live producer) records the event
        # type in `ingress_metadata["event_type"]`; reconstruct the header
        # here so the live cutover path derives the SAME draft the inline
        # ingest() would (which received the real header). Backfill carries
        # it via webhook_metadata above; other sources read the body and
        # ignore headers.
        event_type = envelope.ingress_metadata.get("event_type")
        if event_type:
            headers = {"X-GitHub-Event": event_type}

    # Dispatch — the handler is a pure (payload, headers) → draft
    # function. For live ingress, headers={} (the verified-at-ingress
    # info is already in `envelope.ingress_metadata`); for backfill,
    # headers carry the replayed webhook_metadata (A27.3).
    handler = get_handler(channel)
    draft = await handler(payload, headers)

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
        # Per-source normalized topic — keeps the observation-writer lanes
        # independent per source (source-isolation.md).
        topic=topic_for("normalized", envelope.source),
        value=orjson.dumps(normalized.model_dump(mode="json")),
        key=str(envelope.tenant_id).encode("utf-8"),
    )
    return envelope, True


# DLQ publish lives in services.ingest.ingestion.dlq.publish (shared with
# the no-op writer). Per M3.1 — the helper preserves the PRIME
# DIRECTIVE: a Kafka publish failure on the DLQ topic must NOT crash
# the worker; failures surface via `normalizer.dlq_publish.failure`.


def main() -> None:
    """Synchronous CLI entry — wraps run_worker in asyncio.run.

    Reads connection details from env (KAFKA_BOOTSTRAP_SERVERS,
    S3_ENDPOINT_URL, S3_RAW_BUCKET, S3_REGION_NAME). Used by:
      - the supervisor (spawned child processes).
      - `python -m services.ingest.ingestion.normalizer --single-worker`
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
        # Source isolation: INGESTION_SOURCE pins this process to one
        # source's lane. Unset → all-sources fallback (dev/sandbox).
        source=os.environ.get("INGESTION_SOURCE") or None,
        # Intra-source fairness: >1 overlaps S3 GETs across tenants
        # (per-tenant order preserved). Default 1 = serial.
        max_concurrency=int(os.environ.get("NORMALIZER_MAX_CONCURRENCY", "1")),
    )
    asyncio.run(run_worker(config))


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    main()


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
