"""services/ingest/ingestion/progress/publisher.py
   — Publishes `onboarding.progress` events to Kafka.

Per ingestion LLD §6 (Bridge contract). The contract Bridge consumes
is the topic shape — partitioning by `tenant_id`, zstd compression,
30-day retention. This module owns the producer side of that contract.

============================================================
TOPIC + PARTITIONING (LLD §6)
============================================================
  topic: onboarding.progress
  partitions: 16          # one consumer per partition
  replication: 3
  retention: 30d
  cleanup_policy: delete
  compression: zstd
  key: tenant_id          # ensures per-tenant ordering

Per-tenant ordering matters because Bridge derives "revenue-at-risk"
state machines from the sequence of events; out-of-order delivery
within a tenant breaks the state machine. Partitioning by
`tenant_id.bytes` puts every tenant's events on the same partition,
which Kafka guarantees to be ordered.

============================================================
WHY THIS THIN WRAPPER, NOT INLINE produce() CALLS
============================================================
Three reasons:
  - One place that owns the topic name + key derivation. A future
    rename (`onboarding.progress` → `bridge.progress`) is a one-line
    change here, not a grep-and-fix across every workflow service.
  - One place where event-to-Pydantic validation happens. Callers
    pass a model instance; the publisher serialises. Garbage input
    can't reach Kafka.
  - Tests can swap in a capturing producer without monkey-patching
    `confluent_kafka.Producer`.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from services.app.gateway.product_workflow_metrics import record_product_workflow_event

from .events import ProgressEvent


log = logging.getLogger(__name__)


TOPIC_ONBOARDING_PROGRESS = "onboarding.progress"


async def publish_progress_event(
    kafka_producer: Any,  # services.ingest.ingestion.kafka.IdempotentProducer
    event: ProgressEvent,
) -> None:
    """Serialise `event` and publish to `onboarding.progress`.

    Returns when the message is in the producer's local queue, NOT
    when broker-ack lands. The N1 cursor-data ordering invariant
    (LLD §3.1) requires callers that need broker-ack to call
    `kafka_producer.flush(...)` BEFORE advancing any state row; see
    `services/ingest/ingestion/workflows/state.py::advance_cursor_atomic_with_kafka_publish`
    for the load-bearing primitive.

    Key = `tenant_id.bytes` (16 bytes); the LLD §6 topic config keys
    on `tenant_id` for per-tenant ordering.
    """
    payload = event.model_dump_json().encode("utf-8")
    key = event.tenant_id.bytes
    await kafka_producer.produce(
        topic=TOPIC_ONBOARDING_PROGRESS,
        value=payload,
        key=key,
    )
    _record_product_workflow_event(event)
    log.debug(
        "progress.event_published",
        extra={
            "event_kind": event.event_kind,
            "tenant_id": str(event.tenant_id),
        },
    )


async def publish_progress_events(
    kafka_producer: Any | None,  # services.ingest.ingestion.kafka.IdempotentProducer
    events: Iterable[ProgressEvent],
) -> None:
    """Publish a batch of progress events, tolerating an unwired producer.

    The orchestrators (TenantOnboarding / SourceOnboarding / Reconciler)
    take an OPTIONAL Kafka producer: it's present in production (wired by
    each `_run_*` CLI entrypoint) and absent in the many unit tests that
    construct a service only to exercise its signal/DB behaviour. When
    `kafka_producer is None` this is a no-op so those callers don't have
    to stand up a fake producer just to ignore it.

    Each event is enqueued via `publish_progress_event` (the per-tenant
    keyed wrapper). Callers MUST invoke this AFTER their DB transaction
    commits — the lifecycle transitions that produce these events are
    claim-via-UPDATE guarded, so a post-commit publish gives the
    at-least-once + Bridge-dedup contract the event models document. A
    publish that fails after the commit drops a progress (not a
    load-bearing) event; the transition itself is durable.
    """
    if kafka_producer is None:
        return
    for event in events:
        try:
            await publish_progress_event(kafka_producer, event)
        except Exception as exc:  # noqa: BLE001 — progress is non-load-bearing
            # Progress events are diagnostic (the Bridge UI's progress
            # bars), NOT load-bearing: the lifecycle transition that
            # emitted them has already committed (callers publish
            # post-commit, per the contract above). A publish failure —
            # a broker hiccup, or an `onboarding.progress` topic that the
            # broker hasn't provisioned (auto-create disabled) — MUST drop
            # the event, never propagate. Previously a KafkaException
            # (e.g. UNKNOWN_TOPIC) escaped here through `_terminate_shard`
            # and crashed the worker AFTER the shard's DB row was marked
            # 'done' but BEFORE the next shard, stranding the rest of the
            # backfill and preventing tenant completion. Best-effort per
            # event makes the durable transition the source of truth.
            log.warning(
                "progress.publish_failed",
                extra={
                    "event_kind": getattr(event, "event_kind", "?"),
                    "error": f"{type(exc).__name__}: {exc}"[:200],
                },
            )


def _record_product_workflow_event(event: ProgressEvent) -> None:
    if event.event_kind == "source.onboarding.started":
        record_product_workflow_event(
            workflow="source_onboarding",
            event="source_onboarding_started",
            outcome="success",
        )
    elif event.event_kind == "source.onboarding.complete":
        record_product_workflow_event(
            workflow="source_onboarding",
            event="source_onboarding_completed",
            outcome="success",
        )


__all__ = [
    "TOPIC_ONBOARDING_PROGRESS",
    "publish_progress_event",
    "publish_progress_events",
]
