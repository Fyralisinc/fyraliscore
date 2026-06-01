"""services/ingestion/progress/publisher.py
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
from typing import Any

from .events import ProgressEvent


log = logging.getLogger(__name__)


TOPIC_ONBOARDING_PROGRESS = "onboarding.progress"


async def publish_progress_event(
    kafka_producer: Any,  # services.ingestion.kafka.IdempotentProducer
    event: ProgressEvent,
) -> None:
    """Serialise `event` and publish to `onboarding.progress`.

    Returns when the message is in the producer's local queue, NOT
    when broker-ack lands. The N1 cursor-data ordering invariant
    (LLD §3.1) requires callers that need broker-ack to call
    `kafka_producer.flush(...)` BEFORE advancing any state row; see
    `services/ingestion/workflows/state.py::advance_cursor_atomic_with_kafka_publish`
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
    log.debug(
        "progress.event_published",
        extra={
            "event_kind": event.event_kind,
            "tenant_id": str(event.tenant_id),
        },
    )


__all__ = [
    "TOPIC_ONBOARDING_PROGRESS",
    "publish_progress_event",
]
