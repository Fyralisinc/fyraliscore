"""services/ingestion/embedding/publish.py — non-crashing publisher.

Per ingestion LLD §5.4. Called from the inline `ingest()` path after
an observation has been committed with `embedding_pending=TRUE`.

PRIME DIRECTIVE preserved (same as DLQ publish):
  A Kafka publish failure here MUST NOT roll back the observation
  insert or raise to the caller. The inline path has ALREADY
  committed; the observation is durable. If this publish fails:
    - The row stays at `embedding_pending=TRUE`.
    - The M3.3 backlog drainer scans for `embedding_pending=TRUE`
      directly from Postgres (NOT through Kafka). It will pick this
      row up regardless of Kafka availability.
  Kafka outages therefore become a latency issue (embedding waits for
  the drainer pass), not a correctness issue.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Callable
from uuid import UUID

import orjson

from services.ingestion.embedding.models import EmbeddingEnvelope


log = logging.getLogger(__name__)


_EMBEDDING_TOPIC = "ingestion.embedding"


async def publish_embedding_request(
    *,
    producer: Any,  # services.ingestion.kafka.IdempotentProducer
    tenant_id: UUID,
    source: str,
    observation_id: UUID,
    on_success: Callable[[], None] | None = None,
    on_failure: Callable[[], None] | None = None,
) -> None:
    """Publish one embedding-needed envelope. Never raises.

    Keyed by `tenant_id` so all of one tenant's embedding work lands
    on the same partition — keeps per-tenant ordering and lets the
    consumer-group rebalance assign whole-tenant work to one worker
    instance. (Per-observation keying would spread one tenant's work
    across partitions, which is fine for throughput but loses the
    "this tenant's queue is stuck on partition N" debuggability.)
    """
    try:
        env = EmbeddingEnvelope(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]
            observation_id=observation_id,
            enqueued_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    except Exception as exc:  # noqa: BLE001
        # Programmer error (e.g. bad source literal). Log + skip.
        if on_failure is not None:
            on_failure()
        log.warning(
            "embedding_publish.envelope_build_failed",
            extra={
                "tenant_id": str(tenant_id),
                "observation_id": str(observation_id),
                "error": str(exc)[:200],
            },
        )
        return

    try:
        await producer.produce(
            topic=_EMBEDDING_TOPIC,
            value=orjson.dumps(env.model_dump(mode="json")),
            key=str(tenant_id).encode("utf-8"),
        )
        if on_success is not None:
            on_success()
    except Exception as exc:  # noqa: BLE001 — PRIME DIRECTIVE
        if on_failure is not None:
            on_failure()
        log.warning(
            "embedding_publish.kafka_error",
            extra={
                "tenant_id": str(tenant_id),
                "observation_id": str(observation_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )


__all__ = ["publish_embedding_request"]
