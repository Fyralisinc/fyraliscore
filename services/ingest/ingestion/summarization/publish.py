"""Best-effort publisher for large-document summarization requests."""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Callable
from uuid import UUID

import orjson

from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.ingestion.raw_tier.envelope import IngressKindLiteral
from services.ingest.ingestion.summarization.models import SummarizationEnvelope


log = logging.getLogger(__name__)


async def publish_summarization_request(
    *,
    producer: Any,
    tenant_id: UUID,
    source: str,
    observation_id: UUID,
    raw_s3_key: str | None = None,
    ingress_kind: IngressKindLiteral | None = None,
    on_success: Callable[[], None] | None = None,
    on_failure: Callable[[], None] | None = None,
) -> None:
    """Publish one summarization-needed envelope. Never raises."""
    try:
        env = SummarizationEnvelope(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]
            observation_id=observation_id,
            raw_s3_key=raw_s3_key,
            ingress_kind=ingress_kind,
            enqueued_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    except Exception as exc:  # noqa: BLE001
        if on_failure is not None:
            on_failure()
        log.warning(
            "summarization_publish.envelope_build_failed",
            extra={
                "tenant_id": str(tenant_id),
                "observation_id": str(observation_id),
                "error": str(exc)[:200],
            },
        )
        return

    try:
        await producer.produce(
            topic=topic_for("summarization", source),
            value=orjson.dumps(env.model_dump(mode="json")),
            key=str(tenant_id).encode("utf-8"),
        )
        if on_success is not None:
            on_success()
    except Exception as exc:  # noqa: BLE001
        if on_failure is not None:
            on_failure()
        log.warning(
            "summarization_publish.kafka_error",
            extra={
                "tenant_id": str(tenant_id),
                "observation_id": str(observation_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )


__all__ = ["publish_summarization_request"]
