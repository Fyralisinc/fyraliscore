"""Durable raw-tier emission used by the Source Connector host."""

from __future__ import annotations

import datetime as dt
import os
from typing import Any, Literal
from uuid import UUID

import orjson

from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope, SourceLiteral
from services.ingest.ingestion.raw_tier.s3 import (
    S3Client,
    build_raw_s3_key,
    compute_content_hash,
)

IngressKind = Literal["webhook", "gateway", "pubsub", "backfill", "poll"]
CUTOVER_FLUSH_TIMEOUT_SEC = float(
    os.environ.get("INGESTION_CUTOVER_FLUSH_TIMEOUT_SEC", "2.0")
)

_metrics: dict[str, int] = {
    "raw_emission.success": 0,
    "raw_emission.s3_put.attempts": 0,
    "raw_emission.kafka_publish.attempts": 0,
    "raw_emission.failure.s3": 0,
    "raw_emission.failure.kafka": 0,
}


def get_metrics() -> dict[str, int]:
    return dict(_metrics)


def reset_metrics() -> None:
    for key in _metrics:
        _metrics[key] = 0


async def emit_raw(
    *,
    tenant_id: UUID,
    source: SourceLiteral,
    ingress_kind: IngressKind,
    connector_installation_id: UUID | None = None,
    raw_body: bytes,
    s3_client: S3Client,
    kafka_producer: Any,
    ingress_metadata: dict[str, Any] | None = None,
    idem_hints: dict[str, str] | None = None,
    bucket: str = os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
    env: str = os.environ.get("INGESTION_ENV", "dev"),
    now: dt.datetime | None = None,
) -> str:
    """Write raw bytes to S3 and publish their durable Kafka pointer.

    Exceptions propagate: contract callers fail closed and must never advance
    a source cursor or acknowledge a gateway frame before this returns.
    """

    del bucket  # bucket ownership belongs to the injected S3 client
    observed_at = now or dt.datetime.now(tz=dt.UTC)
    content_hash = compute_content_hash(raw_body)
    key = build_raw_s3_key(
        env=env,
        source=source,
        tenant_id=tenant_id,
        ymd=observed_at.date(),
        content_hash=content_hash,
    )
    _metrics["raw_emission.s3_put.attempts"] += 1
    try:
        await s3_client.put_if_absent(key, raw_body)
    except Exception:
        _metrics["raw_emission.failure.s3"] += 1
        raise
    envelope = RawEnvelope(
        source=source,
        tenant_id=tenant_id,
        raw_s3_key=key,
        content_hash=content_hash,
        ingested_at=observed_at,
        ingress_kind=ingress_kind,
        connector_installation_id=connector_installation_id,
        ingress_metadata=ingress_metadata or {},
        idem_hints=idem_hints or {},
    )
    _metrics["raw_emission.kafka_publish.attempts"] += 1
    try:
        await kafka_producer.produce(
            topic=topic_for("raw", source),
            value=orjson.dumps(envelope.model_dump(mode="json")),
            key=str(tenant_id).encode(),
        )
    except Exception:
        _metrics["raw_emission.failure.kafka"] += 1
        raise
    _metrics["raw_emission.success"] += 1
    return key


__all__ = [
    "CUTOVER_FLUSH_TIMEOUT_SEC",
    "emit_raw",
    "get_metrics",
    "reset_metrics",
]
