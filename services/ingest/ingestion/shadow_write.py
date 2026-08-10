"""Raw-tier publisher retained for supplemental synchronous ingress routes.

The stable connector fleet emits raw evidence through host services. Instagram
and Facebook Pages still enter through their provider-specific gateway routes,
so this adapter applies the same durable S3-before-Kafka ordering and envelope
contract for those two sources.
"""

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

_DEFAULT_BUCKET = os.environ.get("S3_RAW_BUCKET", "fyralis-raw")
_DEFAULT_ENV = os.environ.get("INGESTION_ENV", "dev")
CUTOVER_FLUSH_TIMEOUT_SEC = float(
    os.environ.get("INGESTION_CUTOVER_FLUSH_TIMEOUT_SEC", "2.0")
)

IngressKind = Literal["webhook", "gateway", "pubsub", "backfill", "poll"]

_metrics: dict[str, int] = {
    "shadow_write.success": 0,
    "shadow_write.s3_put.attempts": 0,
    "shadow_write.kafka_publish.attempts": 0,
    "shadow_write.failure.s3": 0,
    "shadow_write.failure.kafka": 0,
    "shadow_write.failure.other": 0,
}


def get_metrics() -> dict[str, int]:
    return dict(_metrics)


def reset_metrics() -> None:
    for key in _metrics:
        _metrics[key] = 0


def _bump(key: str) -> None:
    _metrics[key] = _metrics.get(key, 0) + 1


async def shadow_write_raw(
    *,
    tenant_id: UUID,
    source: SourceLiteral,
    ingress_kind: IngressKind,
    raw_body: bytes,
    s3_client: S3Client,
    kafka_producer: Any,
    ingress_metadata: dict[str, Any] | None = None,
    idem_hints: dict[str, str] | None = None,
    bucket: str = _DEFAULT_BUCKET,
    env: str = _DEFAULT_ENV,
    now: dt.datetime | None = None,
) -> str:
    """Persist exact bytes, then publish their versioned raw pointer."""
    del bucket  # S3Client is already bound to its bucket.
    timestamp = now or dt.datetime.now(tz=dt.UTC)
    content_hash = compute_content_hash(raw_body)
    s3_key = build_raw_s3_key(
        env=env,
        source=source,
        tenant_id=tenant_id,
        ymd=timestamp.date(),
        content_hash=content_hash,
    )

    _bump("shadow_write.s3_put.attempts")
    try:
        await s3_client.put_if_absent(s3_key, raw_body)
    except Exception:
        _bump("shadow_write.failure.s3")
        raise

    envelope = RawEnvelope(
        source=source,
        tenant_id=tenant_id,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=timestamp,
        ingress_kind=ingress_kind,
        connector_version="supplemental-v1",
        parser_version="supplemental-provider-adapter-v1",
        ingress_metadata=ingress_metadata or {},
        idem_hints=idem_hints or {},
    )

    _bump("shadow_write.kafka_publish.attempts")
    try:
        await kafka_producer.produce(
            topic=topic_for("raw", source),
            value=orjson.dumps(envelope.model_dump(mode="json")),
            key=str(tenant_id).encode(),
        )
    except Exception:
        _bump("shadow_write.failure.kafka")
        raise

    _bump("shadow_write.success")
    return s3_key


__all__ = [
    "CUTOVER_FLUSH_TIMEOUT_SEC",
    "get_metrics",
    "reset_metrics",
    "shadow_write_raw",
]
