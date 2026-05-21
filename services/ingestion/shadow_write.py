"""Centralised shadow-write coordinator for the M2 raw tier.

Per M2 work-order §M2.1 and §M2.2. All three ingress surfaces
(webhook, gateway, pubsub) call `shadow_write_raw(...)` to:

    1. Compute the content hash of the raw body.
    2. Build the S3 key per LLD §5.1.
    3. PutIfAbsent the body to S3 (idempotent via content_hash).
    4. Build the `RawEnvelope` (M1.4) with the right `ingress_kind`.
    5. Publish the envelope to `ingestion.raw` keyed by tenant_id.

PRIME DIRECTIVE (M2 work order):
    "The shadow path is best-effort; if it fails for any reason, the
    inline response must still be 200 OK with the observation
    written. Failure of the shadow path is logged but does not
    propagate to the user."

Every codepath in this module that raises is caught at the boundary
by the caller's `try/except` — see the call sites in the webhook
router, the Discord gateway worker, and the Gmail Pub/Sub endpoint.
The module itself does NOT swallow exceptions; the caller does. This
keeps stack traces useful when the safety mechanism is being tested.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Literal
from uuid import UUID

import orjson

from services.ingestion.raw_tier.envelope import RawEnvelope
from services.ingestion.raw_tier.s3 import (
    S3Client,
    build_raw_s3_key,
    compute_content_hash,
)


log = logging.getLogger(__name__)


# Per LLD §5.1 the bucket prefix is `s3://fyralis-raw/...`; the env
# override lets dev runs point at moto without changing code.
_DEFAULT_BUCKET = os.environ.get("S3_RAW_BUCKET", "fyralis-raw")
_DEFAULT_ENV = os.environ.get("INGESTION_ENV", "dev")
_RAW_TOPIC = "ingestion.raw"


IngressKind = Literal["webhook", "gateway", "pubsub", "backfill", "poll"]


# In-process counters. M3 will replace these with the real metrics
# pipeline (OTel + Prometheus exporters per LLD §5.2). For M2 they
# are best-effort attempts that anybody can inspect via the
# `get_metrics()` helper below.
_metrics: dict[str, int] = {
    "shadow_write.success": 0,
    "shadow_write.s3_put.attempts": 0,
    "shadow_write.kafka_publish.attempts": 0,
    "shadow_write.failure.s3": 0,
    "shadow_write.failure.kafka": 0,
    "shadow_write.failure.other": 0,
}


def get_metrics() -> dict[str, int]:
    """Return a snapshot of the in-process counters. Test-friendly."""
    return dict(_metrics)


def reset_metrics() -> None:
    """Clear all counters. Test-only."""
    for k in _metrics:
        _metrics[k] = 0


def _bump(key: str, by: int = 1) -> None:
    _metrics[key] = _metrics.get(key, 0) + by


async def shadow_write_raw(
    *,
    tenant_id: UUID,
    source: Literal["slack", "github", "discord", "gmail"],
    ingress_kind: IngressKind,
    raw_body: bytes,
    s3_client: S3Client,
    kafka_producer: Any,  # services.ingestion.kafka.IdempotentProducer
    ingress_metadata: dict[str, Any] | None = None,
    idem_hints: dict[str, str] | None = None,
    bucket: str = _DEFAULT_BUCKET,
    env: str = _DEFAULT_ENV,
    now: dt.datetime | None = None,
) -> str:
    """Perform one shadow write: S3 PutIfAbsent + Kafka publish.

    Returns the S3 key of the written body (also embedded in the
    envelope's `raw_s3_key`). Callers may log this for traceability;
    failure to log it has no functional impact.

    Raises whatever the S3 client / Kafka producer raises on failure.
    Callers MUST wrap the call in `try/except Exception` per the
    M2 prime directive.

    Parameters:
      tenant_id        — UUID; partition key for Kafka and S3 prefix.
      source           — webhook/gateway/pubsub know the source by
                         construction (verified upstream).
      ingress_kind     — "webhook" / "gateway" / "pubsub" — used by
                         the normalizer to dispatch to the right
                         handler shape (e.g. webhook payloads have
                         JSON envelopes that gateway frames don't).
      raw_body         — exact bytes received. NOT re-serialized;
                         the content_hash depends on byte-equality.
      s3_client        — connected M1.4 S3Client (caller owns
                         lifecycle).
      kafka_producer   — started IdempotentProducer (M2.1).
      ingress_metadata — free-form per-source dict (delivery_id,
                         event_type, shard_id, cursor_token …).
      idem_hints       — free-form per-source dict (forecast of
                         the external_id prefix; used by the
                         normalizer's idempotency-key constructor).
      bucket / env     — override S3 bucket / env prefix; defaults
                         come from S3_RAW_BUCKET / INGESTION_ENV env.
      now              — inject for testing; defaults to UTC now.
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    content_hash = compute_content_hash(raw_body)
    s3_key = build_raw_s3_key(
        env=env,
        source=source,
        tenant_id=tenant_id,
        ymd=now.date(),
        content_hash=content_hash,
    )

    # ---- 1. S3 PutIfAbsent ----
    # We do NOT compress here — the raw bytes go to S3 as-is. M5+'s
    # backfill path zstd-compresses before PUT (LLD §5.1), but for
    # the shadow webhook/gateway/pubsub path the bodies are small
    # (<10 KB typical) and avoiding compression keeps the shadow
    # codepath as cheap as possible.
    _bump("shadow_write.s3_put.attempts")
    try:
        await s3_client.put_if_absent(s3_key, raw_body)
    except Exception:
        _bump("shadow_write.failure.s3")
        raise

    # ---- 2. Build envelope ----
    envelope = RawEnvelope(
        source=source,
        tenant_id=tenant_id,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=now,
        ingress_kind=ingress_kind,
        ingress_metadata=ingress_metadata or {},
        idem_hints=idem_hints or {},
    )
    envelope_bytes = orjson.dumps(envelope.model_dump(mode="json"))

    # ---- 3. Kafka publish (keyed by tenant for partition affinity) ----
    _bump("shadow_write.kafka_publish.attempts")
    try:
        await kafka_producer.produce(
            topic=_RAW_TOPIC,
            value=envelope_bytes,
            key=str(tenant_id).encode("utf-8"),
        )
    except Exception:
        _bump("shadow_write.failure.kafka")
        raise

    _bump("shadow_write.success")
    return s3_key


__all__ = [
    "get_metrics",
    "reset_metrics",
    "shadow_write_raw",
]
