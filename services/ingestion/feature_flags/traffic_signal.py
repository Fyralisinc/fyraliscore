"""services/ingestion/feature_flags/traffic_signal.py
   — Producer for the cutover circuit breaker's tenant traffic signal.

Per ingestion LLD §11.3.

The signal topic (`ingestion.tenant_traffic_signal`) carries a 1%
deterministic-hash sample of every webhook + Kafka-publish event,
keyed by tenant. The cutover circuit breaker (M5.1) consumes the
last ~90s of signal to identify "active tenants" before measuring
per-partition lag.

Why a separate topic rather than reading `ingestion.raw` directly:
  • A second consumer group on the production topic would compete
    with the normalizer for offsets and add operational complexity.
  • Sampling at 1% bounds the signal topic's volume at ~1% of raw
    traffic — cheap to consume per-tick.
  • Deterministic hashing means the same envelope's signal is
    consistently kept or dropped; the breaker observes a stable
    sample regardless of producer retry behaviour.

This module ships ONLY the producer (M5.1). The webhook router's
M5.3 cutover change wires it in; the FetchPage activity wiring is
deferred until M6 (per the LLD §11.3 + the work-unit prompt).
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any
from uuid import UUID

import orjson


log = logging.getLogger(__name__)


# Per LLD §11.3 — 1% sampling. Sample rate is a module-level constant
# so all callers (webhook router + FetchPage in M6) sample at the
# same rate.
SAMPLE_PCT = 1.0  # percent
SIGNAL_TOPIC = "ingestion.tenant_traffic_signal"


def _hash_to_unit(content_hash: bytes) -> float:
    """Map a content hash to [0.0, 1.0). Deterministic per hash."""
    # blake2b/digest-first-8-bytes → uint64 → [0, 2^64) → [0, 1).
    digest = hashlib.blake2b(content_hash, digest_size=8).digest()
    return int.from_bytes(digest, "big") / (1 << 64)


def should_sample(content_hash: bytes) -> bool:
    """Return True iff this content-hash falls in the sampling fraction.

    Public for tests. Deterministic: same hash → same decision.
    """
    return _hash_to_unit(content_hash) < (SAMPLE_PCT / 100.0)


async def maybe_emit_traffic_signal(
    *,
    tenant_id: UUID,
    source: str,
    ingress_kind: str,
    raw_partition: int,
    content_hash: bytes,
    kafka_producer: Any,  # services.ingestion.kafka.IdempotentProducer
) -> None:
    """Emit a signal-topic record with ~1% probability.

    Cheap and non-blocking on the happy path (90%+ of calls return
    immediately after the hash check). On the sample path, the
    publish is fire-and-forget — failures are logged and swallowed
    per the M2 shadow-path prime directive: producer-side signal
    failures must NEVER propagate to the user-visible webhook
    response.

    Parameters
    ----------
    tenant_id : the tenant whose traffic this signal reports.
    source : "slack" / "github" / "discord" / "gmail" / etc.
    ingress_kind : "webhook" / "gateway" / "pubsub" / "backfill".
    raw_partition : the partition the just-published envelope landed
        on. Used by the breaker to correlate this tenant with the
        lag-per-partition reading.
    content_hash : the same content-hash the M2.1 shadow path uses
        for S3 PutIfAbsent. Drives the sampling decision so duplicate
        retries of the same envelope produce the same sample
        decision (idempotent under retries).
    kafka_producer : a started `IdempotentProducer`. The producer's
        topic-existence semantics handle topic auto-creation if
        broker config permits, otherwise the publish silently fails
        (logged).
    """
    if not should_sample(content_hash):
        return

    try:
        body = orjson.dumps({
            "tenant_id": str(tenant_id),
            "source": source,
            "ingress_kind": ingress_kind,
            "raw_partition": raw_partition,
            "emitted_at_ms": int(time.time() * 1000),
        })
        await kafka_producer.produce(
            topic=SIGNAL_TOPIC,
            value=body,
            key=str(tenant_id).encode("utf-8"),
        )
    except Exception as exc:  # noqa: BLE001
        # Per the M2 shadow-path directive: producer-side signal
        # failures MUST NOT propagate. Log and swallow.
        log.warning(
            "traffic_signal.publish_failed",
            extra={
                "tenant_id": str(tenant_id),
                "source": source,
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )


__all__ = [
    "SAMPLE_PCT",
    "SIGNAL_TOPIC",
    "maybe_emit_traffic_signal",
    "should_sample",
]
