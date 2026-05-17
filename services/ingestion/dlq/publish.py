"""services/ingestion/dlq/publish.py — best-effort DLQ publish helpers.

Per M3.1. Used by both the normalizer worker
(`services/ingestion/normalizer/worker.py`) and the no-op writer
(`services/ingestion/writers/observation_writer.py`) so the
two-line failure-handling pattern stays consistent.

The CORE contract (PRIME DIRECTIVE preserved from M2.4):
  A DLQ publish failure MUST NOT crash the caller. The function
  never raises; it logs + bumps a caller-supplied metric and
  returns.

Why "best-effort" extraction:
  Some failure paths reach DLQ with the full parsed envelope (e.g.
  M3.1's normalizer EnvelopeInvariantError path — we have the
  RawEnvelope). Others reach DLQ with only the raw Kafka message
  bytes (e.g. byte garbage that failed orjson.loads). The shared
  helper tries to extract `tenant_id` + `source` from the bytes;
  if it can't (no fields present), it SKIPS the publish — the
  `ingestion_failures.source` CHECK constraint forbids NULL source
  and `tenant_id` is NOT NULL FK.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Callable
from uuid import UUID

import orjson

from services.ingestion.dlq.models import DLQEnvelope, WireFailureKind


log = logging.getLogger(__name__)


_DLQ_TOPIC = "ingestion.dlq"
_VALID_SOURCES = frozenset({"slack", "github", "discord", "gmail"})


def extract_dlq_fields_best_effort(
    msg_bytes: bytes,
) -> tuple[UUID | None, str | None, str | None]:
    """Try to extract `(tenant_id, source, raw_s3_key)` from a Kafka
    message body. Returns Nones for fields it can't parse.

    Handles both shapes:
      - RawEnvelope (normalizer input) — fields at top level.
      - NormalizedEnvelope (writer input) — fields at top level.
    Both envelope shapes name tenant_id / source identically.
    """
    try:
        d = orjson.loads(msg_bytes)
    except (orjson.JSONDecodeError, ValueError, TypeError):
        return None, None, None
    if not isinstance(d, dict):
        return None, None, None

    tenant_id: UUID | None = None
    raw_tenant = d.get("tenant_id")
    if isinstance(raw_tenant, str):
        try:
            tenant_id = UUID(raw_tenant)
        except (ValueError, TypeError):
            tenant_id = None

    source: str | None = None
    raw_source = d.get("source")
    if raw_source in _VALID_SOURCES:
        source = raw_source

    raw_s3_key: str | None = None
    raw_key = d.get("raw_s3_key")
    if isinstance(raw_key, str) and raw_key:
        raw_s3_key = raw_key

    return tenant_id, source, raw_s3_key


async def publish_dlq(
    *,
    producer: Any,  # services.ingestion.kafka.IdempotentProducer
    failure_kind: WireFailureKind,
    error_summary: str,
    # Either: full envelope available (preferred path)
    tenant_id: UUID | None = None,
    source: str | None = None,
    raw_s3_key: str | None = None,
    # Or: only bytes (best-effort extraction fallback)
    msg_bytes: bytes | None = None,
    # Caller metrics — bumped on success / failure / skip respectively.
    on_success: Callable[[], None] | None = None,
    on_failure: Callable[[], None] | None = None,
    on_skipped: Callable[[], None] | None = None,
    error_context: dict[str, Any] | None = None,
) -> None:
    """Publish a DLQ envelope to `ingestion.dlq`. Never raises.

    Either pass `tenant_id` + `source` explicitly (preferred — happens
    when the caller has a parsed envelope) OR pass `msg_bytes` for
    best-effort extraction.

    Caller-supplied metric callbacks let each surface keep its own
    namespaced counters (`normalizer.dlq_publish.*` vs
    `writer.dlq_publish.*`).
    """
    # If tenant_id / source not supplied, try best-effort extract.
    if (tenant_id is None or source is None) and msg_bytes is not None:
        t_extracted, s_extracted, k_extracted = (
            extract_dlq_fields_best_effort(msg_bytes)
        )
        if tenant_id is None:
            tenant_id = t_extracted
        if source is None:
            source = s_extracted
        if raw_s3_key is None:
            raw_s3_key = k_extracted

    if tenant_id is None or source is None:
        if on_skipped is not None:
            on_skipped()
        log.info(
            "dlq_publish.skipped_no_keys",
            extra={
                "failure_kind": failure_kind,
                "has_tenant_id": tenant_id is not None,
                "has_source": source is not None,
            },
        )
        return

    try:
        env = DLQEnvelope(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]
            failure_kind=failure_kind,
            raw_s3_key=raw_s3_key,
            error_summary=error_summary[:500] if error_summary else "(empty)",
            error_context=error_context or {},
            failed_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    except Exception as exc:  # noqa: BLE001
        # Programmer error (failure_kind typo, etc.). Log + skip;
        # do not crash the caller.
        if on_failure is not None:
            on_failure()
        log.warning(
            "dlq_publish.envelope_build_failed",
            extra={
                "failure_kind": failure_kind,
                "error": str(exc)[:200],
            },
        )
        return

    try:
        await producer.produce(
            topic=_DLQ_TOPIC,
            value=orjson.dumps(env.model_dump(mode="json")),
            key=str(tenant_id).encode("utf-8"),
        )
        if on_success is not None:
            on_success()
    except Exception as exc:  # noqa: BLE001 — PRIME DIRECTIVE
        if on_failure is not None:
            on_failure()
        log.warning(
            "dlq_publish.kafka_error",
            extra={
                "failure_kind": failure_kind,
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )


__all__ = [
    "extract_dlq_fields_best_effort",
    "publish_dlq",
]
