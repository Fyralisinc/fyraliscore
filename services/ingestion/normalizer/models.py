"""Pydantic v2 models for the normalizer's output topic.

Per ingestion LLD §5.2 and M2 work-order §M2.3.

The `NormalizedEnvelope` wraps the upstream `RawEnvelope`'s identity
fields (tenant_id, content_hash, raw_s3_key, ingress_kind, source)
with the handler-produced `ObservationDraft` fields. Consumers of
`ingestion.normalized`:

  - M2.4 — no-op writer that logs each message + asserts invariants.
  - M3+  — the real batched-INSERT writer for `observations`.

Pure JSON (no Python-only types) so the wire format is stable
across writer reimplementations.
"""
from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from services.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)


class NormalizedEnvelope(BaseModel):
    """The Kafka message body on `ingestion.normalized`.

    Field grouping:
      - Envelope identity (envelope_version, source, ingress_kind).
      - Upstream traceability (tenant_id, raw_s3_key, content_hash,
        raw_ingested_at) so the writer can correlate this row back
        to the raw body and to the original ingress event.
      - Handler-produced draft fields (source_channel, content_text,
        content, occurred_at, trust_tier, kind, source_actor_ref,
        external_id, entities_hint) — 1:1 with `ObservationDraft`.
      - Normalizer-local (normalized_at, ingress_metadata,
        idem_hints) — pass-through from the raw envelope plus a
        timestamp so M3+ can measure raw → normalized latency.
    """

    model_config = ConfigDict(extra="forbid")

    envelope_version: int = 1
    # ---- Envelope identity ----
    source: SourceLiteral
    ingress_kind: IngressKindLiteral
    # ---- Upstream traceability ----
    tenant_id: UUID
    raw_s3_key: str = Field(min_length=1)
    content_hash: str = Field(min_length=1)
    raw_ingested_at: dt.datetime
    # ---- Handler-produced draft (1:1 with ObservationDraft) ----
    source_channel: str = Field(min_length=1)
    content_text: str
    content: dict[str, Any] = Field(default_factory=dict)
    occurred_at: dt.datetime
    trust_tier: str = Field(min_length=1)
    kind: str = "signal"
    source_actor_ref: str | None = None
    external_id: str | None = None
    entities_hint: list[dict[str, Any]] = Field(default_factory=list)
    # ---- Normalizer-local ----
    normalized_at: dt.datetime
    ingress_metadata: dict[str, Any] = Field(default_factory=dict)
    idem_hints: dict[str, str] = Field(default_factory=dict)


__all__ = ["NormalizedEnvelope"]
