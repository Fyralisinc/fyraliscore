"""Pydantic v2 model for the `ingestion.raw` Kafka envelope.

Per ingestion LLD §5.1 and HLD §"`ingestion.raw` envelope shape"
(02-high-level-design.md:356-378). The envelope is a pointer
(~1-4 KB) carrying `raw_s3_key`, `content_hash`, `ingress_kind`,
`tenant_id`, and `idem_hints`. Bodies live in S3; Kafka throughput
is dominated by message count, not byte count.

Versioning policy:
  - additive fields only within v1 (the on-wire `envelope_version`
    stays 1).
  - breaking changes bump to v2 — a new model class consumers can
    opt into.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


SourceLiteral = Literal["slack", "github", "discord", "gmail"]
IngressKindLiteral = Literal["webhook", "gateway", "pubsub", "backfill"]


class RawEnvelope(BaseModel):
    """The Kafka message body on `ingestion.raw`.

    `envelope_version` is pinned to 1; the literal type rejects any
    other value at validation time. Future versions bump the class
    and rev the literal.

    `ingress_metadata` and `idem_hints` are free-form because the
    per-source values differ (webhook delivery_id vs. gateway
    sequence-number vs. backfill cursor_token). Schemas for those
    sub-dicts live with each ingress producer.
    """

    model_config = ConfigDict(
        # Reject extra top-level fields so a v2 producer can't
        # silently land bad data on a v1 consumer.
        extra="forbid",
        # Pydantic v2 default — but explicit so a reader knows.
        frozen=False,
    )

    envelope_version: Literal[1] = 1
    source: SourceLiteral
    tenant_id: UUID
    raw_s3_key: str = Field(min_length=1)
    content_hash: str = Field(min_length=1)
    ingested_at: datetime
    ingress_kind: IngressKindLiteral
    ingress_metadata: dict[str, Any] = Field(default_factory=dict)
    idem_hints: dict[str, str] = Field(default_factory=dict)


__all__ = ["IngressKindLiteral", "RawEnvelope", "SourceLiteral"]
