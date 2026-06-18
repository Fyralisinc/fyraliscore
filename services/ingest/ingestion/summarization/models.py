"""Kafka envelope for large-document summarization requests."""
from __future__ import annotations

import datetime as dt
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)


class SummarizationEnvelope(BaseModel):
    """Kafka message body on ``ingestion.summarization``.

    Like the embedding envelope, this carries only enough identity for the
    worker to reread the observation and update it. Large text stays in raw S3
    or Postgres, not in Kafka.
    """

    model_config = ConfigDict(extra="forbid", frozen=False)

    envelope_version: Literal[1] = 1
    tenant_id: UUID
    source: SourceLiteral
    observation_id: UUID
    raw_s3_key: str | None = None
    ingress_kind: IngressKindLiteral | None = None
    enqueued_at: dt.datetime


__all__ = ["SummarizationEnvelope"]
