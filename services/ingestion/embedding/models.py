"""services/ingestion/embedding/models.py — embedding-needed envelope.

Per ingestion LLD §5.4 and M3.2 work order. The wire shape on
`ingestion.embedding`.

Design:
  - Carries only the OBSERVATION ID (not the content_text). The
    worker re-reads `content_text` from Postgres to keep the wire
    payload small and avoid duplicating large message bodies through
    Kafka's commit log.
  - Carries `source` redundantly so the DLQ publish path can
    populate `ingestion_failures.source` (NOT NULL) even if the
    observation row is gone by the time the worker runs.
  - `extra="forbid"`: same versioning policy as RawEnvelope /
    NormalizedEnvelope. Additive within v1; breaking change bumps
    the version and adds a new model class.
"""
from __future__ import annotations

import datetime as dt
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from services.ingestion.raw_tier.envelope import SourceLiteral


class EmbeddingEnvelope(BaseModel):
    """Kafka message body on `ingestion.embedding`.

    Field mapping:
      tenant_id      → observations.tenant_id (for RLS + logging)
      source         → observations.kind / source channel family
                       (for DLQ.failure_kind classification)
      observation_id → observations.id — the row the worker SELECTs
                       to fetch content_text and UPDATEs with the
                       computed embedding.
      enqueued_at    → producer-side timestamp (UTC). Worker uses
                       this for lag observability (Kafka header
                       timestamp is also present but this is the
                       producer's clock at decision time).
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=False,
    )

    envelope_version: Literal[1] = 1
    tenant_id: UUID
    source: SourceLiteral
    observation_id: UUID
    enqueued_at: dt.datetime


__all__ = ["EmbeddingEnvelope"]
