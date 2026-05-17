"""services/ingestion/dlq/models.py — DLQ envelope (Pydantic v2).

Per ingestion LLD §1.3 and §5.5. The on-wire envelope shape for
`ingestion.dlq`. The DLQ writer translates this into an UPSERT on
`ingestion_failures` (LLD §1.3 schema, migration 0046).

Wire failure_kind vs DB failure_kind
====================================
The Kafka envelope carries a dotted producer-namespaced kind
(e.g. ``normalizer.parse_failure``). The DB CHECK constraint
(per `0046_ingestion_failures.sql`) enumerates a coarser bucket
(e.g. ``normalizer_parse_error``). The DLQ writer maps wire →
DB via a module-local table; see
`services.ingestion.writers.dlq_writer.dlq_writer._WIRE_TO_DB_FAILURE_KIND`.

Why the indirection: producers want fine-grained kinds for log
filtering / alerting; ops queries against the DB want stable
buckets that don't churn with every new producer. The wire schema
can extend additively (M3.2 will add ``embedding.ollama_failure``)
without touching the DB enum (which requires a migration each
time).

Versioning policy: additive within v1 (envelope_version stays 1);
breaking changes bump to a new model class.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from services.ingestion.raw_tier.envelope import SourceLiteral


# Wire-side failure kinds. M3.1 shipped three; M3.2 adds the fourth
# (`embedding.ollama_failure`). The DB CHECK enum was already extended
# in migration 0051 with the corresponding bucket name
# `embedding_ollama_failure`, so M3.2 needs no DB migration of its own.
WireFailureKind = Literal[
    "normalizer.parse_failure",     # Pydantic / orjson decode failure on RawEnvelope
    "normalizer.invariant_failure", # EnvelopeInvariantError (M2.4)
    "writer.invariant_failure",     # NormalizedEnvelope rejected by writer
    "embedding.ollama_failure",     # OllamaError after client-level retries (M3.2)
]


class DLQEnvelope(BaseModel):
    """The Kafka message body on `ingestion.dlq`.

    Field mapping to `ingestion_failures` (LLD §1.3):

        tenant_id      → ingestion_failures.tenant_id
        source         → ingestion_failures.source
        failure_kind   → ingestion_failures.failure_kind (via wire→DB map)
        raw_s3_key     → ingestion_failures.raw_s3_key
        error_summary  → ingestion_failures.error_summary
        error_context  → ingestion_failures.error_context (jsonb)
        failed_at      → ingestion_failures.first_seen_at on insert,
                         updates last_seen_at on UPSERT.

    UPSERT key (LLD §5.5): (tenant_id, source, raw_s3_key, failure_kind).
    Re-published failures bump attempt_count, do NOT create duplicates.
    """

    model_config = ConfigDict(
        # Reject extras so a v2 producer can't silently land bad data
        # on a v1 consumer.
        extra="forbid",
        frozen=False,
    )

    envelope_version: Literal[1] = 1
    tenant_id: UUID
    source: SourceLiteral
    failure_kind: WireFailureKind
    raw_s3_key: str | None = None
    # LLD pattern: error summaries cap at ~200 chars to keep the DB
    # row compact. Producers truncate before publishing.
    error_summary: str = Field(min_length=1, max_length=500)
    error_context: dict[str, Any] = Field(default_factory=dict)
    failed_at: dt.datetime


__all__ = ["DLQEnvelope", "WireFailureKind"]
