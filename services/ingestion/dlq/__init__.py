"""services/ingestion/dlq — Dead-letter queue envelope + publish helpers.

Per ingestion LLD §1.3 (`ingestion_failures` table) and §5.5 (DLQ
publish pattern). M3.1.

This package exports:
  - DLQEnvelope, WireFailureKind: the wire shape.
  - publish_dlq, extract_dlq_fields_best_effort: the helpers used
    by the normalizer worker and the no-op writer to publish
    failures with PRIME-DIRECTIVE-preserving error handling.

The DLQ writer (`services.ingestion.writers.dlq_writer`) consumes
these envelopes and UPSERTs `ingestion_failures` rows.
"""
from services.ingestion.dlq.models import (
    DLQEnvelope,
    WireFailureKind,
)
from services.ingestion.dlq.publish import (
    extract_dlq_fields_best_effort,
    publish_dlq,
)

__all__ = [
    "DLQEnvelope",
    "WireFailureKind",
    "extract_dlq_fields_best_effort",
    "publish_dlq",
]
