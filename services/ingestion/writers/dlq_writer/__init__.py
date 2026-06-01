"""services/ingestion/writers/dlq_writer — DLQ → ingestion_failures.

Per ingestion LLD §5.5 + §1.3. M3.1.

This package consumes `ingestion.dlq` from Kafka and UPSERTs each
envelope into `ingestion_failures` (the queryable ops surface).
First activation of M1's `pgbouncer_compatible` pool flag in the
new pipeline.
"""
from services.ingestion.writers.dlq_writer.dlq_writer import (
    DLQWriterConfig,
    get_metrics,
    main,
    reset_metrics,
    run_dlq_writer,
    upsert_failure,
)

__all__ = [
    "DLQWriterConfig",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_dlq_writer",
    "upsert_failure",
]
