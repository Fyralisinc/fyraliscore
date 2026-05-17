"""Kafka producer + consumer plumbing for ingestion (M2+).

Public surface for M2:
  - `IdempotentProducer` — async wrapper over confluent-kafka's
    idempotent producer. One instance per process, attached to
    FastAPI app state.

M3 will add consumers (aiokafka — see normalizer/worker.py).
"""
from services.ingestion.kafka.producer import (  # noqa: F401
    IdempotentProducer,
    ProducerConfig,
)

__all__ = ["IdempotentProducer", "ProducerConfig"]
