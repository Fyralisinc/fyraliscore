"""services/ingestion/writers/embedding_worker — Ollama embeddings.

Per ingestion LLD §5.4. M3.2.

Consumes `ingestion.embedding`, calls Ollama for each observation
that still has `embedding_pending=TRUE`, UPDATEs the observation
under the LLD §5.4 guard. Terminal Ollama failures (after the
OllamaClient's internal retry loop) publish a DLQ envelope with
failure_kind="embedding.ollama_failure" and continue consuming.

PATH A (DB-writing). Uses `pgbouncer_compatible=True` per the M1.3
ADR and the precedent set by `dlq_writer` in M3.1.
"""
from services.ingestion.writers.embedding_worker.embedding_worker import (
    EmbeddingWorkerConfig,
    embed_and_update,
    get_metrics,
    main,
    reset_metrics,
    run_embedding_worker,
)

__all__ = [
    "EmbeddingWorkerConfig",
    "embed_and_update",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_embedding_worker",
]
