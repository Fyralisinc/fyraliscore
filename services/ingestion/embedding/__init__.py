"""services/ingestion/embedding — embedding-needed signal envelope.

Per ingestion LLD §5.4. M3.2.

This package exports:
  - EmbeddingEnvelope: the wire shape on `ingestion.embedding`.
  - publish_embedding_request: best-effort publish helper used by the
    inline `services.ingestion.core.ingest` path AFTER an observation
    has been committed with `embedding_pending=TRUE`.

The embedding worker (`services.ingestion.writers.embedding_worker`)
consumes these envelopes, calls Ollama, and UPDATEs the observation
under the LLD §5.4 guard `WHERE embedding_pending = TRUE`. The guard
is load-bearing for two reasons documented in
[docs/ingestion/05-lld-amendments.md] amendment A3:
  (a) safe coexistence with the inline path during the M5 cutover
      window — inline + worker cannot both win the UPDATE.
  (b) operator-driven re-embed support — setting
      `embedding_pending = TRUE` on an existing-embedding row forces a
      re-compute, which would silently no-op under the alternative
      `WHERE embedding IS NULL` guard.
"""
from services.ingestion.embedding.models import EmbeddingEnvelope
from services.ingestion.embedding.publish import publish_embedding_request

__all__ = [
    "EmbeddingEnvelope",
    "publish_embedding_request",
]
