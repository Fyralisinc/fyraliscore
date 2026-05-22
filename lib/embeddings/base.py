"""
lib/embeddings/base.py — embedder Protocol.

Every embedder backend (Ollama, OpenAI, future: Voyage, Cohere) implements
this Protocol so callers can swap providers without touching call sites.

The two non-obvious contracts:

  * `expected_dim` is the dimension the backend will actually produce.
    Production schemas are pinned at VECTOR(768) (see SCHEMA-LOCK.md
    S1.1/S2.1/S6.1), so any backend whose default dim differs MUST be
    configured to project down to 768 (OpenAI text-embedding-3-* supports
    a `dimensions` request parameter; Voyage / Cohere have similar knobs).
    Mismatch is enforced at embed() time and surfaces as a precise
    EmbedderDimensionMismatch error rather than an opaque pgvector cast
    failure on INSERT.

  * `model_name` is whatever string identifies the underlying model.
    Used in logs, cost-attribution rows, and reconciliation event
    metadata so a future audit can answer "which embedder produced
    this Model's vector?" — necessary when investigating drift after
    a backend swap.

Implementations live alongside this file:
  - lib/embeddings/ollama.py   → OllamaEmbedder (default; nomic-embed-text)
  - lib/embeddings/openai_backend.py → OpenAIEmbedder (text-embedding-3-small + dimensions=768)
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from lib.shared.errors import CompanyOSError


class EmbedderError(CompanyOSError):
    """Base class for embedder backends. Subclasses live in each backend."""
    default_code = "embedder_error"


class EmbedderDimensionMismatch(EmbedderError):
    """Backend produced a vector of unexpected dimension. Caught early so
    pgvector INSERT doesn't fail with an opaque cast error downstream."""
    default_code = "embedder_dimension_mismatch"


@runtime_checkable
class Embedder(Protocol):
    """Async embedder. All Company OS embedding flows depend on this Protocol.

    Implementations must:
      - return a unit-norm or near-unit-norm vector (HNSW cosine distance
        is direction-only; we don't normalize on the read path)
      - raise EmbedderDimensionMismatch on dim mismatch (don't silently
        truncate or pad)
      - be safe under concurrent use — callers share one instance across
        many tasks
    """

    @property
    def expected_dim(self) -> int:
        """The dim the backend will produce. Pinned to 768 in production."""
        ...

    @property
    def model_name(self) -> str:
        """Identifier of the underlying model (for logs, audit, cost rows)."""
        ...

    async def embed(self, text: str) -> list[float]:
        """Embed a single string into a vector of `expected_dim` floats."""
        ...

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed many strings. Order of outputs matches inputs. Empty
        input → empty output (do NOT round-trip to the backend)."""
        ...

    async def close(self) -> None:
        """Release any backend-owned resources (HTTP client, etc.).
        Idempotent."""
        ...


__all__ = [
    "Embedder",
    "EmbedderError",
    "EmbedderDimensionMismatch",
]
