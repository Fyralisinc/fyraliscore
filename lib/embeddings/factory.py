"""
lib/embeddings/factory.py — pick an Embedder backend from env / explicit kwargs.

Single entry point so the rest of the code never imports a concrete
backend by name. New backends are added by extending the dispatch
mapping below; call sites stay untouched.

Selection rules (in priority order):
  1. Explicit `backend=` kwarg ('ollama' | 'openai').
  2. EMBEDDER_BACKEND env var ('ollama' | 'openai').
  3. Implicit fallback: if OPENAI_API_KEY is set and OLLAMA_URL is not,
     prefer OpenAI; otherwise default to Ollama. This keeps local
     development on Ollama without manual config while letting cloud
     deployments switch by setting OPENAI_API_KEY.

The returned object satisfies the `Embedder` Protocol from
lib.embeddings.base — callers should type their parameters as
`Embedder` (not the concrete class) so future swaps are zero-touch.
"""
from __future__ import annotations

import os
from typing import Literal

from lib.embeddings.base import Embedder, EmbedderError


BackendName = Literal["ollama", "openai"]


def _resolve_backend(explicit: BackendName | None) -> BackendName:
    if explicit is not None:
        if explicit not in ("ollama", "openai"):
            raise EmbedderError(
                f"unknown embedder backend: {explicit!r}; "
                f"expected 'ollama' or 'openai'",
            )
        return explicit
    env = os.environ.get("EMBEDDER_BACKEND")
    if env:
        env = env.lower().strip()
        if env not in ("ollama", "openai"):
            raise EmbedderError(
                f"EMBEDDER_BACKEND={env!r}; expected 'ollama' or 'openai'",
            )
        return env  # type: ignore[return-value]
    # Implicit fallback.
    has_openai_key = bool(os.environ.get("OPENAI_API_KEY"))
    has_ollama_url = bool(os.environ.get("OLLAMA_URL"))
    if has_openai_key and not has_ollama_url:
        return "openai"
    return "ollama"


def make_embedder(
    backend: BackendName | None = None,
) -> Embedder:
    """Construct and return an Embedder for the resolved backend.

    Caller owns the lifecycle — call `await embedder.close()` when done
    (or use as `async with`).
    """
    chosen = _resolve_backend(backend)
    if chosen == "openai":
        from lib.embeddings.openai_backend import OpenAIEmbedder
        return OpenAIEmbedder()
    # Default + 'ollama'
    from lib.embeddings.ollama import OllamaClient
    return OllamaClient()


__all__ = [
    "BackendName",
    "make_embedder",
]
