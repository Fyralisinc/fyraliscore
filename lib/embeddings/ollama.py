"""
lib/embeddings/ollama.py — async client for Ollama /api/embeddings.

Spec §23: Ollama is the local, private embedding service. Default
model is `nomic-embed-text` (v1.5 is what we pin here) producing
768-dim vectors. The embedding column in `observations`, `models`,
and `entity_aliases` is `VECTOR(768)` per SCHEMA-LOCK.md S1/S2/S6.

This client:
- is fully async (httpx.AsyncClient under the hood)
- retries transient errors with exponential backoff (5xx, connection
  errors, timeouts); does NOT retry 4xx (caller bug)
- fails loud on dimension mismatch so that a misconfigured model is
  caught at ingestion time rather than silently produces unusable
  vectors
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import httpx

from lib.shared.errors import CompanyOSError


EMBEDDING_DIM = 768  # SCHEMA-LOCK.md S1.1 / S2.1 / S6.1 — VECTOR(768)
DEFAULT_MODEL = "nomic-embed-text"


class OllamaError(CompanyOSError):
    default_code = "ollama_error"


class OllamaDimensionMismatch(OllamaError):
    default_code = "ollama_dimension_mismatch"


@dataclass(frozen=True)
class OllamaConfig:
    base_url: str = "http://localhost:11434"
    model: str = DEFAULT_MODEL
    timeout_s: float = 20.0
    max_retries: int = 3
    initial_backoff_s: float = 0.2
    backoff_factor: float = 2.0
    expected_dim: int = EMBEDDING_DIM

    @classmethod
    def from_env(cls) -> "OllamaConfig":
        return cls(
            base_url=os.environ.get("OLLAMA_URL", cls.base_url),
            model=os.environ.get("OLLAMA_EMBED_MODEL", cls.model),
            timeout_s=float(os.environ.get("OLLAMA_TIMEOUT_S", cls.timeout_s)),
        )


class OllamaClient:
    """
    Thin async wrapper. Callers can share a single instance — it is
    safe under concurrent use (httpx.AsyncClient is).
    """

    def __init__(
        self,
        config: OllamaConfig | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config or OllamaConfig.from_env()
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
        )

    async def close(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> "OllamaClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------
    async def embed(self, text: str) -> list[float]:
        """
        Embed a single string. Raises OllamaError on persistent
        failure; raises OllamaDimensionMismatch if the returned
        vector is not exactly 768 floats.
        """
        if not isinstance(text, str):
            raise TypeError(f"text must be str, got {type(text).__name__}")
        body = {"model": self.config.model, "prompt": text}
        payload = await self._post_with_retry("/api/embeddings", body)
        vec = payload.get("embedding")
        if not isinstance(vec, list) or not all(isinstance(x, (int, float)) for x in vec):
            raise OllamaError("Ollama response missing 'embedding' list", body=payload)
        if len(vec) != self.config.expected_dim:
            raise OllamaDimensionMismatch(
                f"embedding dim mismatch: got {len(vec)}, expected "
                f"{self.config.expected_dim}",
                got=len(vec),
                expected=self.config.expected_dim,
                model=self.config.model,
            )
        return [float(x) for x in vec]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Embed many strings. Ollama's /api/embeddings endpoint is
        single-input, so this is a concurrent fan-out (bounded by
        Ollama's own queueing, not by us).
        """
        if not texts:
            return []
        return await asyncio.gather(*(self.embed(t) for t in texts))

    # -----------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------
    async def _post_with_retry(
        self,
        path: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        last_exc: Exception | None = None
        backoff = self.config.initial_backoff_s

        for attempt in range(self.config.max_retries):
            try:
                resp = await self._client.post(path, json=body)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                    httpx.PoolTimeout, httpx.RemoteProtocolError) as e:
                last_exc = e
                if attempt == self.config.max_retries - 1:
                    raise OllamaError(
                        f"ollama unreachable after {attempt + 1} attempts: {e}",
                        path=path,
                    ) from e
                await asyncio.sleep(backoff)
                backoff *= self.config.backoff_factor
                continue

            if resp.status_code >= 500:
                last_exc = OllamaError(
                    f"ollama returned {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:500],
                )
                if attempt == self.config.max_retries - 1:
                    raise last_exc
                await asyncio.sleep(backoff)
                backoff *= self.config.backoff_factor
                continue

            if resp.status_code >= 400:
                # 4xx is a caller bug; don't retry.
                raise OllamaError(
                    f"ollama 4xx: {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:500],
                )

            try:
                return resp.json()
            except Exception as e:
                raise OllamaError(
                    f"ollama returned non-JSON response: {e}",
                    body=resp.text[:500],
                ) from e

        # Theoretically unreachable — the loop always either returns
        # or raises — but defensive.
        raise OllamaError(f"ollama retry loop exhausted: {last_exc}") from last_exc


__all__ = [
    "OllamaClient",
    "OllamaConfig",
    "OllamaError",
    "OllamaDimensionMismatch",
    "EMBEDDING_DIM",
    "DEFAULT_MODEL",
]
