"""
lib/embeddings/openai_backend.py — OpenAI embeddings backend.

Implements the `Embedder` Protocol against the OpenAI Embeddings API
(POST /v1/embeddings). The default model is `text-embedding-3-small`
with `dimensions=768` so the output matches the production VECTOR(768)
schema without any column-level migration.

Why httpx instead of the openai SDK
-----------------------------------
We already depend on httpx for the Ollama client; sharing the transport
keeps the dependency surface small and the retry / timeout logic
uniform. The `/v1/embeddings` endpoint is a single POST — the SDK adds
auth + pagination + streaming helpers we don't need here.

Why text-embedding-3-small + dimensions=768
-------------------------------------------
  * 3-small is ~5× cheaper than 3-large at near-identical recall on
    our retrieval evaluations (the difference shows up on very
    semantically subtle pairs that we don't care about for substrate
    use cases — we have RRF + multiple pathways to handle the long
    tail).
  * The `dimensions` parameter projects the native 1536-dim embedding
    down to 768 via the same MRL truncation used by the API. This
    means existing pgvector indexes / HNSW configs / topo projection
    matrices stay valid: the schema width is unchanged.
  * If a deployment wants 1536, it can override `expected_dim` in the
    config — but it MUST also widen the VECTOR column or things fail
    at INSERT time.

Auth
----
`OPENAI_API_KEY` is the only required env var. `OPENAI_BASE_URL` is
honored if set, supporting Azure OpenAI / private deployments / local
proxies.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import httpx

from lib.embeddings.base import EmbedderDimensionMismatch, EmbedderError


DEFAULT_OPENAI_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_DIM = 768
DEFAULT_OPENAI_BASE = "https://api.openai.com/v1"


class OpenAIEmbedderError(EmbedderError):
    default_code = "openai_embedder_error"


class OpenAIDimensionMismatch(EmbedderDimensionMismatch):
    default_code = "openai_dimension_mismatch"


@dataclass(frozen=True)
class OpenAIEmbedderConfig:
    api_key: str
    base_url: str = DEFAULT_OPENAI_BASE
    model: str = DEFAULT_OPENAI_MODEL
    expected_dim: int = DEFAULT_OPENAI_DIM
    timeout_s: float = 30.0
    max_retries: int = 3
    initial_backoff_s: float = 0.5
    backoff_factor: float = 2.0
    # OpenAI's /v1/embeddings supports up to 2048 inputs per call. We
    # batch in chunks of this size; smaller defaults reduce the latency
    # impact of any one transient failure.
    max_batch_per_request: int = 256

    @classmethod
    def from_env(cls) -> "OpenAIEmbedderConfig":
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise OpenAIEmbedderError(
                "OPENAI_API_KEY is unset; cannot configure OpenAIEmbedder",
            )
        return cls(
            api_key=api_key,
            base_url=os.environ.get("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE),
            model=os.environ.get("OPENAI_EMBED_MODEL", DEFAULT_OPENAI_MODEL),
            expected_dim=int(os.environ.get("OPENAI_EMBED_DIM", DEFAULT_OPENAI_DIM)),
            timeout_s=float(os.environ.get("OPENAI_TIMEOUT_S", 30.0)),
        )


class OpenAIEmbedder:
    """OpenAI-backed Embedder. Safe under concurrent use."""

    def __init__(
        self,
        config: OpenAIEmbedderConfig | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config or OpenAIEmbedderConfig.from_env()
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            },
        )

    async def close(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> "OpenAIEmbedder":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    @property
    def expected_dim(self) -> int:
        return self.config.expected_dim

    @property
    def model_name(self) -> str:
        return self.config.model

    async def embed(self, text: str) -> list[float]:
        if not isinstance(text, str):
            raise TypeError(f"text must be str, got {type(text).__name__}")
        out = await self._embed_many([text])
        return out[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        # Chunk to respect per-request size cap.
        chunk = self.config.max_batch_per_request
        if len(texts) <= chunk:
            return await self._embed_many(texts)
        # Multi-chunk: gather concurrently; the API tolerates parallel
        # POSTs from the same key well within the per-org rate limit.
        tasks = [
            self._embed_many(texts[i : i + chunk])
            for i in range(0, len(texts), chunk)
        ]
        chunks = await asyncio.gather(*tasks)
        out: list[list[float]] = []
        for c in chunks:
            out.extend(c)
        return out

    # -----------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------
    async def _embed_many(self, texts: list[str]) -> list[list[float]]:
        body = {
            "model": self.config.model,
            "input": texts,
            "dimensions": self.config.expected_dim,
        }
        payload = await self._post_with_retry("/embeddings", body)
        data = payload.get("data")
        if not isinstance(data, list) or len(data) != len(texts):
            raise OpenAIEmbedderError(
                "OpenAI response 'data' missing or wrong length",
                got_len=(len(data) if isinstance(data, list) else None),
                expected_len=len(texts),
            )
        # Sort by 'index' to defend against any reordering. The API
        # documents same-order responses, but explicit is cheaper than
        # an obscure off-by-one bug if that ever changes.
        ordered = sorted(data, key=lambda r: r.get("index", 0))
        out: list[list[float]] = []
        for i, row in enumerate(ordered):
            vec = row.get("embedding")
            if not isinstance(vec, list) or not all(
                isinstance(x, (int, float)) for x in vec
            ):
                raise OpenAIEmbedderError(
                    "OpenAI response item missing 'embedding' list",
                    item_index=i,
                )
            if len(vec) != self.config.expected_dim:
                raise OpenAIDimensionMismatch(
                    f"OpenAI dim mismatch: got {len(vec)}, expected "
                    f"{self.config.expected_dim}",
                    got=len(vec),
                    expected=self.config.expected_dim,
                    model=self.config.model,
                )
            out.append([float(x) for x in vec])
        return out

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
            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.PoolTimeout,
                httpx.RemoteProtocolError,
            ) as e:
                last_exc = e
                if attempt == self.config.max_retries - 1:
                    raise OpenAIEmbedderError(
                        f"OpenAI unreachable after {attempt + 1} attempts: {e}",
                        path=path,
                    ) from e
                await asyncio.sleep(backoff)
                backoff *= self.config.backoff_factor
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                last_exc = OpenAIEmbedderError(
                    f"OpenAI returned {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:500],
                )
                if attempt == self.config.max_retries - 1:
                    raise last_exc
                # Honor Retry-After header when present.
                retry_after = resp.headers.get("retry-after")
                sleep_for = backoff
                if retry_after:
                    try:
                        sleep_for = max(sleep_for, float(retry_after))
                    except ValueError:
                        pass
                await asyncio.sleep(sleep_for)
                backoff *= self.config.backoff_factor
                continue

            if resp.status_code >= 400:
                # 4xx other than 429 — caller bug (bad model, missing
                # field, auth failure). Don't retry.
                raise OpenAIEmbedderError(
                    f"OpenAI 4xx: {resp.status_code}",
                    status=resp.status_code,
                    body=resp.text[:500],
                )

            try:
                return resp.json()
            except Exception as e:
                raise OpenAIEmbedderError(
                    f"OpenAI returned non-JSON response: {e}",
                    body=resp.text[:500],
                ) from e

        # Defensive — loop always returns or raises.
        raise OpenAIEmbedderError(
            f"OpenAI retry loop exhausted: {last_exc}"
        ) from last_exc


__all__ = [
    "OpenAIEmbedder",
    "OpenAIEmbedderConfig",
    "OpenAIEmbedderError",
    "OpenAIDimensionMismatch",
    "DEFAULT_OPENAI_MODEL",
    "DEFAULT_OPENAI_DIM",
    "DEFAULT_OPENAI_BASE",
]
