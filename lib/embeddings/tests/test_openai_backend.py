"""Unit tests for lib/embeddings/openai_backend.py — uses respx to mock
the OpenAI embeddings endpoint so tests stay offline-safe + deterministic."""
from __future__ import annotations

import httpx
import pytest
import respx

from lib.embeddings.base import Embedder
from lib.embeddings.openai_backend import (
    DEFAULT_OPENAI_DIM,
    OpenAIDimensionMismatch,
    OpenAIEmbedder,
    OpenAIEmbedderConfig,
    OpenAIEmbedderError,
)


BASE = "http://openai-mock/v1"


def _cfg(**overrides) -> OpenAIEmbedderConfig:
    defaults = dict(
        api_key="sk-test-key",
        base_url=BASE,
        model="text-embedding-3-small",
        expected_dim=DEFAULT_OPENAI_DIM,
        timeout_s=1.0,
        max_retries=3,
        initial_backoff_s=0.0,
        backoff_factor=1.0,
        max_batch_per_request=8,
    )
    defaults.update(overrides)
    return OpenAIEmbedderConfig(**defaults)


def _embed_response(texts: list[str], dim: int = DEFAULT_OPENAI_DIM) -> dict:
    return {
        "object": "list",
        "data": [
            {"object": "embedding", "index": i, "embedding": [0.01 * (i + 1)] * dim}
            for i, _ in enumerate(texts)
        ],
        "model": "text-embedding-3-small",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }


# =====================================================================
# Single embed
# =====================================================================

async def test_embed_returns_vector_of_expected_dim():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/embeddings").respond(200, json=_embed_response(["hi"]))
        async with OpenAIEmbedder(_cfg()) as e:
            vec = await e.embed("hi")
        assert len(vec) == DEFAULT_OPENAI_DIM
        assert all(isinstance(x, float) for x in vec)


async def test_embed_rejects_dim_mismatch():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/embeddings").respond(200, json=_embed_response(["hi"], dim=512))
        async with OpenAIEmbedder(_cfg()) as e:
            with pytest.raises(OpenAIDimensionMismatch):
                await e.embed("hi")


async def test_embed_rejects_non_string_input():
    async with OpenAIEmbedder(_cfg()) as e:
        with pytest.raises(TypeError):
            await e.embed(123)  # type: ignore[arg-type]


async def test_embed_passes_dimensions_param():
    captured: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        import json
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_embed_response(["x"]))

    with respx.mock(base_url=BASE) as mock:
        mock.post("/embeddings").mock(side_effect=_capture)
        async with OpenAIEmbedder(_cfg()) as e:
            await e.embed("x")

    assert captured["model"] == "text-embedding-3-small"
    assert captured["dimensions"] == DEFAULT_OPENAI_DIM
    assert captured["input"] == ["x"]


# =====================================================================
# Batch embed
# =====================================================================

async def test_embed_batch_empty_input_no_request():
    """Empty input must NOT round-trip to the backend."""
    async with OpenAIEmbedder(_cfg()) as e:
        out = await e.embed_batch([])
    assert out == []


async def test_embed_batch_under_chunk_size_one_request():
    texts = [f"t{i}" for i in range(5)]  # under chunk=8
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/embeddings").respond(200, json=_embed_response(texts))
        async with OpenAIEmbedder(_cfg()) as e:
            out = await e.embed_batch(texts)
        assert route.call_count == 1
    assert len(out) == 5


async def test_embed_batch_over_chunk_size_multiple_requests():
    texts = [f"t{i}" for i in range(20)]  # 3 chunks of 8

    def _resp(request: httpx.Request) -> httpx.Response:
        import json
        body = json.loads(request.content)
        return httpx.Response(200, json=_embed_response(body["input"]))

    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/embeddings").mock(side_effect=_resp)
        async with OpenAIEmbedder(_cfg()) as e:
            out = await e.embed_batch(texts)
        assert route.call_count == 3
    assert len(out) == 20


async def test_embed_batch_preserves_order_when_response_reorders():
    texts = ["alpha", "beta", "gamma"]

    def _scrambled(request: httpx.Request) -> httpx.Response:
        # Return data in reverse with explicit indices — embedder must
        # sort by 'index' to recover input order.
        return httpx.Response(200, json={
            "data": [
                {"index": 2, "embedding": [3.0] * DEFAULT_OPENAI_DIM},
                {"index": 0, "embedding": [1.0] * DEFAULT_OPENAI_DIM},
                {"index": 1, "embedding": [2.0] * DEFAULT_OPENAI_DIM},
            ],
            "model": "text-embedding-3-small",
        })

    with respx.mock(base_url=BASE) as mock:
        mock.post("/embeddings").mock(side_effect=_scrambled)
        async with OpenAIEmbedder(_cfg()) as e:
            out = await e.embed_batch(texts)

    assert out[0][0] == 1.0
    assert out[1][0] == 2.0
    assert out[2][0] == 3.0


# =====================================================================
# Error handling
# =====================================================================

async def test_500_retries_then_raises():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/embeddings").respond(500, text="boom")
        async with OpenAIEmbedder(_cfg(max_retries=3)) as e:
            with pytest.raises(OpenAIEmbedderError, match="500"):
                await e.embed("x")
        assert route.call_count == 3


async def test_429_retries_then_raises():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/embeddings").respond(429, text="rate limited")
        async with OpenAIEmbedder(_cfg(max_retries=3)) as e:
            with pytest.raises(OpenAIEmbedderError, match="429"):
                await e.embed("x")
        assert route.call_count == 3


async def test_401_no_retry():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/embeddings").respond(401, text="bad key")
        async with OpenAIEmbedder(_cfg(max_retries=3)) as e:
            with pytest.raises(OpenAIEmbedderError, match="401"):
                await e.embed("x")
        # 4xx (non-429) MUST NOT retry.
        assert route.call_count == 1


async def test_response_data_length_mismatch_raises():
    with respx.mock(base_url=BASE) as mock:
        # Request 3 inputs but response only includes 2 items.
        mock.post("/embeddings").respond(200, json={
            "data": [
                {"index": 0, "embedding": [0.1] * DEFAULT_OPENAI_DIM},
                {"index": 1, "embedding": [0.2] * DEFAULT_OPENAI_DIM},
            ],
            "model": "text-embedding-3-small",
        })
        async with OpenAIEmbedder(_cfg()) as e:
            with pytest.raises(OpenAIEmbedderError, match="length"):
                await e.embed_batch(["a", "b", "c"])


# =====================================================================
# Protocol conformance
# =====================================================================

def test_protocol_runtime_check():
    cfg = _cfg()
    e = OpenAIEmbedder(cfg)
    # Embedder is @runtime_checkable so isinstance works.
    assert isinstance(e, Embedder)
    assert e.expected_dim == DEFAULT_OPENAI_DIM
    assert e.model_name == "text-embedding-3-small"


# =====================================================================
# Auth header
# =====================================================================

async def test_auth_header_set_correctly():
    captured_auth: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured_auth["value"] = request.headers.get("authorization")
        return httpx.Response(200, json=_embed_response(["x"]))

    with respx.mock(base_url=BASE) as mock:
        mock.post("/embeddings").mock(side_effect=_capture)
        async with OpenAIEmbedder(_cfg(api_key="sk-special")) as e:
            await e.embed("x")
    assert captured_auth["value"] == "Bearer sk-special"
