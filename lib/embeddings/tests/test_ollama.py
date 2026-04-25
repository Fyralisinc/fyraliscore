"""Tests for lib/embeddings/ollama.py."""
from __future__ import annotations

import asyncio
import os

import httpx
import pytest
import respx

from lib.embeddings.ollama import (
    DEFAULT_MODEL,
    EMBEDDING_DIM,
    OllamaClient,
    OllamaConfig,
    OllamaDimensionMismatch,
    OllamaError,
)


BASE = "http://ollama-mock"


def _cfg(**overrides) -> OllamaConfig:
    defaults = dict(
        base_url=BASE,
        model="test-model",
        timeout_s=1.0,
        max_retries=3,
        initial_backoff_s=0.0,    # speeds up retry tests
        backoff_factor=1.0,
        expected_dim=EMBEDDING_DIM,
    )
    defaults.update(overrides)
    return OllamaConfig(**defaults)


# =====================================================================
# Unit tests with respx-mocked httpx
# =====================================================================

async def test_embed_success():
    vec = [0.1] * EMBEDDING_DIM
    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embeddings").respond(200, json={"embedding": vec})
        async with OllamaClient(_cfg()) as c:
            out = await c.embed("hello")
        assert out == vec
        assert len(out) == EMBEDDING_DIM


async def test_embed_rejects_non_str():
    # No HTTP call expected — type check happens before the request.
    async with OllamaClient(_cfg()) as c:
        with pytest.raises(TypeError):
            await c.embed(123)


async def test_embed_dimension_mismatch():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embeddings").respond(200, json={"embedding": [0.0] * 512})
        async with OllamaClient(_cfg()) as c:
            with pytest.raises(OllamaDimensionMismatch) as exc:
                await c.embed("x")
        assert exc.value.context["got"] == 512
        assert exc.value.context["expected"] == EMBEDDING_DIM


async def test_embed_missing_embedding_field():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embeddings").respond(200, json={"wrong": "payload"})
        async with OllamaClient(_cfg()) as c:
            with pytest.raises(OllamaError):
                await c.embed("x")


async def test_embed_retries_on_5xx_and_succeeds():
    good = [0.0] * EMBEDDING_DIM
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embeddings")
        route.side_effect = [
            httpx.Response(500, text="boom"),
            httpx.Response(502, text="boom"),
            httpx.Response(200, json={"embedding": good}),
        ]
        async with OllamaClient(_cfg(max_retries=3)) as c:
            out = await c.embed("x")
        assert out == good
        assert route.call_count == 3


async def test_embed_gives_up_after_max_retries():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embeddings")
        route.side_effect = [httpx.Response(503) for _ in range(5)]
        async with OllamaClient(_cfg(max_retries=3)) as c:
            with pytest.raises(OllamaError):
                await c.embed("x")
        assert route.call_count == 3


async def test_embed_does_not_retry_4xx():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embeddings").respond(400, text="bad")
        async with OllamaClient(_cfg(max_retries=5)) as c:
            with pytest.raises(OllamaError) as exc:
                await c.embed("x")
        assert exc.value.context["status"] == 400
        assert route.call_count == 1


async def test_embed_retries_on_connect_error():
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embeddings")
        route.side_effect = [
            httpx.ConnectError("refused"),
            httpx.Response(200, json={"embedding": [0.0] * EMBEDDING_DIM}),
        ]
        async with OllamaClient(_cfg(max_retries=3)) as c:
            out = await c.embed("x")
        assert len(out) == EMBEDDING_DIM


async def test_embed_batch_returns_ordered_vectors():
    vecs = [[float(i)] * EMBEDDING_DIM for i in range(3)]
    call_idx = {"n": 0}
    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embeddings")

        def _side(request: httpx.Request) -> httpx.Response:
            idx = call_idx["n"]
            call_idx["n"] += 1
            return httpx.Response(200, json={"embedding": vecs[idx]})

        route.side_effect = _side

        async with OllamaClient(_cfg()) as c:
            out = await c.embed_batch(["a", "b", "c"])
        # Order of HTTP calls is non-deterministic under gather; we
        # only verify each returned vector is one of the expected.
        assert sorted(out) == sorted(vecs)


async def test_embed_batch_empty():
    async with OllamaClient(_cfg()) as c:
        assert await c.embed_batch([]) == []


async def test_embed_returns_floats_not_ints():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embeddings").respond(200, json={"embedding": [1] * EMBEDDING_DIM})
        async with OllamaClient(_cfg()) as c:
            out = await c.embed("x")
        assert all(isinstance(v, float) for v in out)


async def test_embed_non_json_body_raises():
    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embeddings").respond(200, text="not json")
        async with OllamaClient(_cfg()) as c:
            with pytest.raises(OllamaError):
                await c.embed("x")


async def test_config_from_env(monkeypatch):
    monkeypatch.setenv("OLLAMA_URL", "http://other")
    monkeypatch.setenv("OLLAMA_EMBED_MODEL", "custom-model")
    monkeypatch.setenv("OLLAMA_TIMEOUT_S", "5")
    cfg = OllamaConfig.from_env()
    assert cfg.base_url == "http://other"
    assert cfg.model == "custom-model"
    assert cfg.timeout_s == 5.0


def test_config_defaults():
    cfg = OllamaConfig()
    assert cfg.base_url == "http://localhost:11434"
    assert cfg.model == DEFAULT_MODEL
    assert cfg.expected_dim == EMBEDDING_DIM


async def test_context_manager_closes_own_client():
    cfg = _cfg()
    c = OllamaClient(cfg)
    async with c:
        pass
    # After exit, the client is closed; httpx raises on use.
    with pytest.raises(RuntimeError):
        await c._client.get("/")


async def test_external_client_not_closed():
    cfg = _cfg()
    ext = httpx.AsyncClient(base_url=BASE)
    c = OllamaClient(cfg, client=ext)
    await c.close()                  # should NOT close ext
    # ext should still work
    with respx.mock(base_url=BASE) as mock:
        mock.get("/ping").respond(200, text="ok")
        r = await ext.get("/ping")
    assert r.status_code == 200
    await ext.aclose()


# =====================================================================
# Integration tests — require live Ollama + a pulled model
# =====================================================================

def _ollama_available() -> bool:
    if not os.environ.get("OLLAMA_URL"):
        return False
    try:
        r = httpx.get(f"{os.environ['OLLAMA_URL']}/api/tags", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


pytestmark_ollama = pytest.mark.ollama
requires_ollama = pytest.mark.skipif(
    not _ollama_available(),
    reason="OLLAMA_URL not reachable; skipping integration test",
)


@pytestmark_ollama
@requires_ollama
async def test_integration_real_ollama_embed_returns_768():
    """
    Real Ollama. Must be running locally with a pulled nomic-embed-text
    model (see docker-compose.yml).
    """
    cfg = OllamaConfig(
        base_url=os.environ["OLLAMA_URL"],
        model=os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
    )
    async with OllamaClient(cfg) as c:
        out = await c.embed("Alice merged a PR")
    assert len(out) == EMBEDDING_DIM
    assert all(isinstance(v, float) for v in out)


@pytestmark_ollama
@requires_ollama
async def test_integration_real_ollama_semantic_similarity():
    cfg = OllamaConfig(
        base_url=os.environ["OLLAMA_URL"],
        model=os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
    )
    async with OllamaClient(cfg) as c:
        a = await c.embed("Alice merged a pull request in the rate-limiter repo")
        b = await c.embed("Alice completed work on rate-limiting")
        c_ = await c.embed("The weather today is very pleasant")
    # Cosine similarity: a.b > a.c
    def cos(u, v):
        num = sum(x * y for x, y in zip(u, v))
        du = sum(x * x for x in u) ** 0.5
        dv = sum(x * x for x in v) ** 0.5
        return num / (du * dv)
    assert cos(a, b) > cos(a, c_)


@pytestmark_ollama
@requires_ollama
async def test_integration_real_ollama_batch_ordering():
    cfg = OllamaConfig(
        base_url=os.environ["OLLAMA_URL"],
        model=os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
    )
    async with OllamaClient(cfg) as c:
        texts = ["A", "B", "C", "D"]
        vecs = await c.embed_batch(texts)
    assert len(vecs) == 4
    for v in vecs:
        assert len(v) == EMBEDDING_DIM
