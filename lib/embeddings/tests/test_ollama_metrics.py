"""Metrics instrumentation tests for lib/embeddings/ollama.py.

The ollama client bumps four shared-registry families per HTTP attempt:

  * ollama_embed_requests_total{model,operation,status}
  * ollama_embed_retries_total{model,operation}
  * ollama_embed_dimension_mismatch_total{model}
  * ollama_embed_request_duration_seconds{model,operation,status}

The registry is process-global, so every assertion here is a
before/after DELTA on the family's `.get(**labels)` accessor — never
an absolute value and never whole-text equality.
"""
from __future__ import annotations

import httpx
import pytest
import respx

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaConfig,
    OllamaDimensionMismatch,
)
from lib.observability import counter, histogram


BASE = "http://ollama-metrics-mock"
MODEL = "metrics-test-model"

_REQUEST_LABELS = ("model", "operation", "status")


def _cfg(**overrides) -> OllamaConfig:
    defaults = dict(
        base_url=BASE,
        model=MODEL,
        timeout_s=1.0,
        max_retries=3,
        initial_backoff_s=0.0,    # speeds up retry tests
        backoff_factor=1.0,
        expected_dim=EMBEDDING_DIM,
    )
    defaults.update(overrides)
    return OllamaConfig(**defaults)


# `counter()` / `histogram()` return the already-registered family when
# the name + label set match (importing lib.embeddings.ollama above
# registered them); help text is ignored on re-lookup.
def _requests():
    return counter(
        "ollama_embed_requests_total", "lookup", _REQUEST_LABELS,
    )


def _retries():
    return counter(
        "ollama_embed_retries_total", "lookup", ("model", "operation"),
    )


def _mismatches():
    return counter(
        "ollama_embed_dimension_mismatch_total", "lookup", ("model",),
    )


def _duration():
    return histogram(
        "ollama_embed_request_duration_seconds", "lookup", _REQUEST_LABELS,
    )


async def test_successful_embed_bumps_requests_total_ok():
    ok_labels = dict(model=MODEL, operation="embed", status="ok")
    before = _requests().get(**ok_labels)
    before_count = _duration().get_count(**ok_labels)

    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embed").respond(
            200, json={"embeddings": [[0.1] * EMBEDDING_DIM]}
        )
        async with OllamaClient(_cfg()) as c:
            out = await c.embed("hello")

    assert len(out) == EMBEDDING_DIM
    assert _requests().get(**ok_labels) - before == 1
    # The per-attempt duration histogram observed exactly one sample.
    assert _duration().get_count(**ok_labels) - before_count == 1


async def test_5xx_then_200_bumps_retries_total():
    retry_labels = dict(model=MODEL, operation="embed")
    ok_labels = dict(model=MODEL, operation="embed", status="ok")
    err_labels = dict(model=MODEL, operation="embed", status="http_5xx")
    before_retries = _retries().get(**retry_labels)
    before_ok = _requests().get(**ok_labels)
    before_5xx = _requests().get(**err_labels)

    with respx.mock(base_url=BASE) as mock:
        route = mock.post("/api/embed")
        route.side_effect = [
            httpx.Response(500, text="boom"),
            httpx.Response(200, json={"embeddings": [[0.0] * EMBEDDING_DIM]}),
        ]
        async with OllamaClient(_cfg(max_retries=3)) as c:
            out = await c.embed("x")

    assert len(out) == EMBEDDING_DIM
    assert route.call_count == 2
    assert _retries().get(**retry_labels) - before_retries == 1
    # Both attempts were counted, each with its own status.
    assert _requests().get(**err_labels) - before_5xx == 1
    assert _requests().get(**ok_labels) - before_ok == 1


async def test_dimension_mismatch_raises_and_bumps_counter():
    before = _mismatches().get(model=MODEL)

    with respx.mock(base_url=BASE) as mock:
        mock.post("/api/embed").respond(
            200, json={"embeddings": [[0.0] * 512]}
        )
        async with OllamaClient(_cfg()) as c:
            with pytest.raises(OllamaDimensionMismatch):
                await c.embed("x")

    assert _mismatches().get(model=MODEL) - before == 1
