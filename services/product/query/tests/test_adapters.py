from __future__ import annotations

import logging
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from lib.shared.circuit_breaker import AsyncCircuitBreaker, CircuitState
from lib.shared.errors import DependencyUnavailableError
from lib.shared.http_retry import HttpRetryConfig, is_retryable_httpx_error
from services.product.query.adapters import (
    HttpRenderingAdapter,
    InMemoryCacheAdapter,
    MockRenderingAdapter,
    PostgresCacheAdapter,
    RenderRequest,
    build_cache_adapter,
    build_rendering_adapter,
)


def _rendering_breaker(name: str, *, min_samples: int = 2) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        name=name,
        failure_threshold=0.5,
        min_samples=min_samples,
        open_duration=30.0,
        record_exception=lambda exc: not isinstance(exc, httpx.HTTPStatusError)
        or is_retryable_httpx_error(exc),
    )


def test_build_rendering_adapter_warns_before_dev_mock_fallback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("QUERY_RENDERING_BASE_URL", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "dev")
    monkeypatch.setenv("COMPANY_OS_ENV", "dev")

    with caplog.at_level(logging.WARNING):
        adapter = build_rendering_adapter()

    assert isinstance(adapter, MockRenderingAdapter)
    assert "QUERY_RENDERING_BASE_URL is unset" in caplog.text


def test_build_rendering_adapter_fails_closed_in_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QUERY_RENDERING_BASE_URL", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="QUERY_RENDERING_BASE_URL is unset"):
        build_rendering_adapter()


def test_build_rendering_adapter_uses_http_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUERY_RENDERING_BASE_URL", "http://rendering:8000")
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    assert isinstance(build_rendering_adapter(), HttpRenderingAdapter)


async def test_http_rendering_adapter_retries_transient_status() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"error": "busy"})
        return httpx.Response(
            200,
            json={
                "response_html": "<p>ok</p>",
                "rendering_model_used": "rnd",
                "cost_usd": "0.01",
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    adapter = HttpRenderingAdapter(
        base_url="http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=2,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        sleep=sleep,
    )
    try:
        response = await adapter.render_conversation_turn(
            RenderRequest(
                tenant_id=uuid4(),
                query="what changed?",
                category="situation",
            )
        )
    finally:
        await client.aclose()

    assert calls == 2
    sleep.assert_awaited_once_with(0.01)
    assert response.response_html == "<p>ok</p>"
    assert response.rendering_model_used == "rnd"


async def test_http_rendering_adapter_does_not_retry_client_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, json={"error": "bad request"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = HttpRenderingAdapter(
        base_url="http://rendering",
        client=client,
        breaker=_rendering_breaker("query_rendering_permanent_test"),
        retry_config=HttpRetryConfig(max_attempts=3),
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await adapter.render_conversation_turn(
                RenderRequest(
                    tenant_id=uuid4(),
                    query="what changed?",
                    category="situation",
                )
            )
    finally:
        await client.aclose()

    assert calls == 1
    assert adapter._breaker.status()["samples"] == 0
    assert adapter._breaker.state == CircuitState.CLOSED


async def test_http_rendering_adapter_reuses_owned_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "response_html": "<p>pooled</p>",
                "rendering_model_used": "rnd",
                "cost_usd": "0.02",
            }

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.posts = []
            self.closed = False
            created.append(self)

        async def post(
            self,
            url: str,
            *,
            json: dict,
            headers: dict[str, str],
        ):
            self.posts.append((url, json, headers))
            return FakeResponse()

        async def aclose(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        "services.product.query.adapters.httpx.AsyncClient",
        FakeAsyncClient,
    )
    adapter = HttpRenderingAdapter(base_url="http://rendering", timeout_s=8.0)
    request = RenderRequest(
        tenant_id=uuid4(),
        query="what changed?",
        category="situation",
    )

    await adapter.render_conversation_turn(request)
    await adapter.render_conversation_turn(request)

    assert len(created) == 1
    assert created[0].kwargs["timeout"] == 8.0
    assert len(created[0].posts) == 2
    assert created[0].closed is False

    await adapter.aclose()
    assert created[0].closed is True


async def test_http_rendering_adapter_raises_structured_unavailable_after_retries() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "busy"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    adapter = HttpRenderingAdapter(
        base_url="http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=2,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        sleep=sleep,
    )
    try:
        with pytest.raises(DependencyUnavailableError) as exc_info:
            await adapter.render_conversation_turn(
                RenderRequest(
                    tenant_id=uuid4(),
                    query="what changed?",
                    category="situation",
                )
            )
    finally:
        await client.aclose()

    err = exc_info.value
    assert calls == 2
    assert err.code == "dependency_unavailable"
    assert err.recoverable is True
    assert err.context["dependency"] == "rendering"
    assert err.context["operation"] == "conversation-turn"
    assert err.context["attempts"] == 2
    assert err.context["status_code"] == 503


async def test_http_rendering_adapter_fast_fails_when_breaker_is_open() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "busy"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sleep = AsyncMock()
    breaker = _rendering_breaker("query_rendering_open_test", min_samples=2)
    adapter = HttpRenderingAdapter(
        base_url="http://rendering",
        client=client,
        retry_config=HttpRetryConfig(
            max_attempts=1,
            initial_backoff_s=0.01,
            max_backoff_s=0.01,
            jitter_ratio=0,
        ),
        breaker=breaker,
        sleep=sleep,
    )
    request = RenderRequest(
        tenant_id=uuid4(),
        query="what changed?",
        category="situation",
    )
    try:
        for _ in range(2):
            with pytest.raises(DependencyUnavailableError):
                await adapter.render_conversation_turn(request)

        assert breaker.state == CircuitState.OPEN
        with pytest.raises(DependencyUnavailableError) as exc_info:
            await adapter.render_conversation_turn(request)
    finally:
        await client.aclose()

    err = exc_info.value
    assert calls == 2
    assert err.context["circuit_open"] is True
    assert err.context["attempts"] == 0
    assert err.context["breaker"] == "query_rendering_open_test"


def test_build_cache_adapter_allows_memory_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QUERY_CACHE_BACKEND", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "dev")
    monkeypatch.setenv("COMPANY_OS_ENV", "dev")

    assert isinstance(build_cache_adapter(), InMemoryCacheAdapter)


def test_build_cache_adapter_fails_closed_in_prod_without_pg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QUERY_CACHE_BACKEND", raising=False)
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="QUERY_CACHE_BACKEND must be 'pg'"):
        build_cache_adapter()


def test_build_cache_adapter_fails_closed_in_prod_without_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUERY_CACHE_BACKEND", "pg")
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="requires a database pool"):
        build_cache_adapter()


def test_build_cache_adapter_uses_postgres_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = object()
    monkeypatch.setenv("QUERY_CACHE_BACKEND", "pg")
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    adapter = build_cache_adapter(pool=pool)

    assert isinstance(adapter, PostgresCacheAdapter)
