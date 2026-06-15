from __future__ import annotations

import logging

import pytest

from services.product.query.adapters import (
    HttpRenderingAdapter,
    InMemoryCacheAdapter,
    MockRenderingAdapter,
    PostgresCacheAdapter,
    build_cache_adapter,
    build_rendering_adapter,
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
