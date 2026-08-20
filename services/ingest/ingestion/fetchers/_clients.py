"""Provider client openers retained for supplemental history ingestion."""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

import asyncpg
import httpx

from lib.shared.circuit_breaker import AsyncCircuitBreaker
from lib.shared.errors import CompanyOSError
from lib.shared.http_retry import is_retryable_httpx_error

_POOL: asyncpg.Pool | None = None
_POOL_LOCK = asyncio.Lock()
_HTTP: httpx.AsyncClient | None = None
_HTTP_LOCK = asyncio.Lock()
_SECRET_STORE: Any = None
_BREAKERS: dict[str, AsyncCircuitBreaker] = {}


def _record_breaker_exception(exc: BaseException) -> bool:
    if isinstance(exc, CompanyOSError):
        return exc.recoverable
    if isinstance(exc, httpx.HTTPStatusError):
        return is_retryable_httpx_error(exc)
    return isinstance(exc, httpx.TransportError)


class _ClientProxy:
    def __init__(self, source: str, client: Any) -> None:
        object.__setattr__(self, "_client", client)
        breaker = _BREAKERS.setdefault(
            source,
            AsyncCircuitBreaker(
                name=f"source_api_{source}",
                record_exception=_record_breaker_exception,
            ),
        )
        object.__setattr__(self, "_breaker", breaker)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if name.startswith("_") or not inspect.iscoroutinefunction(attr):
            return attr

        @wraps(attr)
        async def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return await self._breaker.call(lambda: attr(*args, **kwargs))

        return _wrapped


async def _http() -> httpx.AsyncClient:
    global _HTTP
    if _HTTP is None:
        async with _HTTP_LOCK:
            if _HTTP is None:
                _HTTP = httpx.AsyncClient(
                    timeout=30.0,
                    limits=httpx.Limits(
                        max_connections=64,
                        max_keepalive_connections=32,
                    ),
                )
    return _HTTP


async def _pool() -> asyncpg.Pool:
    global _POOL
    if _POOL is None:
        async with _POOL_LOCK:
            if _POOL is None:
                from services.ingest.ingestion.workflows.runtime import (
                    make_workflow_pool,
                )

                _POOL = await make_workflow_pool(os.environ["DATABASE_URL"])
    return _POOL


async def _secrets() -> Any:
    global _SECRET_STORE
    if _SECRET_STORE is None:
        from lib.shared.secrets import build_secret_store

        _SECRET_STORE = build_secret_store(await _pool())
    return _SECRET_STORE


async def _noop() -> None:
    return None


Opener = tuple[Any, Callable[[], Awaitable[None]]]


async def open_facebook_pages_client(install: asyncpg.Record) -> Opener:
    from services.ingest.integrations.facebook_pages.client import (
        FacebookPagesClient,
        graph_api_base_url,
    )

    page_id = str(install["page_id"])
    spammer = bool(os.environ.get("SYNTHETIC_SOURCE_API_BASE"))
    client = FacebookPagesClient(
        base_url=graph_api_base_url(),
        access_token=f"spam-facebook-pages::{page_id}" if spammer else None,
        page_access_token_ref=(
            install["page_access_token_ref"]
            if "page_access_token_ref" in install
            else None
        ),
        pool=None if spammer else await _pool(),
        secret_store=None if spammer else await _secrets(),
        tenant_id=install["tenant_id"],
        http_client=await _http(),
    )
    return _ClientProxy("facebook_pages", client), _noop


async def open_instagram_client(install: asyncpg.Record) -> Opener:
    from services.ingest.integrations.instagram.client import InstagramClient

    spammer = bool(os.environ.get("SYNTHETIC_SOURCE_API_BASE"))
    configured_base = os.environ.get(
        "INSTAGRAM_API_BASE_URL", "https://graph.instagram.com"
    )
    if spammer:
        configured_base = (
            f"{os.environ['SYNTHETIC_SOURCE_API_BASE'].rstrip('/')}/instagram"
        )
    client = InstagramClient(
        base_url=(
            configured_base
            if spammer
            else str(
                install["base_url"]
                if "base_url" in install
                else configured_base
            )
        ),
        secret_store=None if spammer else await _secrets(),
        tenant_id=install["tenant_id"],
        secret_ref=(
            install["access_token_ref"] if "access_token_ref" in install else None
        ),
        access_token="spam-instagram" if spammer else None,
        http_client=await _http(),
    )
    return _ClientProxy("instagram", client), _noop


__all__ = ["open_facebook_pages_client", "open_instagram_client"]
