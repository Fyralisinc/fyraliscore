"""
services/product/query/adapters.py — pluggable adapters for services we depend
on that may not yet exist on the dogfood branch.

Two seams:

  1. RenderingAdapter — calls `services/product/rendering/` via HTTP per
     CONTRACTS §2.1. Until Agent-RND lands, `MockRenderingAdapter`
     synthesizes a conservative, voice-rule-friendly HTML response
     from the context bundle so the end-to-end pipeline runs.

  2. CacheAdapter — writes query-prefetch entries to the shared
     `view_ceo_cache` table. Until Agent-GRT's migration lands, the
     default adapter is an in-memory stub. The same module exposes
     `PostgresCacheAdapter` which is enabled once the table exists.

Both adapters share a discriminator protocol so tests can swap them
without monkey-patching.

Keeping this file small and honest means the day Agent-RND's
`POST /rendering/conversation-turn` endpoint lands we flip a flag in
factory-build and the mock drops out. No hidden contract drift.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional, Protocol
from uuid import UUID

import httpx

from lib.shared.circuit_breaker import AsyncCircuitBreaker, CircuitOpenError
from lib.shared.errors import DependencyUnavailableError
from lib.shared.env import is_prod
from lib.shared.http_retry import (
    HttpRetryConfig,
    SleepFn,
    is_retryable_httpx_error,
    sleep_before_retry,
)

log = logging.getLogger(__name__)


def _record_rendering_breaker_exception(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return is_retryable_httpx_error(exc)
    return True


# ---------------------------------------------------------------------
# Rendering adapter
# ---------------------------------------------------------------------


@dataclass
class RenderRequest:
    """Contract §2.1/§2.2 — `RenderConversationTurnRequest`. We pass
    the entire StrategyResult payload shape; Agent-RND documents the
    authoritative fields. Anything unexpected is ignored server-side
    per their convention."""
    tenant_id: UUID
    query: str
    category: str
    conversation_history: list[dict[str, Any]] = field(default_factory=list)
    card_context: Optional[dict[str, Any]] = None
    context_bundle: dict[str, Any] = field(default_factory=dict)
    strategy_notes: dict[str, Any] = field(default_factory=dict)
    retrieval_trace: dict[str, Any] = field(default_factory=dict)


@dataclass
class RenderResponse:
    """Contract §2.2."""
    response_html: str
    rendering_model_used: str
    cost_usd: Decimal


class RenderingAdapter(Protocol):
    async def render_conversation_turn(
        self, req: RenderRequest
    ) -> RenderResponse: ...


class MockRenderingAdapter:
    """Stand-in while Agent-RND ships `services/product/rendering/`.

    We build a deterministic HTML stub that surfaces the
    highest-scored Models + a short header so integration tests have
    something to assert against. The voice rules Agent-RND will apply
    are NOT enforced here — this is a placeholder, not a substitute.
    """

    def __init__(self, *, simulated_latency_ms: int = 50) -> None:
        self._latency_ms = simulated_latency_ms

    async def render_conversation_turn(
        self, req: RenderRequest
    ) -> RenderResponse:
        # Simulate realistic latency so prefetch vs non-prefetched
        # benchmarks aren't no-ops in unit tests.
        if self._latency_ms > 0:
            await asyncio.sleep(self._latency_ms / 1000.0)

        # Short HTML with the inline span classes Agent-UI expects.
        header = (
            f'<p class="meta"><span class="n">{req.category}</span> '
            f"response to: "
            f'<span class="serif">{_escape(req.query)}</span></p>'
        )
        bundle = req.context_bundle or {}
        models_line = ""
        mdls = bundle.get("models", []) or []
        if mdls:
            first = mdls[0]
            prop = first.get("proposition") or first.get("natural") or ""
            models_line = (
                f'<p><span class="serif">Leading model:</span> '
                f"{_escape(str(prop))[:280]}.</p>"
            )
        tail = (
            f'<p class="n muted">'
            f"models={len(mdls)} "
            f"observations={len(bundle.get('observations', []) or [])} "
            f"commitments={len(bundle.get('acts_summary', {}).get('commitments', []) or [])}"
            "</p>"
        )
        html = header + models_line + tail
        return RenderResponse(
            response_html=html,
            rendering_model_used="mock-rendering",
            cost_usd=Decimal("0"),
        )


class HttpRenderingAdapter:
    """Hits `POST /rendering/conversation-turn` per CONTRACTS §2.1.

    Week-4 integration note: RND's wire schema lives in
    `services/product/rendering/api.py::ConversationTurnRequestBody` —
    `{tenant_id, timestamp, query, retrieval_context, substrate_state?,
     conversation_history[{role,text}], founder_context?}`. This adapter
    maps QRY's internal `RenderRequest` shape to that wire.
    """

    def __init__(
        self,
        *,
        base_url: str,
        timeout_s: float = 60.0,
        auth_token: Optional[str] = None,
        client: httpx.AsyncClient | None = None,
        retry_config: HttpRetryConfig | None = None,
        breaker: AsyncCircuitBreaker | None = None,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s
        self._auth = auth_token
        self._client = client
        self._owned_client: httpx.AsyncClient | None = None
        self._retry_config = retry_config or HttpRetryConfig()
        self._breaker = breaker or AsyncCircuitBreaker(
            name="query_rendering",
            record_exception=_record_rendering_breaker_exception,
        )
        self._sleep = sleep

    def _client_for_request(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        if self._owned_client is None:
            self._owned_client = httpx.AsyncClient(timeout=self._timeout)
        return self._owned_client

    async def aclose(self) -> None:
        if self._owned_client is not None:
            await self._owned_client.aclose()
            self._owned_client = None

    def _unavailable_error(
        self,
        *,
        url: str,
        exc: BaseException,
        attempts: int | None = None,
        circuit_open: bool = False,
    ) -> DependencyUnavailableError:
        status_code = (
            exc.response.status_code if isinstance(exc, httpx.HTTPStatusError)
            else None
        )
        context: dict[str, Any] = {
            "attempts": attempts
            if attempts is not None
            else self._retry_config.max_attempts,
            "status_code": status_code,
            "url": url,
            "cause_type": type(exc).__name__,
        }
        if circuit_open:
            context["circuit_open"] = True
            if isinstance(exc, CircuitOpenError):
                context["breaker"] = exc.context.get("breaker")
        return DependencyUnavailableError(
            "rendering",
            "conversation-turn",
            **context,
        )

    async def render_conversation_turn(
        self, req: RenderRequest
    ) -> RenderResponse:
        from datetime import datetime, timezone

        headers = {"content-type": "application/json"}
        if self._auth:
            headers["authorization"] = f"Bearer {self._auth}"

        # Conversation-history: QRY stores dicts with `query` +
        # `response_html`. RND expects alternating {role, text}.
        rnd_history: list[dict[str, str]] = []
        for turn in req.conversation_history or []:
            q = str(turn.get("query", "")).strip()
            a = str(turn.get("response_html", "")).strip()
            if q:
                rnd_history.append({"role": "founder", "text": q})
            if a:
                rnd_history.append({"role": "system", "text": a})

        # Retrieval_context: fold QRY's context_bundle + strategy_notes +
        # retrieval_trace + card_context into a single dict — RND's
        # prompt reads it structurally. Category is passed through.
        retrieval_context: dict[str, Any] = dict(req.context_bundle or {})
        retrieval_context["_category"] = req.category
        if req.strategy_notes:
            retrieval_context["_strategy_notes"] = req.strategy_notes
        if req.retrieval_trace:
            retrieval_context["_retrieval_trace"] = req.retrieval_trace
        if req.card_context:
            retrieval_context["_card_context"] = req.card_context

        payload = {
            "tenant_id": str(req.tenant_id),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": req.query,
            "retrieval_context": retrieval_context,
            "conversation_history": rnd_history,
        }
        url = f"{self._base_url}/rendering/conversation-turn"
        try:
            body = await self._breaker.call(
                lambda: self._post_json_with_retries(
                    self._client_for_request(),
                    url=url,
                    payload=payload,
                    headers=headers,
                )
            )
        except CircuitOpenError as exc:
            raise self._unavailable_error(
                url=url,
                exc=exc,
                attempts=0,
                circuit_open=True,
            ) from exc
        return RenderResponse(
            response_html=str(body.get("response_html", "")),
            rendering_model_used=str(
                body.get("rendering_model_used") or "unknown"
            ),
            cost_usd=Decimal(str(body.get("cost_usd") or "0")),
        )

    async def _post_json_with_retries(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        last_error: BaseException | None = None
        for attempt_index in range(self._retry_config.max_attempts):
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                if not is_retryable_httpx_error(exc):
                    raise
                last_error = exc
                if attempt_index >= self._retry_config.max_attempts - 1:
                    break
                await sleep_before_retry(
                    attempt_index=attempt_index,
                    config=self._retry_config,
                    sleep=self._sleep,
                )
        assert last_error is not None
        raise self._unavailable_error(url=url, exc=last_error) from last_error


def build_rendering_adapter() -> RenderingAdapter:
    """Factory. Controlled by env `QUERY_RENDERING_BASE_URL`. When
    unset we return the mock in dev/test and fail closed in production."""
    base = os.environ.get("QUERY_RENDERING_BASE_URL")
    if not base:
        message = (
            "QUERY_RENDERING_BASE_URL is unset; query rendering is using "
            "MockRenderingAdapter"
        )
        if is_prod():
            raise RuntimeError(message)
        log.warning(message)
        return MockRenderingAdapter()
    return HttpRenderingAdapter(
        base_url=base,
        auth_token=os.environ.get("QUERY_RENDERING_AUTH_TOKEN"),
    )


def _escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ---------------------------------------------------------------------
# Cache adapter
# ---------------------------------------------------------------------


class CacheAdapter(Protocol):
    async def get(self, tenant_id: UUID, key: str) -> Optional[dict[str, Any]]: ...
    async def set(
        self,
        tenant_id: UUID,
        key: str,
        content: dict[str, Any],
        *,
        reason: str = "scheduled",
    ) -> None: ...
    async def invalidate(self, tenant_id: UUID, key: str) -> None: ...


@dataclass
class _CacheRow:
    content: dict[str, Any]
    cached_at: float
    reason: str


class InMemoryCacheAdapter:
    """Default cache adapter. Keeps entries in a process-local dict so
    tests + dogfood work until Agent-GRT's migration lands."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], _CacheRow] = {}
        self._lock = asyncio.Lock()

    async def get(self, tenant_id: UUID, key: str) -> Optional[dict[str, Any]]:
        async with self._lock:
            row = self._store.get((str(tenant_id), key))
            if row is None:
                return None
            return {
                "content": row.content,
                "cached_at": row.cached_at,
                "reason": row.reason,
            }

    async def set(
        self,
        tenant_id: UUID,
        key: str,
        content: dict[str, Any],
        *,
        reason: str = "scheduled",
    ) -> None:
        async with self._lock:
            self._store[(str(tenant_id), key)] = _CacheRow(
                content=content,
                cached_at=time.time(),
                reason=reason,
            )

    async def invalidate(self, tenant_id: UUID, key: str) -> None:
        async with self._lock:
            self._store.pop((str(tenant_id), key), None)

    async def clear_all(self) -> None:
        async with self._lock:
            self._store.clear()


class PostgresCacheAdapter:
    """Writes to the shared `view_ceo_cache` table. Only enable once
    Agent-GRT's migration lands. Until then the factory returns the
    in-memory adapter.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def get(self, tenant_id: UUID, key: str) -> Optional[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT cached_content, cached_at, recomputed_reason
                FROM view_ceo_cache
                WHERE tenant_id = $1 AND cache_key = $2
                """,
                tenant_id, key,
            )
            if row is None:
                return None
            raw = row["cached_content"]
            content = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            return {
                "content": content,
                "cached_at": row["cached_at"].timestamp(),
                "reason": row["recomputed_reason"],
            }

    async def set(
        self,
        tenant_id: UUID,
        key: str,
        content: dict[str, Any],
        *,
        reason: str = "scheduled",
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO view_ceo_cache
                    (tenant_id, cache_key, cached_content, cached_at, recomputed_reason)
                VALUES ($1, $2, $3::jsonb, now(), $4)
                ON CONFLICT (tenant_id, cache_key) DO UPDATE
                  SET cached_content = EXCLUDED.cached_content,
                      cached_at = EXCLUDED.cached_at,
                      recomputed_reason = EXCLUDED.recomputed_reason
                """,
                tenant_id, key, json.dumps(content), reason,
            )

    async def invalidate(self, tenant_id: UUID, key: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM view_ceo_cache WHERE tenant_id = $1 AND cache_key = $2",
                tenant_id, key,
            )


_DEFAULT_CACHE = InMemoryCacheAdapter()


def build_cache_adapter(pool: Any = None) -> CacheAdapter:
    """Factory. If `QUERY_CACHE_BACKEND=pg` and a pool is supplied, use
    PostgresCacheAdapter. Dev/test may fall back to the in-memory stub, but
    production fails closed so Query/Ask cache state is shared across workers.
    """
    backend = os.environ.get("QUERY_CACHE_BACKEND", "memory").strip().lower()
    if backend == "pg":
        if pool is not None:
            return PostgresCacheAdapter(pool)
        message = "QUERY_CACHE_BACKEND=pg requires a database pool"
        if is_prod():
            raise RuntimeError(message)
        log.warning(message)
        return _DEFAULT_CACHE
    if is_prod():
        raise RuntimeError(
            "QUERY_CACHE_BACKEND must be 'pg' in production; refusing "
            "process-local InMemoryCacheAdapter"
        )
    if backend != "memory":
        log.warning(
            "unknown QUERY_CACHE_BACKEND=%r; using InMemoryCacheAdapter",
            backend,
        )
    return _DEFAULT_CACHE


def get_default_cache_adapter() -> CacheAdapter:
    """Return the module-level in-memory adapter. Shared across the
    classifier, handler, and prefetch by default so keys are visible
    in one place in unit tests."""
    return _DEFAULT_CACHE


__all__ = [
    "RenderRequest",
    "RenderResponse",
    "RenderingAdapter",
    "MockRenderingAdapter",
    "HttpRenderingAdapter",
    "build_rendering_adapter",
    "CacheAdapter",
    "InMemoryCacheAdapter",
    "PostgresCacheAdapter",
    "build_cache_adapter",
    "get_default_cache_adapter",
]
