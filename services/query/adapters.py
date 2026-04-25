"""
services/query/adapters.py — pluggable adapters for services we depend
on that may not yet exist on the dogfood branch.

Two seams:

  1. RenderingAdapter — calls `services/rendering/` via HTTP per
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

log = logging.getLogger(__name__)


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
    """Stand-in while Agent-RND ships `services/rendering/`.

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
    `services/rendering/api.py::ConversationTurnRequestBody` —
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
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_s
        self._auth = auth_token

    async def render_conversation_turn(
        self, req: RenderRequest
    ) -> RenderResponse:
        import httpx  # local import
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
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            body = r.json()
        return RenderResponse(
            response_html=str(body.get("response_html", "")),
            rendering_model_used=str(
                body.get("rendering_model_used") or "unknown"
            ),
            cost_usd=Decimal(str(body.get("cost_usd") or "0")),
        )


def build_rendering_adapter() -> RenderingAdapter:
    """Factory. Controlled by env `QUERY_RENDERING_BASE_URL`. When
    unset we return the mock so local dev just works."""
    base = os.environ.get("QUERY_RENDERING_BASE_URL")
    if not base:
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
    PostgresCacheAdapter. Otherwise the in-memory stub."""
    backend = os.environ.get("QUERY_CACHE_BACKEND", "memory")
    if backend == "pg" and pool is not None:
        return PostgresCacheAdapter(pool)
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
