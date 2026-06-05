"""Legacy substrate list endpoints."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request


def build_substrate_router() -> APIRouter:
    router = APIRouter(tags=["substrate"])

    @router.get("/observations")
    async def get_observations(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        auth = request.state.auth
        deps = _deps(request)
        rows = await deps.pool.fetch(
            """
            SELECT id, kind, source_channel, occurred_at, content_text
            FROM observations
            WHERE tenant_id = $1
            ORDER BY occurred_at DESC
            LIMIT $2 OFFSET $3
            """,
            auth.tenant_id,
            _clip(limit, 1, 500),
            max(offset, 0),
        )
        return {
            "items": [
                {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "source_channel": r["source_channel"],
                    "occurred_at": r["occurred_at"].isoformat(),
                    "content_text": r["content_text"],
                }
                for r in rows
            ],
            "stub": True,
        }

    @router.get("/models")
    async def get_models(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "models",
            ("id", "proposition", "confidence", "status", "created_at"),
            limit,
            offset,
        )

    @router.get("/commitments")
    async def get_commitments(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "commitments",
            ("id", "title", "state", "owner_id", "due_date", "created_at"),
            limit,
            offset,
        )

    @router.get("/goals")
    async def get_goals(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "goals",
            ("id", "title", "state", "altitude", "cached_health", "created_at"),
            limit,
            offset,
        )

    @router.get("/decisions")
    async def get_decisions(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "decisions",
            ("id", "title", "state", "created_at"),
            limit,
            offset,
        )

    @router.get("/resources")
    async def get_resources(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            "resources",
            ("id", "kind", "identity", "utilization_state", "created_at"),
            limit,
            offset,
        )

    return router


async def _generic_list(
    request: Request,
    table: str,
    columns: tuple[str, ...],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    auth = request.state.auth
    deps = _deps(request)
    col_list = ", ".join(columns)
    query = (
        f"SELECT {col_list} FROM {table} "
        "WHERE tenant_id = $1 "
        "ORDER BY created_at DESC "
        "LIMIT $2 OFFSET $3"
    )
    rows = await deps.pool.fetch(
        query, auth.tenant_id, _clip(limit, 1, 500), max(offset, 0)
    )
    items: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {}
        for c in columns:
            v = r[c]
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            item[c] = str(v) if isinstance(v, UUID) else v
        items.append(item)
    return {"items": items, "stub": True}


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


def _clip(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))
