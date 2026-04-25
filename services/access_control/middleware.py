"""
services/access_control/middleware.py — Gateway decorator for route-
level access control.

Usage:

    from services.access_control.middleware import requires_access

    @app.get("/commitments/{cid}")
    @requires_access("commitment", lambda request: request.path_params["cid"])
    async def get_commitment(cid: str, request: Request): ...

The decorator:

  1. Extracts the tenant_id + actor_id from request.state.auth
     (BearerAuthMiddleware has already run).
  2. Resolves the target entity id via the `entity_resolver` callback.
  3. Calls `can_read_by_id` with the tenant-bound pool.
  4. On deny → returns 403 with a structured body carrying `reason`.
  5. On allow → forwards to the handler.

Returning a JSONResponse here is a FastAPI convention — the decorator
is compatible with both plain async handlers and FastAPI endpoint
dependency injection because we forward *args, **kwargs unchanged.
"""
from __future__ import annotations

import functools
import logging
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import Request
from fastapi.responses import JSONResponse

from .audit import record_override
from .checks import AccessDecision, can_read_by_id


log = logging.getLogger(__name__)


EntityResolver = Callable[[Request], Any]


def requires_access(
    entity_type: str,
    entity_resolver: EntityResolver,
) -> Callable[..., Any]:
    """Decorate a FastAPI route handler.

    `entity_resolver(request)` returns either a string/UUID entity id,
    or None/"" to skip the check (tenant-list endpoints).
    """
    def _wrap(func: Callable[..., Awaitable[Any]]):
        @functools.wraps(func)
        async def _inner(*args: Any, **kwargs: Any):
            request: Request | None = kwargs.get("request")
            if request is None:
                for a in args:
                    if isinstance(a, Request):
                        request = a
                        break
            if request is None:
                # Cannot resolve — fail closed.
                return JSONResponse(
                    {"error": "access_denied", "reason": "request_missing"},
                    status_code=403,
                )
            auth = getattr(request.state, "auth", None)
            if auth is None:
                return JSONResponse(
                    {"error": "access_denied", "reason": "unauthenticated"},
                    status_code=401,
                )
            raw_id = entity_resolver(request)
            if raw_id is None or raw_id == "":
                # Skip check — decorator treats this as "list endpoint".
                return await func(*args, **kwargs)
            try:
                entity_id = (
                    raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
                )
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "access_denied", "reason": "invalid_entity_id"},
                    status_code=400,
                )
            # Pool from app.state.deps — deferred import so access_control
            # doesn't pull FastAPI at module load time when unused.
            deps = getattr(request.app.state, "deps", None)
            if deps is None:
                return JSONResponse(
                    {"error": "access_denied", "reason": "deps_missing"},
                    status_code=500,
                )
            async with deps.pool.acquire() as conn:
                decision: AccessDecision = await can_read_by_id(
                    auth.actor_id,
                    entity_type,  # type: ignore[arg-type]
                    entity_id,
                    conn=conn,
                    tenant_id=auth.tenant_id,
                )
                if decision.allowed and decision.override_applied:
                    # Best-effort audit log.
                    await record_override(
                        auth.actor_id,
                        entity_type,
                        entity_id,
                        _map_reason_to_override_kind(decision.reason),
                        conn=conn,
                        tenant_id=auth.tenant_id,
                        reason=decision.reason,
                    )
            if not decision.allowed:
                log.info(
                    "access_denied",
                    extra={
                        "actor_id": str(auth.actor_id),
                        "entity_type": entity_type,
                        "entity_id": str(entity_id),
                        "reason": decision.reason,
                    },
                )
                return JSONResponse(
                    {
                        "error": "access_denied",
                        "reason": decision.reason,
                        "entity_type": entity_type,
                        "entity_id": str(entity_id),
                    },
                    status_code=403,
                )
            return await func(*args, **kwargs)
        return _inner
    return _wrap


def _map_reason_to_override_kind(reason: str) -> str:
    if reason == "admin_override":
        return "admin"
    if reason == "leadership_override":
        return "leadership"
    if reason == "model_self_scope":
        return "first_person"
    return "system"


__all__ = ["requires_access"]
