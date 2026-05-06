"""services/demo/router.py — FastAPI APIRouter for every demo endpoint.

Mounted by services/gateway/main.py via `_register_demo_routes(app)`.
All routes here live under `/v1/demo/*` plus the SSE endpoint at
`/v1/recommendations/stream` (kept on the recommendation namespace per
the build plan).

The router runs *behind* the gateway's BearerAuthMiddleware for
authenticated endpoints (sessions, simulator, reset, end). Two
endpoints are public:

  * `GET  /v1/demo/companies`            — list of demo companies for
                                            the picker page
  * `POST /v1/demo/sessions/start`        — provisions tenant + token

These are added to `_PUBLIC_PATH_PREFIXES` in gateway/main.py via the
`PUBLIC_DEMO_PREFIXES` constant exported from this module.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from services.demo.repo import (
    end_demo_session,
    get_demo_session,
    list_demo_configs,
    touch_demo_session,
)
from services.demo.sessions import end_session, reset_session, start_session
from services.demo.simulator import inject_signal, list_suggested_signals
from services.demo.sse import stream_for_actor


# Path prefixes the gateway should treat as unauthenticated. The
# picker page calls /v1/demo/companies (anonymous browsers) and
# /v1/demo/sessions/start (anonymous → mints session token).
PUBLIC_DEMO_PREFIXES: tuple[str, ...] = (
    "/v1/demo/companies",
    "/v1/demo/sessions/start",
)


log = structlog.get_logger("demo.router")


demo_router = APIRouter()


# ---------------------------------------------------------------------
# Picker — public
# ---------------------------------------------------------------------


@demo_router.get("/v1/demo/companies")
async def list_companies(request: Request) -> JSONResponse:
    """Return the three preloaded company cards for the picker."""
    deps = _deps(request)
    rows = await list_demo_configs(deps.pool)
    items = [
        {
            "company_id": r.company_id,
            "name": r.name,
            "tagline": r.tagline,
            "description": r.description,
        }
        for r in rows
    ]
    return JSONResponse({"items": items}, status_code=200)


# ---------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------


@demo_router.post("/v1/demo/sessions/start")
async def start(request: Request) -> JSONResponse:
    """Provision a fresh demo tenant + return an auth token bound to
    the CEO actor of that tenant. Public (no auth required) — anyone
    who lands on /demo can drop into a session."""
    deps = _deps(request)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    company_id = body.get("company_id") if isinstance(body, dict) else None
    if not isinstance(company_id, str) or company_id not in (
        "truss", "northwind", "meridian", "pelago"
    ):
        return JSONResponse(
            {"error": "invalid_company_id",
             "allowed": ["truss", "northwind", "meridian", "pelago"]},
            status_code=400,
        )

    try:
        result = await start_session(deps.pool, company_id=company_id)
    except Exception as e:  # noqa: BLE001
        log.error("demo_start_failed", error=str(e), error_type=type(e).__name__)
        return JSONResponse(
            {"error": "demo_start_failed", "detail": str(e)},
            status_code=500,
        )

    # Register the new tenant with the greeting scheduler so it receives
    # cache refreshes (and WebSocket pushes) after think runs.
    try:
        ceo_view = getattr(request.app.state, "ceo_view", None)
        if ceo_view:
            from services.greeting.snapshot import FounderContext
            scheduler = ceo_view["scheduler"]
            founder = FounderContext(
                tenant_id=result.tenant_id,
                role="ceo",
                display_name=company_id.capitalize(),
                timezone_name="UTC",
                observed_rhythms={},
            )
            scheduler.register_tenant(result.tenant_id, founder)
    except Exception as _reg_exc:  # noqa: BLE001
        log.warning("demo_scheduler_register_failed", error=str(_reg_exc))

    return JSONResponse(
        {
            "session_id": str(result.session_id),
            "tenant_id": str(result.tenant_id),
            "auth_token": result.auth_token,
            "auth_token_expires_at": result.auth_token_expires_at.isoformat(),
            "ceo_actor_id": str(result.ceo_actor_id),
            "company_id": result.company_id,
        },
        status_code=201,
    )


@demo_router.post("/v1/demo/sessions/{session_id}/end")
async def end_(session_id: str, request: Request) -> JSONResponse:
    """Mark a demo session ended. Authenticated."""
    deps = _deps(request)
    try:
        sid = UUID(session_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_session_id"}, status_code=400)
    auth = _require_auth(request)
    if auth is None:
        return _unauth()
    sess = await get_demo_session(deps.pool, sid)
    if sess is None or sess.tenant_id != auth.tenant_id:
        return JSONResponse({"error": "not_found"}, status_code=404)
    await end_session(deps.pool, session_id=sid, end_reason="user_ended")
    return JSONResponse({"ended": True}, status_code=200)


@demo_router.post("/v1/demo/sessions/{session_id}/reset")
async def reset(session_id: str, request: Request) -> JSONResponse:
    """Wipe + reload tenant state to its initial snapshot. Auth token
    stays valid because we keep the same tenant_id and CEO actor id."""
    deps = _deps(request)
    try:
        sid = UUID(session_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_session_id"}, status_code=400)
    auth = _require_auth(request)
    if auth is None:
        return _unauth()
    sess = await get_demo_session(deps.pool, sid)
    if sess is None or sess.tenant_id != auth.tenant_id:
        return JSONResponse({"error": "not_found"}, status_code=404)
    try:
        await reset_session(deps.pool, session_id=sid)
    except Exception as e:  # noqa: BLE001
        log.error("demo_reset_failed", error=str(e))
        return JSONResponse({"error": "reset_failed", "detail": str(e)}, status_code=500)
    return JSONResponse({"reset": True}, status_code=200)


@demo_router.get("/v1/demo/sessions/{session_id}")
async def get_session_info(session_id: str, request: Request) -> JSONResponse:
    """Surface the session's running cost / counters for the UI.

    Numeric fields (total_cost_usd, cost_cap_usd) are emitted as JSON
    numbers so the React client can call `.toFixed(2)` directly. The
    cost cap is read from the demo_config and surfaced here so the UI
    doesn't have to fetch the config separately.
    """
    deps = _deps(request)
    try:
        sid = UUID(session_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_session_id"}, status_code=400)
    auth = _require_auth(request)
    if auth is None:
        return _unauth()
    sess = await get_demo_session(deps.pool, sid)
    if sess is None or sess.tenant_id != auth.tenant_id:
        return JSONResponse({"error": "not_found"}, status_code=404)
    from services.demo.repo import get_demo_config_by_id

    cfg = await get_demo_config_by_id(deps.pool, sess.demo_config_id)
    return JSONResponse(
        {
            "id": str(sess.id),
            "tenant_id": str(sess.tenant_id),
            "demo_config_id": str(sess.demo_config_id),
            "ceo_actor_id": str(sess.ceo_actor_id) if sess.ceo_actor_id else None,
            "company_id": cfg.company_id if cfg else None,
            "started_at": sess.started_at.isoformat(),
            "last_active_at": sess.last_active_at.isoformat(),
            "ended_at": sess.ended_at.isoformat() if sess.ended_at else None,
            "total_cost_usd": float(sess.total_cost_usd),
            "cost_cap_usd": float(cfg.cost_cap_usd_per_session) if cfg else 0.0,
            "signals_injected": sess.signals_injected,
            "actions_taken": sess.actions_taken,
            "cost_cap_breached": sess.cost_cap_breached_at is not None,
        },
        status_code=200,
    )


# ---------------------------------------------------------------------
# Simulator — inject signals + suggested-signals catalog
# ---------------------------------------------------------------------


@demo_router.get("/v1/demo/simulator/suggested")
async def suggested_signals(request: Request) -> JSONResponse:
    """Per-company per-tab pre-canned signals for the simulator UI."""
    deps = _deps(request)
    auth = _require_auth(request)
    if auth is None:
        return _unauth()
    # Resolve the company_id via the session.
    from services.demo.repo import get_active_session_for_tenant, get_demo_config_by_id

    sess = await get_active_session_for_tenant(deps.pool, auth.tenant_id)
    if sess is None:
        return JSONResponse({"items": {}}, status_code=200)
    cfg = await get_demo_config_by_id(deps.pool, sess.demo_config_id)
    company_id = cfg.company_id if cfg else "northwind"
    return JSONResponse(
        {"company_id": company_id, "tabs": list_suggested_signals(company_id)},
        status_code=200,
    )


@demo_router.post("/v1/demo/simulator/inject")
async def inject(request: Request) -> JSONResponse:
    """Drop a signal into the substrate as if it came over the channel.

    Body: {channel: 'slack:message'|'email:message'|..., payload: {...}}
    """
    deps = _deps(request)
    auth = _require_auth(request)
    if auth is None:
        return _unauth()
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "expected_object"}, status_code=400)
    channel = body.get("channel")
    payload = body.get("payload")
    if not isinstance(channel, str) or not isinstance(payload, dict):
        return JSONResponse(
            {"error": "channel and payload required"},
            status_code=400,
        )
    from services.demo.repo import get_active_session_for_tenant

    sess = await get_active_session_for_tenant(deps.pool, auth.tenant_id)
    sid = sess.id if sess else None
    try:
        result = await inject_signal(
            pool=deps.pool,
            tenant_id=auth.tenant_id,
            actor_id=auth.actor_id,
            channel=channel,
            payload=payload,
            demo_session_id=sid,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
    except Exception as e:  # noqa: BLE001
        log.error(
            "demo_inject_failed",
            error=str(e),
            error_type=type(e).__name__,
            channel=channel,
        )
        return JSONResponse(
            {"error": "inject_failed", "detail": str(e)},
            status_code=400,
        )
    if sid is not None:
        async with deps.pool.acquire() as conn:
            await touch_demo_session(conn, sid)
    return JSONResponse(result, status_code=201)


# ---------------------------------------------------------------------
# SSE stream — recommendation lifecycle for the action list
# ---------------------------------------------------------------------


@demo_router.get("/v1/recommendations/stream")
async def recommendations_stream(request: Request) -> StreamingResponse:
    """SSE: pushes recommendation events for the authenticated actor.
    No body shape — the response is a stream of `data:` frames."""
    auth = _require_auth(request)
    if auth is None:
        return _unauth()  # type: ignore[return-value]
    actor_param = request.query_params.get("actor_id")
    if actor_param:
        try:
            actor = UUID(actor_param)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_actor_id"}, status_code=400,
            )  # type: ignore[return-value]
        if actor != auth.actor_id:
            return JSONResponse(
                {"error": "forbidden",
                 "reason": "cross_actor_subscription_not_supported"},
                status_code=403,
            )  # type: ignore[return-value]
    else:
        actor = auth.actor_id

    return StreamingResponse(
        stream_for_actor(tenant_id=auth.tenant_id, actor_id=actor),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",         # disables nginx buffering
        },
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised")
    return deps


def _require_auth(request: Request) -> Any:
    return getattr(request.state, "auth", None)


def _unauth() -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": "missing_bearer"},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


__all__ = ["demo_router", "PUBLIC_DEMO_PREFIXES"]
