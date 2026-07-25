"""Shared native-connect wrapper for provider OAuth install flows."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


OAuthHandoffBuilder = Callable[
    [UUID, Any, Request, dict[str, Any]],
    Awaitable[dict[str, Any]],
]


def build_oauth_native_connect_router(
    *,
    source: str,
    authorization_mode: str,
    provider_console_url: str,
    payload_fields: list[str],
    build_handoff: OAuthHandoffBuilder,
) -> APIRouter:
    """Expose uniform `/connect/*` endpoints for OAuth callback sources.

    OAuth providers still finalize through their provider callback. These
    endpoints give the BYOC browser agent the same preflight/finalize contract
    as token, DWD, gateway, and webhook sources: preflight validates runtime
    configuration; finalize either confirms the callback-created install row or
    returns the install URL/state for the admin-present approval flow.
    """

    router = APIRouter(prefix=f"/integrations/{source}", tags=[source])

    @router.post("/connect/preflight")
    async def connect_preflight(request: Request) -> JSONResponse:
        tenant_id = _tenant_from_request(request)
        pool = _pool_from_request(request)
        body = await _json_body(request)
        handoff = await build_handoff(tenant_id, pool, request, body)
        missing = list(handoff.get("missing_configuration") or [])
        if missing:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "source": source,
                    "state": "missing_runtime_configuration",
                    "missing_configuration": missing,
                    "message": (
                        "Customer-cloud runtime configuration is required "
                        "before the provider approval flow can open."
                    ),
                },
            )
        return JSONResponse(
            content=_handoff_payload(
                source=source,
                authorization_mode=authorization_mode,
                provider_console_url=provider_console_url,
                payload_fields=payload_fields,
                handoff=handoff,
                state="ready_for_provider_approval",
            )
        )

    @router.post("/connect/finalize")
    async def connect_finalize(request: Request) -> JSONResponse:
        tenant_id = _tenant_from_request(request)
        pool = _pool_from_request(request)
        body = await _json_body(request)
        installation_id = str(body.get("installation_id") or "").strip() or None
        install_row = (
            await _load_exact_install_row(
                pool,
                tenant_id=tenant_id,
                source=source,
                installation_id=installation_id,
            )
            if installation_id is not None
            else None
        )
        if install_row is not None:
            return JSONResponse(
                content={
                    "ok": True,
                    "source": source,
                    "state": "connected",
                    "installation_id": str(install_row["installation_id"]),
                    "enabled": bool(install_row["enabled"]),
                    "provider_callback_required": False,
                    "raw_secret_values_included": False,
                }
            )

        handoff = await build_handoff(tenant_id, pool, request, body)
        missing = list(handoff.get("missing_configuration") or [])
        if missing:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "source": source,
                    "state": "missing_runtime_configuration",
                    "missing_configuration": missing,
                    "message": (
                        "Customer-cloud runtime configuration is required "
                        "before the provider approval flow can open."
                    ),
                },
            )
        return JSONResponse(
            status_code=202,
            content=_handoff_payload(
                source=source,
                authorization_mode=authorization_mode,
                provider_console_url=provider_console_url,
                payload_fields=payload_fields,
                handoff=handoff,
                state="waiting_for_provider_callback",
            )
            | {
                "message": (
                    "Provider approval is prepared. The install row is written "
                    "only after the provider callback returns to the customer "
                    "cloud."
                )
            },
        )

    return router


def _handoff_payload(
    *,
    source: str,
    authorization_mode: str,
    provider_console_url: str,
    payload_fields: list[str],
    handoff: dict[str, Any],
    state: str,
) -> dict[str, Any]:
    return {
        "ok": True,
        "source": source,
        "state": state,
        "authorization_mode": authorization_mode,
        "install_url": handoff.get("install_url"),
        "oauth_redirect_url": handoff.get("oauth_redirect_url"),
        "events_request_url": handoff.get("events_request_url"),
        "provider_console_url": handoff.get("provider_console_url")
        or provider_console_url,
        "payload_fields": payload_fields,
        "provider_callback_required": True,
        "state_expires_in_seconds": 600,
        "raw_secret_values_included": False,
    }


async def _load_exact_install_row(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
    installation_id: str,
) -> Any:
    """Load the callback-created installation named by the request.

    ``(provider, installation_id)`` is unique in the database. Deliberately
    omit the former tenant-only "latest installation" fallback: a finalize
    request without provider installation identity starts/continues the
    provider handoff instead of accidentally confirming a sibling install.
    """

    return await pool.fetchrow(
        """
        SELECT installation_id, enabled, installed_at
          FROM provider_installations
         WHERE tenant_id = $1
           AND provider = $2
           AND installation_id = $3
        """,
        tenant_id,
        source,
        installation_id,
    )


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return {}
    return body if isinstance(body, dict) else {}


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    tid = auth.tenant_id
    return tid if isinstance(tid, UUID) else UUID(str(tid))


def _pool_from_request(request: Request) -> Any:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="database pool unavailable")
    return pool


__all__ = ["build_oauth_native_connect_router"]
