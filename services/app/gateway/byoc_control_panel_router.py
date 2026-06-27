"""Bearer-authenticated BYOC control-panel read proxy."""
from __future__ import annotations

from collections.abc import Iterable

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import ValidationError

from services.app.gateway.byoc_control_plane_router import (
    read_byoc_control_panel_state_from_request,
)
from services.platform.runtime.byoc_control_panel_access import (
    ByocControlPanelAccessGrant,
    ByocControlPanelAccessQuery,
    evaluate_byoc_control_panel_access,
)
from services.platform.runtime.byoc_control_panel_state import (
    ByocControlPanelState,
    ByocControlPanelStateQuery,
)


def build_byoc_control_panel_router() -> APIRouter:
    router = APIRouter(
        prefix="/byoc/control-panel",
        tags=["byoc-control-panel"],
    )

    @router.get("/state")
    async def get_browser_control_panel_state(
        request: Request,
        deployment_id: str,
        customer_id: str | None = None,
        recent_limit: int = 10,
    ) -> ByocControlPanelState:
        auth = _require_gateway_auth(request)
        try:
            access_query = ByocControlPanelAccessQuery(
                tenant_id=auth.tenant_id,
                deployment_id=deployment_id,
                customer_id=customer_id,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        decision = evaluate_byoc_control_panel_access(
            query=access_query,
            grants=_access_grants_from_state(request),
        )
        if not decision.allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"errors": [f"control_panel_access: {decision.reason_code}"]},
            )
        try:
            state_query = ByocControlPanelStateQuery(
                deployment_id=deployment_id,
                customer_id=decision.customer_id,
                recent_limit=recent_limit,
            )
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc
        return await read_byoc_control_panel_state_from_request(
            request,
            query=state_query,
        )

    return router


def _require_gateway_auth(request: Request):
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "missing_gateway_bearer_auth"},
        )
    return auth


def _access_grants_from_state(
    request: Request,
) -> tuple[ByocControlPanelAccessGrant, ...]:
    grants = getattr(request.app.state, "byoc_control_panel_access_grants", ())
    if isinstance(grants, Iterable):
        return tuple(grants)
    return ()


__all__ = ["build_byoc_control_panel_router"]
