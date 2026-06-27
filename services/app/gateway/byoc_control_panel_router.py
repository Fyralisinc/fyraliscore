"""Bearer-authenticated BYOC control-panel read proxy."""
from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import ValidationError

from services.app.gateway.byoc_control_plane_router import (
    read_byoc_control_panel_state_from_request,
)
from services.platform.runtime.byoc_control_panel_access import (
    ByocControlPanelAccessGrant,
    ByocControlPanelAccessGrantList,
    ByocControlPanelAccessGrantStore,
    ByocControlPanelAccessQuery,
    InMemoryByocControlPanelAccessGrantStore,
    PostgresByocControlPanelAccessGrantStore,
    build_byoc_control_panel_access_grant_list,
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

    @router.get("/deployments")
    async def list_browser_control_panel_deployments(
        request: Request,
        customer_id: str | None = None,
    ) -> ByocControlPanelAccessGrantList:
        auth = _require_gateway_auth(request)
        try:
            grants = await _access_grants_for_tenant(
                request,
                tenant_id=auth.tenant_id,
                customer_id=customer_id,
            )
            return build_byoc_control_panel_access_grant_list(
                tenant_id=auth.tenant_id,
                customer_id=customer_id,
                grants=grants,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [str(exc)]},
            ) from exc
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"errors": [error["msg"] for error in exc.errors()]},
            ) from exc

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
            grants=await _access_grants_from_state(
                request,
                query=access_query,
            ),
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


async def _access_grants_for_tenant(
    request: Request,
    *,
    tenant_id: UUID,
    customer_id: str | None = None,
) -> tuple[ByocControlPanelAccessGrant, ...]:
    store = _access_grant_store_from_state(request)
    if store is not None:
        return await store.list_grants(
            tenant_id=tenant_id,
            customer_id=customer_id,
        )
    grants = getattr(request.app.state, "byoc_control_panel_access_grants", ())
    if isinstance(grants, Iterable):
        return tuple(grants)
    return ()


async def _access_grants_from_state(
    request: Request,
    *,
    query: ByocControlPanelAccessQuery,
) -> tuple[ByocControlPanelAccessGrant, ...]:
    store = _access_grant_store_from_state(request)
    if store is not None:
        return await store.list_grants(
            tenant_id=query.tenant_id,
            customer_id=query.customer_id,
            deployment_id=query.deployment_id,
        )
    grants = getattr(request.app.state, "byoc_control_panel_access_grants", ())
    if isinstance(grants, Iterable):
        return tuple(grants)
    return ()


def _access_grant_store_from_state(
    request: Request,
) -> ByocControlPanelAccessGrantStore | None:
    existing = getattr(
        request.app.state,
        "byoc_control_panel_access_grant_store",
        None,
    )
    if existing is not None:
        return existing
    grants = getattr(request.app.state, "byoc_control_panel_access_grants", None)
    if grants is not None:
        return None
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresByocControlPanelAccessGrantStore(pool)
        request.app.state.byoc_control_panel_access_grant_store = created
        return created
    created = InMemoryByocControlPanelAccessGrantStore()
    request.app.state.byoc_control_panel_access_grant_store = created
    return created


__all__ = ["build_byoc_control_panel_router"]
