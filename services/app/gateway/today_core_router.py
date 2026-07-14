"""Gateway routes for the legacy /v1 Today surface and artifact drawers."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.shared.ids import uuid7
from services.app.gateway.artifact_drawers import fetch_artifact
from services.app.gateway.auth import AuthContext
from services.platform.access_control.audit import (
    record_override_if_needed as record_access_override_if_needed,
)
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read_by_id,
)
from services.platform.access_control.authority import principal_for_actor
from services.platform.access_control.roles import has_role
from services.platform.operator_action_audit import record_operator_action


_ARTIFACT_ACCESS_KIND: dict[str, EntityKind] = {
    "commitment": "commitment",
    "goal": "goal",
    "decision": "decision",
    "resource": "resource",
    "observation": "observation",
    "model": "model",
}


def build_today_core_router() -> APIRouter:
    router = APIRouter(tags=["today-core"])

    @router.get("/v1/artifacts/{artifact_type}/{artifact_id}")
    async def get_artifact_endpoint(
        artifact_type: str,
        artifact_id: str,
        request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            aid = UUID(artifact_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_artifact_id"}, status_code=400
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            decision = await _can_read_artifact(
                auth,
                artifact_type,
                aid,
                conn=conn,
            )
            if not decision.allowed:
                if decision.reason == "entity_not_found":
                    return JSONResponse(
                        {"error": "not_found", "type": artifact_type},
                        status_code=404,
                    )
                return JSONResponse(
                    {
                        "error": "forbidden",
                        "reason": decision.reason,
                        "type": artifact_type,
                    },
                    status_code=status.HTTP_403_FORBIDDEN,
                )
            await _record_override_if_needed(
                decision,
                actor_id=auth.actor_id,
                entity_type=artifact_type,
                entity_id=aid,
                conn=conn,
                tenant_id=auth.tenant_id,
            )
            payload = await fetch_artifact(
                artifact_type,
                aid,
                auth.tenant_id,
                conn,
                actor_id=auth.actor_id,
            )
        if payload is None:
            return JSONResponse(
                {"error": "not_found", "type": artifact_type},
                status_code=404,
            )
        return JSONResponse(payload, status_code=200)

    @router.get("/v1/today")
    async def today_endpoint(request: Request) -> JSONResponse:
        from services.product.greeting import ViewerStateRepo
        from services.product.today import build_today

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")

        actor_param = request.query_params.get("actor_id")
        target_actor = auth.actor_id
        if actor_param is not None:
            try:
                target_actor = UUID(str(actor_param))
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "invalid_actor_id"}, status_code=400
                )
            if target_actor != auth.actor_id:
                return JSONResponse(
                    {
                        "error": "forbidden",
                        "reason": "cross_actor_access_not_supported",
                    },
                    status_code=status.HTTP_403_FORBIDDEN,
                )

        deps = _deps(request)
        display_name: str | None = None
        async with deps.pool.acquire() as conn:
            actor_row = await conn.fetchrow(
                "SELECT display_name FROM actors WHERE id = $1 AND tenant_id = $2",
                target_actor,
                auth.tenant_id,
            )
            if actor_row is not None:
                display_name = actor_row["display_name"]

            tenant_row = await conn.fetchrow(
                "SELECT min(ingested_at) AS first_seen FROM observations "
                "WHERE tenant_id = $1",
                auth.tenant_id,
            )
            days_since = 1
            if tenant_row and tenant_row["first_seen"] is not None:
                delta = datetime.now(timezone.utc) - tenant_row["first_seen"]
                days_since = max(1, int(delta.days) + 1)

            brand_row = await conn.fetchrow(
                "SELECT current_value FROM resources "
                "WHERE tenant_id = $1 AND kind = 'ip' "
                "  AND identity = 'fyralis.brand_name' "
                "  AND archived_at IS NULL "
                "ORDER BY last_updated_at DESC LIMIT 1",
                auth.tenant_id,
            )
            brand_name = "Fyralis"
            if brand_row is not None:
                cv = brand_row["current_value"] or {}
                if isinstance(cv, str):
                    try:
                        cv = json.loads(cv)
                    except json.JSONDecodeError:
                        cv = {}
                if isinstance(cv, dict) and isinstance(cv.get("name"), str):
                    brand_name = cv["name"]

            viewer_state_repo = ViewerStateRepo(deps.pool)
            previous_last_seen = await viewer_state_repo.upsert_last_seen(
                auth.tenant_id,
                str(target_actor),
                datetime.now(timezone.utc),
                conn=conn,
            )
            principal = await principal_for_actor(
                auth.actor_id,
                conn=conn,
                tenant_id=auth.tenant_id,
            )

            payload = await build_today(
                tenant_id=auth.tenant_id,
                actor_id=target_actor,
                actor_display_name=display_name,
                brand_name=brand_name,
                conn=conn,
                days_since_inception=days_since,
                previous_last_seen_at=previous_last_seen,
                principal=principal,
            )
        return JSONResponse(payload.to_dict(), status_code=200)

    @router.post("/v1/today/brand")
    async def today_brand_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        new_name = (body or {}).get("name")
        if not isinstance(new_name, str) or not new_name.strip():
            return JSONResponse({"error": "name_required"}, status_code=400)
        new_name = new_name.strip()[:64]

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            if not await _can_manage_brand(auth, conn=conn):
                return JSONResponse(
                    {
                        "error": "forbidden",
                        "reason": "brand_update_requires_admin_or_leadership",
                    },
                    status_code=status.HTTP_403_FORBIDDEN,
                )
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT id FROM resources "
                    "WHERE tenant_id = $1 AND kind = 'ip' "
                    "  AND identity = 'fyralis.brand_name' "
                    "  AND archived_at IS NULL",
                    auth.tenant_id,
                )
                if existing is None:
                    resource_id = uuid7()
                    await conn.execute(
                        """
                        INSERT INTO resources (
                            id, tenant_id, kind, identity, current_value,
                            created_at, last_updated_at
                        ) VALUES ($1, $2, 'ip', 'fyralis.brand_name',
                                  $3::jsonb, now(), now())
                        """,
                        resource_id,
                        auth.tenant_id,
                        json.dumps({"name": new_name}),
                    )
                else:
                    resource_id = existing["id"]
                    await conn.execute(
                        "UPDATE resources SET current_value = $2::jsonb, "
                        "last_updated_at = now() "
                        "WHERE id = $1 AND tenant_id = $3",
                        resource_id,
                        json.dumps({"name": new_name}),
                        auth.tenant_id,
                    )
                await record_operator_action(
                    conn,
                    tenant_id=auth.tenant_id,
                    actor_id=auth.actor_id,
                    action="today.brand.update",
                    resource_type="resource",
                    resource_id=resource_id,
                    metadata={"name": new_name},
                )
        return JSONResponse({"ok": True, "name": new_name}, status_code=200)

    return router


def _auth(request: Request) -> AuthContext | None:
    return getattr(request.state, "auth", None)


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


async def _can_manage_brand(auth: AuthContext, *, conn: Any) -> bool:
    return bool(
        await has_role(
            auth.actor_id,
            "admin",
            conn=conn,
            tenant_id=auth.tenant_id,
        )
        or await has_role(
            auth.actor_id,
            "leadership",
            conn=conn,
            tenant_id=auth.tenant_id,
        )
    )


async def _can_read_artifact(
    auth: AuthContext,
    artifact_type: str,
    artifact_id: UUID,
    *,
    conn: Any,
) -> AccessDecision:
    if artifact_type == "actor":
        exists = await conn.fetchval(
            "SELECT 1 FROM actors WHERE id = $1 AND tenant_id = $2",
            artifact_id,
            auth.tenant_id,
        )
        if not exists:
            return AccessDecision(False, "entity_not_found")
        if artifact_id == auth.actor_id:
            return AccessDecision(True, "actor_self")
        if await has_role(
            auth.actor_id,
            "admin",
            conn=conn,
            tenant_id=auth.tenant_id,
        ):
            return AccessDecision(True, "admin_override", override_applied=True)
        if await has_role(
            auth.actor_id,
            "leadership",
            conn=conn,
            tenant_id=auth.tenant_id,
        ):
            return AccessDecision(
                True,
                "leadership_override",
                override_applied=True,
            )
        return AccessDecision(False, "actor_out_of_scope")

    access_kind = _ARTIFACT_ACCESS_KIND.get(artifact_type)
    if access_kind is None:
        return AccessDecision(False, "entity_not_found")
    return await can_read_by_id(
        auth.actor_id,
        access_kind,
        artifact_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )


async def _record_override_if_needed(
    decision: AccessDecision,
    *,
    actor_id: UUID,
    entity_type: str,
    entity_id: UUID,
    conn: Any,
    tenant_id: UUID,
) -> None:
    await record_access_override_if_needed(
        decision,
        actor_id=actor_id,
        entity_type=entity_type,
        entity_id=entity_id,
        conn=conn,
        tenant_id=tenant_id,
    )
