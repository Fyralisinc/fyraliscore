"""Gateway routes for the Structure page overlays."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from services.app.gateway.artifact_drawers import fetch_commitment_overlay
from services.app.gateway.auth import AuthContext


def build_structure_router() -> APIRouter:
    router = APIRouter(tags=["structure"])

    @router.get("/v1/structure/overlay/{commitment_id}")
    async def structure_overlay_endpoint(
        commitment_id: str,
        request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            cid = UUID(commitment_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_commitment_id"}, status_code=400
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            bundle = await fetch_commitment_overlay(
                cid,
                auth.tenant_id,
                conn,
            )
        if bundle is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(bundle, status_code=200)

    @router.get("/v1/structure/recent")
    async def structure_recent_endpoint(
        request: Request,
        since_minutes: int = 10,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")

        try:
            raw_minutes = int(since_minutes)
        except (ValueError, TypeError):
            raw_minutes = 10
        all_active = raw_minutes <= 0
        window_minutes = max(1, min(1440 * 365, raw_minutes))

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            if all_active:
                rows = await conn.fetch(
                    "SELECT id FROM commitments "
                    "WHERE tenant_id = $1 "
                    "  AND terminal_at IS NULL "
                    "ORDER BY last_state_change_at DESC NULLS LAST, "
                    "         created_at DESC "
                    "LIMIT 500",
                    auth.tenant_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id FROM commitments "
                    "WHERE tenant_id = $1 "
                    "  AND ( "
                    "    created_at >= now() - ($2 || ' minutes')::interval "
                    "    OR last_state_change_at >= now() - ($2 || ' minutes')::interval "
                    "  ) "
                    "  AND terminal_at IS NULL "
                    "ORDER BY last_state_change_at DESC NULLS LAST, "
                    "         created_at DESC "
                    "LIMIT 500",
                    auth.tenant_id,
                    str(window_minutes),
                )

            commitments_payload: list[dict[str, Any]] = []
            goals_by_id: dict[str, dict[str, Any]] = {}
            people_by_id: dict[str, dict[str, Any]] = {}
            customers_by_id: dict[str, dict[str, Any]] = {}
            decisions_by_id: dict[str, dict[str, Any]] = {}
            resources_by_id: dict[str, dict[str, Any]] = {}

            for r in rows:
                cid = r["id"]
                bundle = await fetch_commitment_overlay(
                    cid,
                    auth.tenant_id,
                    conn,
                )
                if bundle is None:
                    continue
                commitments_payload.append(bundle["commitment"])
                for g in bundle["goals"]:
                    goals_by_id.setdefault(g["id"], g)
                for p in bundle["people"]:
                    people_by_id.setdefault(p["id"], p)
                for c in bundle["customers"]:
                    customers_by_id.setdefault(c["id"], c)
                for d in bundle.get("decisions", []):
                    decisions_by_id.setdefault(d["id"], d)
                for rs in bundle.get("resources", []):
                    rid = rs["id"]
                    if rid not in resources_by_id:
                        resources_by_id[rid] = {
                            "id": rid,
                            "label": rs["label"],
                            "kind": rs["kind"],
                            "unit": rs.get("unit"),
                        }

            goal_all_rows = await conn.fetch(
                "SELECT id, title, altitude, parent_goal_id FROM goals "
                "WHERE tenant_id = $1 AND archived_at IS NULL "
                "ORDER BY altitude, title "
                "LIMIT 200",
                auth.tenant_id,
            )
            for gr in goal_all_rows:
                gid = str(gr["id"])
                if gid in goals_by_id:
                    continue
                altitude = (
                    gr["altitude"]
                    if gr["altitude"] in ("strategic", "operational")
                    else "operational"
                )
                goals_by_id[gid] = {
                    "id": gid,
                    "label": gr["title"],
                    "altitude": altitude,
                    "parent_goal_id": (
                        str(gr["parent_goal_id"]) if gr["parent_goal_id"] else None
                    ),
                }

            actor_rows = await conn.fetch(
                "SELECT id, display_name, metadata FROM actors "
                "WHERE tenant_id = $1 AND status = 'active' "
                "  AND type IN ('human_internal', 'human') "
                "ORDER BY display_name "
                "LIMIT 80",
                auth.tenant_id,
            )
            for ar in actor_rows:
                aid = str(ar["id"])
                md = ar["metadata"]
                if isinstance(md, str):
                    try:
                        md = json.loads(md)
                    except json.JSONDecodeError:
                        md = {}
                elif not isinstance(md, dict):
                    md = {}
                role = md.get("title") or md.get("role") or "Team member"
                people_by_id[aid] = {
                    "id": aid,
                    "label": ar["display_name"],
                    "role": role,
                }

        return JSONResponse(
            {
                "commitments": commitments_payload,
                "goals": list(goals_by_id.values()),
                "people": list(people_by_id.values()),
                "customers": list(customers_by_id.values()),
                "decisions": list(decisions_by_id.values()),
                "resources": list(resources_by_id.values()),
            },
            status_code=200,
        )

    @router.get("/v1/structure/resources/aggregate")
    async def structure_resources_aggregate(request: Request) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            res_rows = await conn.fetch(
                "SELECT id, kind, identity, description, current_value, "
                "       utilization_state, controllability, metadata "
                "FROM resources "
                "WHERE tenant_id = $1 "
                "  AND archived_at IS NULL "
                "  AND kind IN ('human', 'financial', 'technical', 'time') "
                "ORDER BY kind, identity",
                auth.tenant_id,
            )

            resources_payload: list[dict[str, Any]] = []
            for r in res_rows:
                cv = r["current_value"]
                if isinstance(cv, str):
                    try:
                        cv = json.loads(cv)
                    except json.JSONDecodeError:
                        cv = {}
                if not isinstance(cv, dict):
                    cv = {}
                md = r["metadata"]
                if isinstance(md, str):
                    try:
                        md = json.loads(md)
                    except json.JSONDecodeError:
                        md = {}
                if not isinstance(md, dict):
                    md = {}

                capacity = cv.get("capacity")
                unit = cv.get("unit") or ""
                label = (
                    cv.get("label")
                    or md.get("label")
                    or r["identity"]
                    or "Resource"
                )

                deployed_row = await conn.fetchrow(
                    "SELECT COALESCE(SUM((deployed_quantity->>'value')::float), 0) AS total, "
                    "       COUNT(*) AS deployments "
                    "FROM resource_deployments rd "
                    "JOIN commitments c ON c.id = rd.commitment_id "
                    "WHERE rd.resource_id = $1 "
                    "  AND rd.released_at IS NULL "
                    "  AND c.tenant_id = $2 "
                    "  AND c.terminal_at IS NULL",
                    r["id"],
                    auth.tenant_id,
                )
                total_deployed = float(deployed_row["total"] or 0.0)
                deployments_count = int(deployed_row["deployments"] or 0)

                cap = (
                    float(capacity)
                    if isinstance(capacity, (int, float))
                    else 0.0
                )
                util_pct = (total_deployed / cap * 100.0) if cap > 0 else 0.0

                top_rows = await conn.fetch(
                    "SELECT c.id, c.title, c.state, c.owner_id, "
                    "       (rd.deployed_quantity->>'value')::float AS qty "
                    "FROM resource_deployments rd "
                    "JOIN commitments c ON c.id = rd.commitment_id "
                    "WHERE rd.resource_id = $1 "
                    "  AND rd.released_at IS NULL "
                    "  AND c.tenant_id = $2 "
                    "  AND c.terminal_at IS NULL "
                    "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
                    "LIMIT 5",
                    r["id"],
                    auth.tenant_id,
                )
                top_consumers: list[dict[str, Any]] = []
                for tr in top_rows:
                    top_consumers.append(
                        {
                            "commitment_id": str(tr["id"]),
                            "label": tr["title"] or "(untitled)",
                            "state": tr["state"],
                            "owner_id": (
                                str(tr["owner_id"]) if tr["owner_id"] else None
                            ),
                            "deployed_quantity": float(tr["qty"] or 0.0),
                        }
                    )

                if util_pct >= 100.0:
                    health = "over-allocated"
                elif util_pct >= 85.0:
                    health = "constrained"
                elif util_pct >= 50.0:
                    health = "deployed"
                elif util_pct > 0:
                    health = "under-utilized"
                else:
                    health = "available"

                resources_payload.append(
                    {
                        "id": str(r["id"]),
                        "kind": r["kind"],
                        "identity": r["identity"],
                        "label": label,
                        "description": r["description"] or "",
                        "capacity": cap,
                        "unit": unit,
                        "deployed": total_deployed,
                        "available": max(0.0, cap - total_deployed),
                        "utilization_pct": util_pct,
                        "deployments_count": deployments_count,
                        "health": health,
                        "category": md.get("category"),
                        "top_consumers": top_consumers,
                    }
                )

        return JSONResponse({"resources": resources_payload}, status_code=200)

    @router.get("/v1/structure/resources/{rid}/overlay")
    async def structure_resource_overlay(
        rid: str,
        request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            resource_uuid = UUID(rid)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_resource_id"}, status_code=400
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            r = await conn.fetchrow(
                "SELECT id, kind, identity, description, current_value, "
                "       utilization_state, metadata "
                "FROM resources "
                "WHERE id = $1 AND tenant_id = $2 "
                "  AND archived_at IS NULL",
                resource_uuid,
                auth.tenant_id,
            )
            if r is None:
                return JSONResponse({"error": "not_found"}, status_code=404)

            cv = r["current_value"]
            if isinstance(cv, str):
                try:
                    cv = json.loads(cv)
                except json.JSONDecodeError:
                    cv = {}
            if not isinstance(cv, dict):
                cv = {}
            md = r["metadata"]
            if isinstance(md, str):
                try:
                    md = json.loads(md)
                except json.JSONDecodeError:
                    md = {}
            if not isinstance(md, dict):
                md = {}

            consumers = await conn.fetch(
                "SELECT c.id, c.title, c.state, c.owner_id, c.due_date, "
                "       (rd.deployed_quantity->>'value')::float AS qty "
                "FROM resource_deployments rd "
                "JOIN commitments c ON c.id = rd.commitment_id "
                "WHERE rd.resource_id = $1 "
                "  AND rd.released_at IS NULL "
                "  AND c.tenant_id = $2 "
                "  AND c.terminal_at IS NULL "
                "ORDER BY (rd.deployed_quantity->>'value')::float DESC NULLS LAST "
                "LIMIT 80",
                resource_uuid,
                auth.tenant_id,
            )

            consumers_payload: list[dict[str, Any]] = []
            owner_ids: set[UUID] = set()
            for cr in consumers:
                if cr["owner_id"] is not None:
                    owner_ids.add(cr["owner_id"])
                consumers_payload.append(
                    {
                        "id": str(cr["id"]),
                        "label": cr["title"] or "(untitled)",
                        "state": cr["state"],
                        "owner_id": (
                            str(cr["owner_id"]) if cr["owner_id"] else None
                        ),
                        "due_date": (
                            cr["due_date"].date().isoformat()
                            if cr["due_date"] is not None
                            else None
                        ),
                        "deployed_quantity": float(cr["qty"] or 0.0),
                    }
                )

            owners_payload: list[dict[str, Any]] = []
            if owner_ids:
                owner_rows = await conn.fetch(
                    "SELECT id, display_name, metadata FROM actors "
                    "WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
                    auth.tenant_id,
                    list(owner_ids),
                )
                for orow in owner_rows:
                    md_o = orow["metadata"]
                    if isinstance(md_o, str):
                        try:
                            md_o = json.loads(md_o)
                        except json.JSONDecodeError:
                            md_o = {}
                    if not isinstance(md_o, dict):
                        md_o = {}
                    role = md_o.get("title") or md_o.get("role") or "Team member"
                    owners_payload.append(
                        {
                            "id": str(orow["id"]),
                            "label": orow["display_name"],
                            "role": role,
                        }
                    )

            cap = float(cv.get("capacity") or 0.0)
            total_deployed = sum(c["deployed_quantity"] for c in consumers_payload)
            util_pct = (total_deployed / cap * 100.0) if cap > 0 else 0.0

        return JSONResponse(
            {
                "resource": {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "identity": r["identity"],
                    "label": cv.get("label") or md.get("label") or r["identity"],
                    "description": r["description"] or "",
                    "capacity": cap,
                    "unit": cv.get("unit") or "",
                    "deployed": total_deployed,
                    "utilization_pct": util_pct,
                    "category": md.get("category"),
                },
                "consumers": consumers_payload,
                "owners": owners_payload,
            },
            status_code=200,
        )

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
