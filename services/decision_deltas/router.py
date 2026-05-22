"""
services/decision_deltas/router.py — HTTP surface for Decision Deltas.

Endpoints (mounted at /v1/decision_deltas):

  GET    /                                    list (filtered)
  GET    /{delta_id}                          detail + evidence
  POST   /{delta_id}/accept                   accept + apply
  POST   /{delta_id}/delegate                 transition to delegated
  POST   /{delta_id}/contest                  transition to contested
  POST   /{delta_id}/add_context              evidence/notes addendum
  POST   /from_recommendation/{rec_id}        promotion bridge

Auth + tenant come from the gateway BearerAuthMiddleware
(`request.state.auth` = AuthContext). The router does not own the
DB pool — it pulls it off `request.app.state.deps`.

The router is NOT registered in services/gateway/main.py here (that
file is in this agent's forbidden zone). The registration line for the
gateway owner is documented in the agent report.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from lib.shared.errors import CompanyOSError, ValidationError
from services.decision_deltas import apply as apply_mod
from services.decision_deltas import promote as promote_mod
from services.decision_deltas import repo as dd_repo


log = logging.getLogger(__name__)


def build_router() -> APIRouter:
    router = APIRouter(
        prefix="/v1/decision_deltas",
        tags=["decision_deltas"],
    )

    # ----- Helpers -----------------------------------------------------

    def _auth(request: Request):
        auth = getattr(request.state, "auth", None)
        if auth is None:
            raise HTTPException(status_code=401, detail="unauthorized")
        return auth

    def _pool(request: Request) -> asyncpg.Pool:
        # Pull pool off the gateway deps without importing _deps()
        # (which would create a cycle on a partial main.py rebuild).
        deps = getattr(request.app.state, "deps", None)
        if deps is None or getattr(deps, "pool", None) is None:
            raise HTTPException(
                status_code=503, detail="pool_unavailable",
            )
        return deps.pool

    def _parse_uuid(raw: str, field: str = "id") -> UUID:
        try:
            return UUID(raw)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400, detail=f"invalid_{field}",
            ) from e

    # ----- LIST --------------------------------------------------------

    @router.get("/")
    async def list_route(request: Request) -> dict[str, Any]:
        auth = _auth(request)
        qp = request.query_params

        status_param = qp.get("status")
        target_kind = qp.get("target_kind")
        target_id_raw = qp.get("target_id")
        category = qp.get("category")
        limit_raw = qp.get("limit", "50")

        try:
            limit = max(1, min(200, int(limit_raw)))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="invalid_limit")

        target_id: UUID | None = None
        if target_id_raw:
            target_id = _parse_uuid(target_id_raw, "target_id")

        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                views = await dd_repo.list_deltas(
                    conn,
                    tenant_id=auth.tenant_id,
                    status=status_param if status_param else None,
                    target_kind=target_kind if target_kind else None,
                    target_id=target_id,
                    category=category if category else None,
                    limit=limit,
                )
        except ValidationError as e:
            raise HTTPException(
                status_code=400,
                detail={"error": "validation_error", "context": e.to_dict()},
            )
        return {
            "items": [_view_to_wire(v) for v in views],
            "count": len(views),
        }

    # ----- GET ONE -----------------------------------------------------

    @router.get("/{delta_id}")
    async def get_one(delta_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        did = _parse_uuid(delta_id, "delta_id")
        pool = _pool(request)
        async with pool.acquire() as conn:
            view = await dd_repo.get_delta(
                conn, tenant_id=auth.tenant_id, delta_id=did,
            )
        if view is None:
            raise HTTPException(status_code=404, detail="not_found")
        return _view_to_wire(view, with_evidence=True)

    # ----- ACCEPT ------------------------------------------------------

    @router.post("/{delta_id}/accept")
    async def accept_route(
        delta_id: str, request: Request,
    ) -> dict[str, Any]:
        auth = _auth(request)
        did = _parse_uuid(delta_id, "delta_id")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    view, triggered = await apply_mod.apply_acceptance(
                        conn=conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        user_id=auth.actor_id,
                    )
        except dd_repo.DeltaNotFoundError:
            raise HTTPException(status_code=404, detail="not_found")
        except dd_repo.InvalidStatusTransitionError as e:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "invalid_status_transition",
                    "context": e.to_dict(),
                },
            )
        except CompanyOSError as e:
            raise HTTPException(
                status_code=400,
                detail={"error": e.code, "context": e.to_dict()},
            )
        return {
            "delta": _view_to_wire(view, with_evidence=True),
            "triggered": triggered,
        }

    # ----- DELEGATE ----------------------------------------------------

    @router.post("/{delta_id}/delegate")
    async def delegate_route(
        delta_id: str, request: Request,
    ) -> dict[str, Any]:
        auth = _auth(request)
        did = _parse_uuid(delta_id, "delta_id")
        body = await _read_json(request)
        owner_raw = body.get("owner_id")
        if not isinstance(owner_raw, str) or not owner_raw.strip():
            raise HTTPException(status_code=400, detail="owner_id_required")
        owner_id = _parse_uuid(owner_raw, "owner_id")
        note = body.get("note")

        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    view = await dd_repo.update_status(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        status="delegated",
                        user_id=auth.actor_id,
                    )
                    # Record the delegation note + assignee inside the
                    # impact JSONB for the inspector. We don't create a
                    # new table for this in Phase 1.
                    await _annotate(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        patch={
                            "delegation": {
                                "owner_id": str(owner_id),
                                "note": (
                                    str(note).strip()
                                    if isinstance(note, str)
                                    else None
                                ),
                                "at": _now_iso(),
                            },
                        },
                    )
                    view = await dd_repo.get_delta(
                        conn, tenant_id=auth.tenant_id, delta_id=did,
                    )
        except dd_repo.DeltaNotFoundError:
            raise HTTPException(status_code=404, detail="not_found")
        except dd_repo.InvalidStatusTransitionError as e:
            raise HTTPException(
                status_code=409,
                detail={"error": "invalid_status_transition", "context": e.to_dict()},
            )
        except CompanyOSError as e:
            raise HTTPException(
                status_code=400,
                detail={"error": e.code, "context": e.to_dict()},
            )
        assert view is not None
        return {"delta": _view_to_wire(view, with_evidence=True)}

    # ----- CONTEST -----------------------------------------------------

    @router.post("/{delta_id}/contest")
    async def contest_route(
        delta_id: str, request: Request,
    ) -> dict[str, Any]:
        auth = _auth(request)
        did = _parse_uuid(delta_id, "delta_id")
        body = await _read_json(request)
        reason = body.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise HTTPException(status_code=400, detail="reason_required")

        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    view = await dd_repo.update_status(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        status="contested",
                        user_id=auth.actor_id,
                    )
                    await _annotate(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        patch={
                            "contest": {
                                "by": str(auth.actor_id),
                                "reason": reason.strip(),
                                "at": _now_iso(),
                            },
                        },
                    )
                    view = await dd_repo.get_delta(
                        conn, tenant_id=auth.tenant_id, delta_id=did,
                    )
        except dd_repo.DeltaNotFoundError:
            raise HTTPException(status_code=404, detail="not_found")
        except dd_repo.InvalidStatusTransitionError as e:
            raise HTTPException(
                status_code=409,
                detail={"error": "invalid_status_transition", "context": e.to_dict()},
            )
        assert view is not None
        return {"delta": _view_to_wire(view, with_evidence=True)}

    # ----- ADD CONTEXT -------------------------------------------------

    @router.post("/{delta_id}/add_context")
    async def add_context_route(
        delta_id: str, request: Request,
    ) -> dict[str, Any]:
        auth = _auth(request)
        did = _parse_uuid(delta_id, "delta_id")
        body = await _read_json(request)
        note = body.get("note")
        if not isinstance(note, str) or not note.strip():
            raise HTTPException(status_code=400, detail="note_required")

        pool = _pool(request)
        async with pool.acquire() as conn:
            current = await dd_repo.get_delta(
                conn, tenant_id=auth.tenant_id, delta_id=did,
            )
            if current is None:
                raise HTTPException(status_code=404, detail="not_found")
            async with conn.transaction():
                await _annotate(
                    conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    patch={
                        "context_notes": [
                            {
                                "by": str(auth.actor_id),
                                "note": note.strip(),
                                "at": _now_iso(),
                            }
                        ],
                    },
                    merge_lists=True,
                )
            view = await dd_repo.get_delta(
                conn, tenant_id=auth.tenant_id, delta_id=did,
            )
        assert view is not None
        return {"delta": _view_to_wire(view, with_evidence=True)}

    # ----- FROM RECOMMENDATION ----------------------------------------

    @router.post("/from_recommendation/{recommendation_id}")
    async def promote_route(
        recommendation_id: str, request: Request,
    ) -> dict[str, Any]:
        auth = _auth(request)
        rid = _parse_uuid(recommendation_id, "recommendation_id")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    delta_id = await promote_mod.promote_from_recommendation(
                        conn,
                        tenant_id=auth.tenant_id,
                        recommendation_id=rid,
                    )
                    view = await dd_repo.get_delta(
                        conn, tenant_id=auth.tenant_id, delta_id=delta_id,
                    )
        except ValidationError as e:
            raise HTTPException(
                status_code=400,
                detail={"error": e.code, "context": e.to_dict()},
            )
        except CompanyOSError as e:
            raise HTTPException(
                status_code=400,
                detail={"error": e.code, "context": e.to_dict()},
            )
        assert view is not None
        return {"delta": _view_to_wire(view, with_evidence=True)}

    return router


# ---------------------------------------------------------------------
# Wire-format helpers
# ---------------------------------------------------------------------


def _view_to_wire(
    view: dd_repo.DecisionDeltaView,
    *,
    with_evidence: bool = False,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(view.id),
        "tenant_id": str(view.tenant_id),
        "status": view.status,
        "label": view.label,
        "main_assertion": view.main_assertion,
        "current_state": view.current_state,
        "suggested_update": view.suggested_update,
        "target_node_kind": view.target_node_kind,
        "target_node_id": (
            str(view.target_node_id)
            if view.target_node_id else None
        ),
        "confidence": view.confidence,
        "confidence_basis": view.confidence_basis,
        "falsification_condition": view.falsification_condition,
        "consequence_preview": view.consequence_preview,
        "impact": view.impact,
        "category": view.category,
        "source_recommendation_id": (
            str(view.source_recommendation_id)
            if view.source_recommendation_id else None
        ),
        "created_at": _isofmt(view.created_at),
        "updated_at": _isofmt(view.updated_at),
        "accepted_at": _isofmt(view.accepted_at),
        "accepted_by": (
            str(view.accepted_by) if view.accepted_by else None
        ),
        "resolution_target_at": _isofmt(view.resolution_target_at),
    }
    if with_evidence:
        out["evidence"] = [
            {
                "id": str(e.id),
                "source": e.source,
                "title": e.title,
                "ts": _isofmt(e.ts),
                "trust_tier": e.trust_tier,
                "excerpt": e.excerpt,
                "weight": e.weight,
                "ordinal": e.ordinal,
            }
            for e in view.evidence
        ]
    return out


def _isofmt(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _read_json(request: Request) -> dict[str, Any]:
    try:
        body = await request.body()
        if not body:
            return {}
        parsed = json.loads(body)
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail="invalid_json") from e
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="invalid_body")
    return parsed


async def _annotate(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    patch: dict[str, Any],
    merge_lists: bool = False,
) -> None:
    """Merge `patch` into the delta's `impact` JSONB.

    When `merge_lists=True`, list values are appended to the existing
    list under the same key (used for context_notes which grows over
    time). Otherwise the patch replaces the key value.
    """
    if not patch:
        return
    row = await conn.fetchrow(
        "SELECT impact FROM decision_deltas "
        "WHERE id = $1 AND tenant_id = $2",
        delta_id, tenant_id,
    )
    if row is None:
        return
    existing_raw = row["impact"]
    if existing_raw is None:
        existing: dict[str, Any] = {}
    elif isinstance(existing_raw, dict):
        existing = dict(existing_raw)
    else:
        try:
            decoded = json.loads(existing_raw)
            existing = decoded if isinstance(decoded, dict) else {}
        except (json.JSONDecodeError, TypeError):
            existing = {}

    if merge_lists:
        for k, v in patch.items():
            if isinstance(v, list):
                prior = existing.get(k)
                if isinstance(prior, list):
                    existing[k] = prior + v
                else:
                    existing[k] = list(v)
            else:
                existing[k] = v
    else:
        existing.update(patch)

    await conn.execute(
        "UPDATE decision_deltas SET impact = $2::jsonb "
        "WHERE id = $1 AND tenant_id = $3",
        delta_id, json.dumps(existing, default=str), tenant_id,
    )


__all__ = ["build_router"]
