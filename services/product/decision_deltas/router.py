"""
services/product/decision_deltas/router.py — HTTP surface for Decision Deltas.

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

The router is NOT registered in services/app/gateway/main.py here (that
file is in this agent's forbidden zone). The registration line for the
gateway owner is documented in the agent report.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from lib.shared.errors import CompanyOSError, ValidationError
from services.product.decision_deltas import apply as apply_mod
from services.product.decision_deltas import promote as promote_mod
from services.product.decision_deltas import repo as dd_repo
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read_by_id,
)
from services.platform.product_action_audit import record_product_action


if TYPE_CHECKING:
    from services.app.gateway.auth import AuthContext


log = logging.getLogger(__name__)

_TARGET_ACCESS_KIND: dict[str, EntityKind] = {
    "customer": "resource",
    "resource": "resource",
    "commitment": "commitment",
    "goal": "goal",
    "decision": "decision",
    "model": "model",
}


def build_router() -> APIRouter:
    router = APIRouter(
        prefix="/v1/decision_deltas",
        tags=["decision_deltas"],
    )

    router.add_api_route("/", list_route, methods=["GET"])
    router.add_api_route("/{delta_id}", get_one, methods=["GET"])
    router.add_api_route("/{delta_id}/accept", accept_route, methods=["POST"])
    router.add_api_route("/{delta_id}/delegate", delegate_route, methods=["POST"])
    router.add_api_route("/{delta_id}/contest", contest_route, methods=["POST"])
    router.add_api_route("/{delta_id}/add_context", add_context_route, methods=["POST"])
    router.add_api_route(
        "/from_recommendation/{recommendation_id}",
        promote_route,
        methods=["POST"],
    )
    return router


def _auth(request: Request) -> AuthContext:
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise HTTPException(status_code=401, detail="unauthorized")
    return auth


def _pool(request: Request) -> asyncpg.Pool:
    deps = getattr(request.app.state, "deps", None)
    if deps is None or getattr(deps, "pool", None) is None:
        raise HTTPException(status_code=503, detail="service_unavailable")
    return deps.pool


def _parse_uuid(raw: str, field: str = "id") -> UUID:
    try:
        return UUID(raw)
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"invalid_{field}") from e


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

    try:
        async with _pool(request).acquire() as conn:
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
    visible: list[dd_repo.DecisionDeltaView] = []
    async with _pool(request).acquire() as conn:
        for view in views:
            decision = await _delta_target_decision(conn, auth, view)
            if decision is None or decision.allowed:
                visible.append(view)
    return {"items": [_view_to_wire(v) for v in visible], "count": len(visible)}


async def get_one(delta_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    did = _parse_uuid(delta_id, "delta_id")
    async with _pool(request).acquire() as conn:
        view = await dd_repo.get_delta(conn, tenant_id=auth.tenant_id, delta_id=did)
        await _ensure_can_read_delta(conn, auth, view)
    if view is None:
        raise HTTPException(status_code=404, detail="not_found")
    return _view_to_wire(view, with_evidence=True)


async def accept_route(delta_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    did = _parse_uuid(delta_id, "delta_id")
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=did,
                )
                await _ensure_can_read_delta(conn, auth, current)
                view, triggered = await apply_mod.apply_acceptance(
                    conn=conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    user_id=auth.actor_id,
                )
                await _record_delta_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="decision_delta.accept",
                    resource_id=did,
                    before=current,
                    after=view,
                    metadata={
                        "target_updated": bool(triggered.get("target_updated")),
                        "target_event_id": triggered.get("target_event_id"),
                        "notifications_dispatched": triggered.get(
                            "notifications_dispatched"
                        ),
                        "resolution_thread_id": triggered.get(
                            "resolution_thread_id"
                        ),
                        "resolution_thread_created": triggered.get(
                            "resolution_thread_created"
                        ),
                    },
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
    return {"delta": _view_to_wire(view, with_evidence=True), "triggered": triggered}


async def delegate_route(delta_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    did = _parse_uuid(delta_id, "delta_id")
    body = await _read_json(request)
    owner_raw = body.get("owner_id")
    if not isinstance(owner_raw, str) or not owner_raw.strip():
        raise HTTPException(status_code=400, detail="owner_id_required")
    owner_id = _parse_uuid(owner_raw, "owner_id")
    note = body.get("note")

    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=did,
                )
                await _ensure_can_read_delta(conn, auth, current)
                await dd_repo.update_status(
                    conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    status="delegated",
                    user_id=auth.actor_id,
                )
                await _annotate_delegation(
                    conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    owner_id=owner_id,
                    note=note,
                )
                view = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=did,
                )
                await _record_delta_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="decision_delta.delegate",
                    resource_id=did,
                    before=current,
                    after=view,
                    metadata={
                        "delegate_to_actor_id": str(owner_id),
                        "note_chars": _text_len(note),
                    },
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


async def contest_route(delta_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    did = _parse_uuid(delta_id, "delta_id")
    body = await _read_json(request)
    reason = body.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise HTTPException(status_code=400, detail="reason_required")

    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=did,
                )
                await _ensure_can_read_delta(conn, auth, current)
                await dd_repo.update_status(
                    conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    status="contested",
                    user_id=auth.actor_id,
                )
                await _annotate_contest(
                    conn,
                    tenant_id=auth.tenant_id,
                    delta_id=did,
                    actor_id=auth.actor_id,
                    reason=reason.strip(),
                )
                view = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=did,
                )
                await _record_delta_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="decision_delta.contest",
                    resource_id=did,
                    before=current,
                    after=view,
                    metadata={"reason_chars": _text_len(reason)},
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


async def add_context_route(delta_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    did = _parse_uuid(delta_id, "delta_id")
    body = await _read_json(request)
    note = body.get("note")
    if not isinstance(note, str) or not note.strip():
        raise HTTPException(status_code=400, detail="note_required")

    async with _pool(request).acquire() as conn:
        current = await dd_repo.get_delta(conn, tenant_id=auth.tenant_id, delta_id=did)
        await _ensure_can_read_delta(conn, auth, current)
        if current is None:
            raise HTTPException(status_code=404, detail="not_found")
        async with conn.transaction():
            await _annotate_context_note(
                conn,
                tenant_id=auth.tenant_id,
                delta_id=did,
                actor_id=auth.actor_id,
                note=note.strip(),
            )
            await _record_delta_action(
                conn,
                request=request,
                auth=auth,
                action="decision_delta.add_context",
                resource_id=did,
                before=current,
                after=current,
                metadata={"note_chars": _text_len(note)},
            )
        view = await dd_repo.get_delta(conn, tenant_id=auth.tenant_id, delta_id=did)
    assert view is not None
    return {"delta": _view_to_wire(view, with_evidence=True)}


async def promote_route(recommendation_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    rid = _parse_uuid(recommendation_id, "recommendation_id")
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                recommendation_decision = await can_read_by_id(
                    auth.actor_id,
                    "model",
                    rid,
                    conn=conn,
                    tenant_id=auth.tenant_id,
                )
                await record_override_if_needed(
                    recommendation_decision,
                    actor_id=auth.actor_id,
                    entity_type="model",
                    entity_id=rid,
                    conn=conn,
                    tenant_id=auth.tenant_id,
                )
                if not recommendation_decision.allowed:
                    raise _forbidden(recommendation_decision.reason)
                delta_id = await promote_mod.promote_from_recommendation(
                    conn,
                    tenant_id=auth.tenant_id,
                    recommendation_id=rid,
                )
                view = await dd_repo.get_delta(
                    conn, tenant_id=auth.tenant_id, delta_id=delta_id,
                )
                await _ensure_can_read_delta(conn, auth, view)
                await _record_delta_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="decision_delta.promote_from_recommendation",
                    resource_id=delta_id,
                    before=None,
                    after=view,
                    metadata={"source_recommendation_id": str(rid)},
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


async def _ensure_can_read_delta(
    conn: asyncpg.Connection,
    auth: AuthContext,
    view: dd_repo.DecisionDeltaView | None,
) -> None:
    if view is None:
        return
    decision = await _delta_target_decision(conn, auth, view)
    if decision is not None and not decision.allowed:
        raise _forbidden(decision.reason)


async def _delta_target_decision(
    conn: asyncpg.Connection,
    auth: AuthContext,
    view: dd_repo.DecisionDeltaView,
) -> AccessDecision | None:
    if view.target_node_kind is None and view.target_node_id is None:
        return None
    if view.target_node_kind is None or view.target_node_id is None:
        return AccessDecision(False, "delta_target_incomplete")
    access_kind = _TARGET_ACCESS_KIND.get(str(view.target_node_kind))
    if access_kind is None:
        return AccessDecision(False, "delta_target_kind_unsupported")
    decision = await can_read_by_id(
        auth.actor_id,
        access_kind,
        view.target_node_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type=access_kind,
        entity_id=view.target_node_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    return None if decision.allowed else decision


def _forbidden(reason: str) -> HTTPException:
    return HTTPException(
        status_code=403,
        detail={"error": "forbidden", "reason": reason},
    )


async def _record_delta_action(
    conn: asyncpg.Connection,
    *,
    request: Request,
    auth: AuthContext,
    action: str,
    resource_id: UUID,
    before: dd_repo.DecisionDeltaView | None,
    after: dd_repo.DecisionDeltaView | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    await record_product_action(
        conn,
        tenant_id=auth.tenant_id,
        actor_id=auth.actor_id,
        action=action,
        resource_type="decision_delta",
        resource_id=resource_id,
        metadata=_delta_action_metadata(
            request=request,
            before=before,
            after=after,
            extra=metadata,
        ),
    )


def _delta_action_metadata(
    *,
    request: Request,
    before: dd_repo.DecisionDeltaView | None,
    after: dd_repo.DecisionDeltaView | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = after or before
    out: dict[str, Any] = {}
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        out["request_id"] = str(request_id)
    if before is not None:
        out["status_before"] = before.status
    if after is not None:
        out["status_after"] = after.status
    if source is not None:
        if source.target_node_kind:
            out["target_node_kind"] = source.target_node_kind
        if source.target_node_id:
            out["target_node_id"] = str(source.target_node_id)
        if source.category:
            out["category"] = source.category
        if source.source_recommendation_id:
            out["source_recommendation_id"] = str(source.source_recommendation_id)
    for key, value in (extra or {}).items():
        if value is not None:
            out[key] = value
    return out


def _text_len(value: Any) -> int:
    return len(value.strip()) if isinstance(value, str) else 0


async def _annotate_delegation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    owner_id: UUID,
    note: Any,
) -> None:
    await _annotate(
        conn,
        tenant_id=tenant_id,
        delta_id=delta_id,
        patch={
            "delegation": {
                "owner_id": str(owner_id),
                "note": str(note).strip() if isinstance(note, str) else None,
                "at": _now_iso(),
            },
        },
    )


async def _annotate_contest(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    actor_id: UUID,
    reason: str,
) -> None:
    await _annotate(
        conn,
        tenant_id=tenant_id,
        delta_id=delta_id,
        patch={
            "contest": {
                "by": str(actor_id),
                "reason": reason,
                "at": _now_iso(),
            },
        },
    )


async def _annotate_context_note(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    actor_id: UUID,
    note: str,
) -> None:
    await _annotate(
        conn,
        tenant_id=tenant_id,
        delta_id=delta_id,
        patch={
            "context_notes": [
                {
                    "by": str(actor_id),
                    "note": note,
                    "at": _now_iso(),
                }
            ],
        },
        merge_lists=True,
    )


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
