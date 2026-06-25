"""HTTP API for Resolution Threads.

Mounted at /v1/resolution_threads. Today and Model may project threads
inline, but this API is the operational backend surface for creating,
updating, observing, and evaluating the trackers.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from lib.shared.errors import CompanyOSError, ValidationError
from services.product.resolution_threads import evaluator, repo
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read_by_id,
)
from services.platform.access_control.roles import has_role


if TYPE_CHECKING:
    from services.app.gateway.auth import AuthContext


_TARGET_ACCESS_KIND: dict[str, EntityKind] = {
    "customer": "resource",
    "resource": "resource",
    "commitment": "commitment",
    "goal": "goal",
    "decision": "decision",
    "model": "model",
}


def build_router() -> APIRouter:
    router = APIRouter(prefix="/v1/resolution_threads", tags=["resolution_threads"])

    router.add_api_route("/", list_route, methods=["GET"])
    router.add_api_route("/", create_route, methods=["POST"])
    router.add_api_route("/{thread_id}", get_route, methods=["GET"])
    router.add_api_route("/{thread_id}/status", update_status_route, methods=["PATCH"])
    router.add_api_route("/{thread_id}/steps/{step_id}", update_step_route, methods=["PATCH"])
    router.add_api_route("/{thread_id}/signals/{signal_id}", update_signal_route, methods=["PATCH"])
    router.add_api_route(
        "/{thread_id}/signals/{signal_id}/observe",
        observe_signal_route,
        methods=["POST"],
    )
    router.add_api_route("/{thread_id}/evaluate", evaluate_route, methods=["POST"])
    return router


def _auth(request: Request):
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise HTTPException(status_code=401, detail="unauthorized")
    return auth


def _pool(request: Request) -> asyncpg.Pool:
    deps = getattr(request.app.state, "deps", None)
    if deps is None or getattr(deps, "pool", None) is None:
        raise HTTPException(status_code=503, detail="service_unavailable")
    return deps.pool


def _uuid(raw: str, field: str) -> UUID:
    try:
        return UUID(raw)
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"invalid_{field}") from e


def _validation_error(e: ValidationError) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})


async def list_route(request: Request) -> dict[str, Any]:
    auth = _auth(request)
    qp = request.query_params
    limit = _limit(qp.get("limit", "50"))
    target_id = _uuid(qp["target_node_id"], "target_node_id") if qp.get("target_node_id") else None
    source_delta_id = _uuid(
        qp["source_decision_delta_id"],
        "source_decision_delta_id",
    ) if qp.get("source_decision_delta_id") else None
    try:
        async with _pool(request).acquire() as conn:
            items = await repo.list_threads(
                conn,
                tenant_id=auth.tenant_id,
                status=qp.get("status"),
                target_node_kind=qp.get("target_node_kind"),
                target_node_id=target_id,
                source_decision_delta_id=source_delta_id,
                limit=limit,
            )
            visible: list[repo.ResolutionThread] = []
            for thread in items:
                decision = await _thread_access_decision(conn, auth, thread)
                if decision.allowed:
                    visible.append(thread)
    except ValidationError as e:
        raise _validation_error(e)
    return {
        "items": [repo.thread_to_wire(t) for t in visible],
        "count": len(visible),
    }


async def create_route(request: Request) -> dict[str, Any]:
    auth = _auth(request)
    body = await _read_json(request)
    source_delta_id = _maybe_uuid(
        body.get("sourceDecisionDeltaId") or body.get("source_decision_delta_id"),
        "sourceDecisionDeltaId",
    )
    target_id = _maybe_uuid(body.get("targetNodeId") or body.get("target_node_id"), "targetNodeId")
    target_kind = body.get("targetNodeKind") or body.get("target_node_kind")
    payload = body.get("thread") if isinstance(body.get("thread"), dict) else body
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                await _ensure_source_delta_visible(conn, auth, source_delta_id)
                await _ensure_target_visible(
                    conn,
                    auth,
                    str(target_kind) if target_kind else None,
                    target_id,
                )
                thread = await repo.create_thread(
                    conn,
                    tenant_id=auth.tenant_id,
                    payload=payload,
                    source_decision_delta_id=source_delta_id,
                    target_node_kind=str(target_kind) if target_kind else None,
                    target_node_id=target_id,
                    created_by=auth.actor_id,
                )
    except ValidationError as e:
        raise _validation_error(e)
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def get_route(thread_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    async with _pool(request).acquire() as conn:
        thread = await repo.get_thread(
            conn,
            tenant_id=auth.tenant_id,
            thread_id=tid,
            include_events=True,
        )
        await _ensure_thread_visible(conn, auth, thread)
    if thread is None:
        raise HTTPException(status_code=404, detail="not_found")
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def update_status_route(thread_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    body = await _read_json(request)
    status = _required_status(body)
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await repo.get_thread(
                    conn, tenant_id=auth.tenant_id, thread_id=tid,
                )
                await _ensure_thread_visible(conn, auth, current)
                thread = await repo.update_thread_status(
                    conn,
                    tenant_id=auth.tenant_id,
                    thread_id=tid,
                    status=status,
                    actor_id=auth.actor_id,
                    reason=body.get("reason") if isinstance(body.get("reason"), str) else None,
                )
    except repo.ResolutionThreadNotFoundError:
        raise HTTPException(status_code=404, detail="not_found")
    except ValidationError as e:
        raise _validation_error(e)
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def update_step_route(thread_id: str, step_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    sid = _uuid(step_id, "step_id")
    body = await _read_json(request)
    status = _required_status(body)
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await repo.get_thread(
                    conn, tenant_id=auth.tenant_id, thread_id=tid,
                )
                await _ensure_thread_visible(conn, auth, current)
                thread = await repo.update_step_status(
                    conn,
                    tenant_id=auth.tenant_id,
                    thread_id=tid,
                    step_id=sid,
                    status=status,
                    actor_id=auth.actor_id,
                    proof=body.get("proof") if isinstance(body.get("proof"), str) else None,
                    blocked_by=body.get("blockedBy") if isinstance(body.get("blockedBy"), str) else None,
                )
    except repo.ResolutionThreadNotFoundError:
        raise HTTPException(status_code=404, detail="not_found")
    except ValidationError as e:
        raise _validation_error(e)
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def update_signal_route(thread_id: str, signal_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    sid = _uuid(signal_id, "signal_id")
    body = await _read_json(request)
    status = _required_status(body)
    evidence = body.get("matchedEvidence") or body.get("matched_evidence")
    if evidence is not None and not isinstance(evidence, dict):
        raise HTTPException(status_code=400, detail="invalid_matched_evidence")
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await repo.get_thread(
                    conn, tenant_id=auth.tenant_id, thread_id=tid,
                )
                await _ensure_thread_visible(conn, auth, current)
                thread = await repo.update_signal_status(
                    conn,
                    tenant_id=auth.tenant_id,
                    thread_id=tid,
                    signal_id=sid,
                    status=status,
                    actor_id=auth.actor_id,
                    matched_evidence=evidence,
                )
    except repo.ResolutionThreadNotFoundError:
        raise HTTPException(status_code=404, detail="not_found")
    except ValidationError as e:
        raise _validation_error(e)
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def observe_signal_route(thread_id: str, signal_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    sid = _uuid(signal_id, "signal_id")
    body = await _read_json(request)
    evidence = body.get("evidence")
    if evidence is not None and not isinstance(evidence, dict):
        raise HTTPException(status_code=400, detail="invalid_evidence")
    status = body.get("status") if isinstance(body.get("status"), str) else "seen"
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await repo.get_thread(
                    conn, tenant_id=auth.tenant_id, thread_id=tid,
                )
                await _ensure_thread_visible(conn, auth, current)
                thread = await evaluator.observe_signal(
                    conn,
                    tenant_id=auth.tenant_id,
                    thread_id=tid,
                    signal_id=sid,
                    status=status,
                    evidence=evidence,
                    actor_id=auth.actor_id,
                )
    except repo.ResolutionThreadNotFoundError:
        raise HTTPException(status_code=404, detail="not_found")
    except ValidationError as e:
        raise _validation_error(e)
    return {"thread": repo.thread_to_wire(thread, include_events=True)}


async def evaluate_route(thread_id: str, request: Request) -> dict[str, Any]:
    auth = _auth(request)
    tid = _uuid(thread_id, "thread_id")
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                current = await repo.get_thread(
                    conn, tenant_id=auth.tenant_id, thread_id=tid,
                )
                await _ensure_thread_visible(conn, auth, current)
                thread, result = await evaluator.evaluate_thread(
                    conn,
                    tenant_id=auth.tenant_id,
                    thread_id=tid,
                    actor_id=auth.actor_id,
                )
    except repo.ResolutionThreadNotFoundError:
        raise HTTPException(status_code=404, detail="not_found")
    except CompanyOSError as e:
        raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
    return {
        "thread": repo.thread_to_wire(thread, include_events=True),
        "evaluation": {
            "threadId": str(result.thread_id),
            "signalsSeen": result.signals_seen,
            "signalsChecked": result.signals_checked,
            "matched": result.matched,
        },
    }


def _required_status(body: dict[str, Any]) -> str:
    status = body.get("status")
    if not isinstance(status, str):
        raise HTTPException(status_code=400, detail="status_required")
    return status


def _limit(raw: str) -> int:
    try:
        return max(1, min(200, int(raw)))
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail="invalid_limit") from e


def _maybe_uuid(raw: Any, field: str) -> UUID | None:
    if raw is None or raw == "":
        return None
    try:
        return UUID(str(raw))
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"invalid_{field}") from e


async def _ensure_thread_visible(
    conn: asyncpg.Connection,
    auth: "AuthContext",
    thread: repo.ResolutionThread | None,
) -> None:
    if thread is None:
        return
    decision = await _thread_access_decision(conn, auth, thread)
    if not decision.allowed:
        raise _forbidden(decision.reason)


async def _thread_access_decision(
    conn: asyncpg.Connection,
    auth: "AuthContext",
    thread: repo.ResolutionThread,
) -> AccessDecision:
    target_decision = await _target_access_decision(
        conn,
        auth,
        thread.target_node_kind,
        thread.target_node_id,
    )
    if target_decision is not None:
        return target_decision
    if thread.created_by == auth.actor_id:
        return AccessDecision(True, "resolution_thread_creator")
    if await has_role(auth.actor_id, "admin", conn=conn, tenant_id=auth.tenant_id):
        decision = AccessDecision(True, "admin_override", override_applied=True)
        await record_override_if_needed(
            decision,
            actor_id=auth.actor_id,
            entity_type="resolution_thread",
            entity_id=thread.id,
            conn=conn,
            tenant_id=auth.tenant_id,
        )
        return decision
    if await has_role(
        auth.actor_id,
        "leadership",
        conn=conn,
        tenant_id=auth.tenant_id,
    ):
        decision = AccessDecision(
            True, "leadership_override", override_applied=True,
        )
        await record_override_if_needed(
            decision,
            actor_id=auth.actor_id,
            entity_type="resolution_thread",
            entity_id=thread.id,
            conn=conn,
            tenant_id=auth.tenant_id,
        )
        return decision
    return AccessDecision(False, "resolution_thread_out_of_scope")


async def _ensure_target_visible(
    conn: asyncpg.Connection,
    auth: "AuthContext",
    target_node_kind: str | None,
    target_node_id: UUID | None,
) -> None:
    decision = await _target_access_decision(
        conn, auth, target_node_kind, target_node_id,
    )
    if decision is not None and not decision.allowed:
        raise _forbidden(decision.reason)


async def _target_access_decision(
    conn: asyncpg.Connection,
    auth: "AuthContext",
    target_node_kind: str | None,
    target_node_id: UUID | None,
) -> AccessDecision | None:
    if target_node_kind is None and target_node_id is None:
        return None
    if target_node_kind is None or target_node_id is None:
        return AccessDecision(False, "resolution_thread_target_incomplete")
    access_kind = _TARGET_ACCESS_KIND.get(str(target_node_kind))
    if access_kind is None:
        return AccessDecision(False, "resolution_thread_target_kind_unsupported")
    decision = await can_read_by_id(
        auth.actor_id,
        access_kind,
        target_node_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type=access_kind,
        entity_id=target_node_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    return None if decision.allowed else decision


async def _ensure_source_delta_visible(
    conn: asyncpg.Connection,
    auth: "AuthContext",
    source_decision_delta_id: UUID | None,
) -> None:
    if source_decision_delta_id is None:
        return
    row = await conn.fetchrow(
        """
        SELECT target_node_kind, target_node_id
        FROM decision_deltas
        WHERE id = $1 AND tenant_id = $2
        """,
        source_decision_delta_id,
        auth.tenant_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="source_decision_delta_not_found")
    await _ensure_target_visible(
        conn,
        auth,
        row["target_node_kind"],
        row["target_node_id"],
    )


def _forbidden(reason: str) -> HTTPException:
    return HTTPException(
        status_code=403,
        detail={"error": "forbidden", "reason": reason},
    )


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
