"""HTTP API for Resolution Threads.

Mounted at /v1/resolution_threads. Today and Model may project threads
inline, but this API is the operational backend surface for creating,
updating, observing, and evaluating the trackers.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from lib.shared.errors import CompanyOSError, ValidationError
from services.resolution_threads import evaluator, repo


def build_router() -> APIRouter:
    router = APIRouter(prefix="/v1/resolution_threads", tags=["resolution_threads"])

    def _auth(request: Request):
        auth = getattr(request.state, "auth", None)
        if auth is None:
            raise HTTPException(status_code=401, detail="unauthorized")
        return auth

    def _pool(request: Request) -> asyncpg.Pool:
        deps = getattr(request.app.state, "deps", None)
        if deps is None or getattr(deps, "pool", None) is None:
            raise HTTPException(status_code=503, detail="pool_unavailable")
        return deps.pool

    def _uuid(raw: str, field: str) -> UUID:
        try:
            return UUID(raw)
        except (TypeError, ValueError) as e:
            raise HTTPException(status_code=400, detail=f"invalid_{field}") from e

    @router.get("/")
    async def list_route(request: Request) -> dict[str, Any]:
        auth = _auth(request)
        qp = request.query_params
        limit = _limit(qp.get("limit", "50"))
        target_id = _uuid(qp["target_node_id"], "target_node_id") if qp.get("target_node_id") else None
        source_delta_id = _uuid(qp["source_decision_delta_id"], "source_decision_delta_id") if qp.get("source_decision_delta_id") else None
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                items = await repo.list_threads(
                    conn,
                    tenant_id=auth.tenant_id,
                    status=qp.get("status"),
                    target_node_kind=qp.get("target_node_kind"),
                    target_node_id=target_id,
                    source_decision_delta_id=source_delta_id,
                    limit=limit,
                )
        except ValidationError as e:
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"items": [repo.thread_to_wire(t) for t in items], "count": len(items)}

    @router.post("/")
    async def create_route(request: Request) -> dict[str, Any]:
        auth = _auth(request)
        body = await _read_json(request)
        source_delta_id = _maybe_uuid(body.get("sourceDecisionDeltaId") or body.get("source_decision_delta_id"), "sourceDecisionDeltaId")
        target_id = _maybe_uuid(body.get("targetNodeId") or body.get("target_node_id"), "targetNodeId")
        target_kind = body.get("targetNodeKind") or body.get("target_node_kind")
        payload = body.get("thread") if isinstance(body.get("thread"), dict) else body
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.get("/{thread_id}")
    async def get_route(thread_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        pool = _pool(request)
        async with pool.acquire() as conn:
            thread = await repo.get_thread(
                conn,
                tenant_id=auth.tenant_id,
                thread_id=tid,
                include_events=True,
            )
        if thread is None:
            raise HTTPException(status_code=404, detail="not_found")
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.patch("/{thread_id}/status")
    async def update_status_route(thread_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        body = await _read_json(request)
        status = body.get("status")
        if not isinstance(status, str):
            raise HTTPException(status_code=400, detail="status_required")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.patch("/{thread_id}/steps/{step_id}")
    async def update_step_route(thread_id: str, step_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        sid = _uuid(step_id, "step_id")
        body = await _read_json(request)
        status = body.get("status")
        if not isinstance(status, str):
            raise HTTPException(status_code=400, detail="status_required")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.patch("/{thread_id}/signals/{signal_id}")
    async def update_signal_route(thread_id: str, signal_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        sid = _uuid(signal_id, "signal_id")
        body = await _read_json(request)
        status = body.get("status")
        if not isinstance(status, str):
            raise HTTPException(status_code=400, detail="status_required")
        evidence = body.get("matchedEvidence") or body.get("matched_evidence")
        if evidence is not None and not isinstance(evidence, dict):
            raise HTTPException(status_code=400, detail="invalid_matched_evidence")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.post("/{thread_id}/signals/{signal_id}/observe")
    async def observe_signal_route(thread_id: str, signal_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        sid = _uuid(signal_id, "signal_id")
        body = await _read_json(request)
        evidence = body.get("evidence")
        if evidence is not None and not isinstance(evidence, dict):
            raise HTTPException(status_code=400, detail="invalid_evidence")
        status = body.get("status") if isinstance(body.get("status"), str) else "seen"
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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
            raise HTTPException(status_code=400, detail={"error": e.code, "context": e.to_dict()})
        return {"thread": repo.thread_to_wire(thread, include_events=True)}

    @router.post("/{thread_id}/evaluate")
    async def evaluate_route(thread_id: str, request: Request) -> dict[str, Any]:
        auth = _auth(request)
        tid = _uuid(thread_id, "thread_id")
        pool = _pool(request)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
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

    return router


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
