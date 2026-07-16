"""Gateway routes for user-facing clarification requests."""

from __future__ import annotations

import contextlib
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lib.shared.errors import ValidationError
from services.app.gateway.auth import AuthContext
from services.domain.clarifications import (
    answer_clarification_request,
    dismiss_clarification_request,
    list_clarification_requests,
)
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)
from services.domain.substrate_candidates import get_substrate_candidate
from services.domain.substrate_promotion import apply_candidate_resolution_answer


class ClarificationAnswerBody(BaseModel):
    answer: dict[str, Any] = Field(default_factory=dict)


class ClarificationDismissBody(BaseModel):
    reason: str = "dismissed"


def build_clarifications_router() -> APIRouter:
    router = APIRouter(tags=["clarifications"])
    router.add_api_route(
        "/v1/clarifications",
        list_clarifications_endpoint,
        methods=["GET"],
    )
    router.add_api_route(
        "/v1/clarifications/{request_id}/answer",
        answer_clarification_endpoint,
        methods=["POST"],
    )
    router.add_api_route(
        "/v1/clarifications/{request_id}/dismiss",
        dismiss_clarification_endpoint,
        methods=["POST"],
    )
    return router


async def list_clarifications_endpoint(
    request: Request,
    status_filter: Literal[
        "open", "answered", "dismissed", "expired", "superseded", "all"
    ] = Query("open", alias="status"),
    limit: int = 50,
) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth("missing_bearer")
    try:
        bounded_limit = max(1, min(200, int(limit)))
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_limit"}, status_code=400)

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        try:
            rows = await list_clarification_requests(
                conn,
                tenant_id=auth.tenant_id,
                status=status_filter,
                limit=bounded_limit,
            )
        except ValidationError as exc:
            return JSONResponse(
                {"error": "invalid_status", "detail": str(exc)},
                status_code=400,
            )
    return JSONResponse(
        {
            "items": [row.to_dict() for row in rows],
            "count": len(rows),
        },
        status_code=200,
    )


async def answer_clarification_endpoint(
    request_id: str,
    request: Request,
    body: ClarificationAnswerBody,
) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth("missing_bearer")
    try:
        cid = UUID(request_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_clarification_id"}, status_code=400)

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        try:
            async with _optional_transaction(conn):
                row = await answer_clarification_request(
                    conn,
                    tenant_id=auth.tenant_id,
                    request_id=cid,
                    answer=body.answer,
                    answered_by=auth.actor_id,
                )
                if row is not None:
                    await _apply_clarification_answer_side_effects(
                        conn,
                        row=row,
                        answer=body.answer,
                        tenant_id=auth.tenant_id,
                        answered_by=auth.actor_id,
                    )
        except ValidationError as exc:
            return JSONResponse(
                {"error": "invalid_answer", "detail": str(exc)},
                status_code=400,
            )
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse(row.to_dict(), status_code=200)


async def dismiss_clarification_endpoint(
    request_id: str,
    request: Request,
    body: ClarificationDismissBody,
) -> JSONResponse:
    auth = _auth(request)
    if auth is None:
        return _unauth("missing_bearer")
    try:
        cid = UUID(request_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_clarification_id"}, status_code=400)

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        row = await dismiss_clarification_request(
            conn,
            tenant_id=auth.tenant_id,
            request_id=cid,
            reason=body.reason,
            answered_by=auth.actor_id,
        )
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse(row.to_dict(), status_code=200)


def _auth(request: Request) -> AuthContext | None:
    auth = getattr(request.state, "auth", None)
    if auth is None:
        return None
    return auth


def _deps(request: Request) -> Any:
    return request.app.state.deps


def _optional_transaction(conn: Any) -> Any:
    transaction = getattr(conn, "transaction", None)
    if transaction is None:
        return contextlib.AsyncExitStack()
    return transaction()


async def _apply_clarification_answer_side_effects(
    conn: Any,
    *,
    row: Any,
    answer: dict[str, Any],
    tenant_id: UUID,
    answered_by: UUID | None,
) -> None:
    if (
        row.kind == "substrate_candidate_resolution"
        and row.object_kind == "substrate_candidate"
        and row.object_id is not None
    ):
        candidate = await get_substrate_candidate(
            conn,
            tenant_id=tenant_id,
            candidate_id=row.object_id,
        )
        if candidate is not None:
            await apply_candidate_resolution_answer(
                conn,
                candidate=candidate,
                answer=answer,
            )
        return

    if row.kind == "entity_resolution" and row.object_kind in {
        "entity_review",
        "grounding_trace",
    }:
        await adjudicate_entity_resolution_clarification(
            conn,
            clarification=row,
            answer=answer,
            tenant_id=tenant_id,
            answered_by=answered_by,
        )


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


__all__ = ["build_clarifications_router"]
