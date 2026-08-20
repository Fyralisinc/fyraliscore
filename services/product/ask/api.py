"""FastAPI routes for the Ask Fyralis overlay."""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from .orchestrator import AskOrchestrator
from .schemas import (
    AskFeedbackRequest,
    AskFeedbackResponse,
    AskSessionCreateRequest,
    AskSessionCreateResponse,
    AskTurnRequest,
    AskTurnResponse,
    EvidenceExpansionRequest,
    EvidenceExpansionResponse,
    ProposedStateChangeActionRequest,
    ProposedStateChangeActionResponse,
)


def _bounded_http_error(status_code: int, code: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail=code)


@dataclass(frozen=True)
class AskAuth:
    tenant_id: UUID
    viewer_id: UUID


def _request_is_production(request: Request) -> bool:
    settings = getattr(request.app.state, "gateway_settings", None)
    return bool(getattr(settings, "is_production", False))


def build_router(
    orchestrator: AskOrchestrator,
    *,
    default_tenant_id: UUID | None = None,
    default_viewer_id: UUID | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/v1/ask", tags=["ask"])

    async def auth_dep(
        request: Request,
        x_tenant_id: str | None = Header(default=None),
        x_actor_id: str | None = Header(default=None),
    ) -> AskAuth:
        auth = getattr(request.state, "auth", None)
        if auth is not None:
            return AskAuth(tenant_id=auth.tenant_id, viewer_id=auth.actor_id)
        if _request_is_production(request):
            raise HTTPException(status_code=401, detail="unauthorized")
        if default_tenant_id is not None and default_viewer_id is not None:
            return AskAuth(tenant_id=default_tenant_id, viewer_id=default_viewer_id)
        if x_tenant_id and x_actor_id:
            try:
                return AskAuth(tenant_id=UUID(x_tenant_id), viewer_id=UUID(x_actor_id))
            except ValueError as exc:
                raise _bounded_http_error(400, "invalid_auth_headers") from exc
        raise _bounded_http_error(401, "unauthorized")

    @router.post("/sessions", response_model=AskSessionCreateResponse)
    async def create_session(
        body: AskSessionCreateRequest,
        auth: AskAuth = Depends(auth_dep),
    ) -> AskSessionCreateResponse:
        session = await orchestrator.create_session(
            tenant_id=auth.tenant_id,
            viewer_id=auth.viewer_id,
            body=body,
        )
        return AskSessionCreateResponse(session=session)

    @router.post("/sessions/{session_id}/messages", response_model=AskTurnResponse)
    async def answer_turn(
        session_id: UUID,
        body: AskTurnRequest,
        auth: AskAuth = Depends(auth_dep),
    ) -> AskTurnResponse:
        try:
            return await orchestrator.answer_turn(
                tenant_id=auth.tenant_id,
                viewer_id=auth.viewer_id,
                session_id=session_id,
                body=body,
            )
        except ValueError as exc:
            raise _bounded_http_error(400, "invalid_query") from exc
        except LookupError as exc:
            raise _bounded_http_error(404, "ask_session_not_found") from exc

    @router.post("/evidence/expand", response_model=EvidenceExpansionResponse)
    async def expand_evidence(
        body: EvidenceExpansionRequest,
        auth: AskAuth = Depends(auth_dep),
    ) -> EvidenceExpansionResponse:
        evidence, omitted = await orchestrator.expand_evidence(
            tenant_id=auth.tenant_id,
            viewer_id=auth.viewer_id,
            retrieval_run_id=body.retrieval_run_id,
        )
        return EvidenceExpansionResponse(
            retrieval_run_id=body.retrieval_run_id,
            evidence=evidence,
            omitted=omitted,
        )

    @router.post(
        "/proposed-state-changes/{change_id}/action",
        response_model=ProposedStateChangeActionResponse,
    )
    async def act_on_change(
        change_id: UUID,
        body: ProposedStateChangeActionRequest,
        auth: AskAuth = Depends(auth_dep),
    ) -> ProposedStateChangeActionResponse:
        try:
            change = await orchestrator.act_on_proposed_change(
                tenant_id=auth.tenant_id,
                change_id=change_id,
                action=body.action,
                note=body.note,
                delegate_to=body.delegate_to,
            )
        except LookupError as exc:
            raise _bounded_http_error(404, "proposed_state_change_not_found") from exc
        return ProposedStateChangeActionResponse(change=change)

    @router.post("/feedback", response_model=AskFeedbackResponse)
    async def feedback(
        body: AskFeedbackRequest,
        auth: AskAuth = Depends(auth_dep),
    ) -> AskFeedbackResponse:
        fid = await orchestrator.add_feedback(
            session_id=body.session_id,
            answer_id=body.answer_id,
            viewer_id=auth.viewer_id,
            feedback_type=body.feedback_type,
            payload=body.payload,
        )
        return AskFeedbackResponse(ok=True, feedback_id=fid)

    return router


__all__ = ["build_router"]
