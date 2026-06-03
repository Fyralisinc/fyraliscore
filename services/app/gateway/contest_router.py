"""Gateway route for model contestability."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.app.gateway.auth import AuthContext


def build_contest_router() -> APIRouter:
    router = APIRouter(tags=["contestability"])

    @router.post("/contest/{model_id}")
    async def post_contest(model_id: str, request: Request) -> JSONResponse:
        from services.reasoning.contestability import (
            ContestationInput,
            NoStandingError,
            contest_model,
        )

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)

        try:
            target_model = UUID(model_id)
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_model_id"}, status_code=400)
        kind = body.get("contestation_kind")
        if kind not in ("belief", "reading"):
            return JSONResponse(
                {"error": "invalid_contestation_kind"}, status_code=400
            )
        rationale = body.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            return JSONResponse({"error": "rationale_required"}, status_code=400)
        contestor_raw = body.get("contestor_actor_id")
        if contestor_raw is None:
            contestor_id = auth.actor_id
        else:
            try:
                contestor_id = UUID(str(contestor_raw))
            except (ValueError, TypeError):
                return JSONResponse(
                    {"error": "invalid_contestor_actor_id"},
                    status_code=400,
                )
            if contestor_id != auth.actor_id:
                return JSONResponse(
                    {"error": "cannot_contest_on_behalf_of_others"},
                    status_code=403,
                )

        deps = _deps(request)
        inp = ContestationInput(
            model_id=target_model,
            contestor_actor_id=contestor_id,
            tenant_id=auth.tenant_id,
            contestation_kind=kind,
            rationale=rationale,
            proposed_alternative=body.get("proposed_alternative"),
        )
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    result = await contest_model(conn, inp)
        except NoStandingError as e:
            return JSONResponse(
                {"error": "no_standing", "detail": e.to_dict()},
                status_code=403,
            )
        except ValidationError as e:
            status_code = 404 if "does not exist" in (e.message or "") else 400
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=status_code,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()}, status_code=400
            )
        return JSONResponse(
            {
                "observation_id": str(result.observation_id),
                "trigger_id": str(result.trigger_id) if result.trigger_id else None,
                "previous_confidence": result.previous_confidence,
                "new_confidence": result.new_confidence,
                "standing_basis": result.standing_basis,
                "override_applied": result.override_applied,
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
