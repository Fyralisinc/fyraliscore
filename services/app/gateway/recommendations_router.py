"""Gateway routes for the recommendation action surface."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.app.gateway.auth import AuthContext


def build_recommendations_router() -> APIRouter:
    router = APIRouter(tags=["recommendations"])

    @router.get("/v1/recommendations")
    async def list_recommendations(request: Request) -> JSONResponse:
        from services.product.recommendations.repo import list_for_actor

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")

        actor_param = request.query_params.get("actor_id")
        if actor_param is None:
            target_actor = auth.actor_id
        else:
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

        limit_raw = request.query_params.get("limit", "15")
        try:
            limit = max(1, min(100, int(limit_raw)))
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_limit"}, status_code=400)

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            views = await list_for_actor(
                tenant_id=auth.tenant_id,
                target_actor_id=target_actor,
                limit=limit,
                conn=conn,
            )

        return JSONResponse(
            {
                "items": [_serialize_recommendation(v) for v in views],
                "count": len(views),
            },
            status_code=200,
        )

    @router.post("/v1/recommendations/{recommendation_id}/act")
    async def act_on_recommendation_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.handlers import (
            AlreadyArchivedError,
            act_on_recommendation,
        )

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )

        try:
            body = await _json_body_or_empty(request)
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        notes_raw = body.get("notes") if isinstance(body, dict) else None
        notes = (
            str(notes_raw).strip()
            if isinstance(notes_raw, str) and notes_raw.strip()
            else None
        )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND claim_role = 'recommendation'",
                        rec_id,
                        auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden", "reason": "not_target_actor"},
                            status_code=403,
                        )

                    result = await act_on_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        notes=notes,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
            )
        except ValidationError as e:
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
            )

        return JSONResponse(
            {
                "recommendation_id": str(result.recommendation_id),
                "target_act_change_kind": result.target_act_change_kind,
                "target_act_change_id": str(result.target_act_change_id),
            },
            status_code=200,
        )

    @router.post("/v1/recommendations/{recommendation_id}/dismiss")
    async def dismiss_recommendation_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.handlers import (
            AlreadyArchivedError,
            dismiss_recommendation,
        )

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )

        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        reason = (body or {}).get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return JSONResponse({"error": "reason_required"}, status_code=400)

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND claim_role = 'recommendation'",
                        rec_id,
                        auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden", "reason": "not_target_actor"},
                            status_code=403,
                        )

                    await dismiss_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        reason=reason,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
            )
        except ValidationError as e:
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
            )

        return JSONResponse(
            {
                "recommendation_id": str(rec_id),
                "archived_with_reason": reason.strip(),
            },
            status_code=200,
        )

    @router.post("/v1/recommendations/{recommendation_id}/ratify")
    async def ratify_hypothesis_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.handlers import (
            AlreadyArchivedError,
            ratify_hypothesis,
        )

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            model_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse({"error": "invalid_model_id"}, status_code=400)

        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "invalid_body"}, status_code=400)
        action = body.get("action")
        if action not in ("approve", "correct", "other", "dismiss"):
            return JSONResponse(
                {
                    "error": "invalid_action",
                    "valid": ["approve", "correct", "other", "dismiss"],
                },
                status_code=400,
            )
        explanation = body.get("explanation")
        if explanation is not None and not isinstance(explanation, str):
            return JSONResponse(
                {"error": "invalid_explanation"}, status_code=400
            )
        correction = body.get("correction")
        if correction is not None and not isinstance(correction, dict):
            return JSONResponse(
                {"error": "invalid_correction"}, status_code=400
            )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND claim_role = 'hypothesis'",
                        model_id,
                        auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404
                        )
                    if (
                        target_row["target_actor_id"] is not None
                        and target_row["target_actor_id"] != auth.actor_id
                    ):
                        return JSONResponse(
                            {"error": "forbidden", "reason": "not_target_actor"},
                            status_code=403,
                        )

                    result = await ratify_hypothesis(
                        model_id=model_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        action=action,
                        explanation=explanation,
                        correction=correction,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
            )
        except ValidationError as e:
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()},
                status_code=400,
            )

        return JSONResponse(
            {
                "model_id": str(result.model_id),
                "action": result.action,
                "archived": result.archived,
                "trigger_id": (
                    str(result.trigger_id) if result.trigger_id else None
                ),
                "captured_observation_id": (
                    str(result.captured_observation_id)
                    if result.captured_observation_id
                    else None
                ),
            },
            status_code=200,
        )

    @router.post("/v1/recommendations/{recommendation_id}/watch")
    async def watch_recommendation_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.watchers import create_watch

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )
        try:
            body = await _json_body_or_empty(request)
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        predicate_raw = body.get("predicate")
        if not isinstance(predicate_raw, str) or not predicate_raw.strip():
            return JSONResponse(
                {"error": "predicate_required"}, status_code=400
            )
        predicate = predicate_raw.strip()

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            watch_id = await create_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                predicate=predicate,
                conn=conn,
            )
        return JSONResponse(
            {
                "ok": True,
                "watch_id": str(watch_id),
                "recommendation_id": str(rec_id),
            },
            status_code=200,
        )

    @router.delete("/v1/recommendations/{recommendation_id}/watch")
    async def unwatch_recommendation_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.watchers import clear_watch

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )

        deps = _deps(request)
        async with deps.pool.acquire() as conn:
            await clear_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                conn=conn,
            )
        return JSONResponse({"ok": True}, status_code=200)

    @router.post("/v1/recommendations/{recommendation_id}/triage")
    async def triage_recommendation_endpoint(
        recommendation_id: str,
        request: Request,
    ) -> JSONResponse:
        from services.product.recommendations.handlers import (
            AlreadyArchivedError,
        )
        from services.product.today import TriageError, triage_recommendation

        auth = _auth(request)
        if auth is None:
            return _unauth("missing_bearer")
        try:
            rec_id = UUID(recommendation_id)
        except (ValueError, TypeError):
            return JSONResponse(
                {"error": "invalid_recommendation_id"}, status_code=400
            )
        try:
            body = await _json_body_or_empty(request)
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        action_raw = body.get("action")
        if action_raw not in ("hold", "route", "snooze", "dismiss"):
            return JSONResponse(
                {
                    "error": "invalid_action",
                    "reason": (
                        "use /act for act; one of "
                        "hold/route/snooze/dismiss here"
                    ),
                },
                status_code=400,
            )

        reason = body.get("reason") if isinstance(body.get("reason"), str) else None
        routed_to = (
            body.get("routed_to")
            if isinstance(body.get("routed_to"), str)
            else None
        )
        snooze_until_raw = body.get("snooze_until")
        snooze_until = None
        if isinstance(snooze_until_raw, str) and snooze_until_raw.strip():
            try:
                snooze_until = datetime.fromisoformat(snooze_until_raw)
            except ValueError:
                return JSONResponse(
                    {"error": "invalid_snooze_until"}, status_code=400
                )

        deps = _deps(request)
        try:
            async with deps.pool.acquire() as conn:
                async with conn.transaction():
                    target_row = await conn.fetchrow(
                        "SELECT target_actor_id FROM models "
                        "WHERE id = $1 AND tenant_id = $2 "
                        "  AND claim_role = 'recommendation'",
                        rec_id,
                        auth.tenant_id,
                    )
                    if target_row is None:
                        return JSONResponse(
                            {"error": "not_found"}, status_code=404
                        )
                    if target_row["target_actor_id"] != auth.actor_id:
                        return JSONResponse(
                            {"error": "forbidden", "reason": "not_target_actor"},
                            status_code=403,
                        )
                    result = await triage_recommendation(
                        recommendation_id=rec_id,
                        actor_id=auth.actor_id,
                        tenant_id=auth.tenant_id,
                        action=action_raw,
                        reason=reason,
                        routed_to=routed_to,
                        snooze_until=snooze_until,
                        conn=conn,
                    )
        except AlreadyArchivedError as e:
            return JSONResponse(
                {"error": "already_archived", "detail": e.to_dict()},
                status_code=409,
            )
        except TriageError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()}, status_code=400
            )
        except ValidationError as e:
            return JSONResponse(
                {"error": "validation_error", "detail": e.to_dict()},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"error": e.code, "detail": e.to_dict()}, status_code=400
            )

        return JSONResponse(
            {
                "ok": True,
                "recommendation_id": str(result.recommendation_id),
                "action": result.action,
            },
            status_code=200,
        )

    return router


def _auth(request: Request) -> AuthContext | None:
    return getattr(request.state, "auth", None)


def _deps(request: Request):  # type: ignore[no-untyped-def]
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


async def _json_body_or_empty(request: Request) -> Any:
    return await request.json() if (await request.body()) else {}


def _serialize_recommendation(view: Any) -> dict[str, Any]:
    target = view.target_entity
    return {
        "id": str(view.id),
        "proposition_text": view.proposition_text,
        "confidence": view.confidence,
        "target_act_ref": view.target_act_ref,
        "proposed_change": view.proposed_change,
        "expected_impact": view.expected_impact,
        "qualitative_impact": view.qualitative_impact,
        "target_actor_id": str(view.target_actor_id),
        "supporting_event_ids": [str(x) for x in view.supporting_event_ids],
        "supporting_model_ids": [str(x) for x in view.supporting_model_ids],
        "created_at": view.created_at.isoformat(),
        "scope_entities": view.scope_entities,
        "rank_score": view.rank_score,
        "feedback_adjustment": getattr(view, "feedback_adjustment", 1.0),
        "feedback_pattern_key": getattr(view, "feedback_pattern_key", None),
        "consequence_preview": getattr(view, "consequence_preview", None),
        "claim_role": getattr(view, "claim_role", None),
        "is_system_hypothesis": bool(
            getattr(view, "is_system_hypothesis", False)
        ),
        "hypothesis_text": getattr(view, "hypothesis_text", None),
        "target_entity": (
            {
                "type": target.type,
                "id": str(target.id),
                "title": target.title,
                "state": target.state,
                "archived": target.archived,
            }
            if target is not None
            else None
        ),
    }
