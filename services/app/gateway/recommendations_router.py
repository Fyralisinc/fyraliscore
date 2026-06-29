"""Gateway routes for the recommendation action surface."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.app.gateway.auth import AuthContext
from services.app.gateway.product_workflow_metrics import record_product_workflow_event
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read_by_id,
)
from services.platform.product_action_audit import record_product_action


_TARGET_ACCESS_KIND: dict[str, EntityKind] = {
    "customer": "resource",
    "resource": "resource",
    "commitment": "commitment",
    "goal": "goal",
    "decision": "decision",
    "model": "model",
}


def build_recommendations_router() -> APIRouter:
    router = APIRouter(tags=["recommendations"])
    router.add_api_route(
        "/v1/recommendations",
        list_recommendations,
        methods=["GET"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/act",
        act_on_recommendation_endpoint,
        methods=["POST"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/dismiss",
        dismiss_recommendation_endpoint,
        methods=["POST"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/ratify",
        ratify_hypothesis_endpoint,
        methods=["POST"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/watch",
        watch_recommendation_endpoint,
        methods=["POST"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/watch",
        unwatch_recommendation_endpoint,
        methods=["DELETE"],
    )
    router.add_api_route(
        "/v1/recommendations/{recommendation_id}/triage",
        triage_recommendation_endpoint,
        methods=["POST"],
    )
    return router


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
            return JSONResponse({"error": "invalid_actor_id"}, status_code=400)
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
        visible = []
        for view in views:
            if await _can_read_recommendation_view(conn, auth, view):
                visible.append(view)

    return JSONResponse(
        {
            "items": [_serialize_recommendation(v) for v in visible],
            "count": len(visible),
        },
        status_code=200,
    )


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
        return JSONResponse({"error": "invalid_recommendation_id"}, status_code=400)

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
                denied = await _ensure_recommendation_visible(
                    conn, auth, rec_id, claim_role="recommendation",
                )
                if denied is not None:
                    return denied

                result = await act_on_recommendation(
                    recommendation_id=rec_id,
                    actor_id=auth.actor_id,
                    tenant_id=auth.tenant_id,
                    notes=notes,
                    conn=conn,
                )
                await _record_recommendation_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="recommendation.act",
                    resource_id=rec_id,
                    metadata={
                        "notes_chars": _text_len(notes),
                        "target_act_change_kind": result.target_act_change_kind,
                        "target_act_change_id": str(result.target_act_change_id),
                    },
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

    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_action",
        outcome="success",
    )
    return JSONResponse(
        {
            "recommendation_id": str(result.recommendation_id),
            "target_act_change_kind": result.target_act_change_kind,
            "target_act_change_id": str(result.target_act_change_id),
        },
        status_code=200,
    )


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
        return JSONResponse({"error": "invalid_recommendation_id"}, status_code=400)

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
                denied = await _ensure_recommendation_visible(
                    conn, auth, rec_id, claim_role="recommendation",
                )
                if denied is not None:
                    return denied

                await dismiss_recommendation(
                    recommendation_id=rec_id,
                    actor_id=auth.actor_id,
                    tenant_id=auth.tenant_id,
                    reason=reason,
                    conn=conn,
                )
                await _record_recommendation_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="recommendation.dismiss",
                    resource_id=rec_id,
                    metadata={"reason_chars": _text_len(reason)},
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

    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_dismissal",
        outcome="success",
    )
    return JSONResponse(
        {
            "recommendation_id": str(rec_id),
            "archived_with_reason": reason.strip(),
        },
        status_code=200,
    )


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
        return JSONResponse({"error": "invalid_explanation"}, status_code=400)
    correction = body.get("correction")
    if correction is not None and not isinstance(correction, dict):
        return JSONResponse({"error": "invalid_correction"}, status_code=400)

    deps = _deps(request)
    try:
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                denied = await _ensure_recommendation_visible(
                    conn, auth, model_id, claim_role="hypothesis",
                )
                if denied is not None:
                    return denied

                result = await ratify_hypothesis(
                    model_id=model_id,
                    actor_id=auth.actor_id,
                    tenant_id=auth.tenant_id,
                    action=action,
                    explanation=explanation,
                    correction=correction,
                    conn=conn,
                )
                await _record_recommendation_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="recommendation.ratify",
                    resource_id=model_id,
                    metadata={
                        "ratify_action": result.action,
                        "archived": result.archived,
                        "trigger_id": result.trigger_id,
                        "captured_observation_id": result.captured_observation_id,
                        "explanation_chars": _text_len(explanation),
                        "has_correction": correction is not None,
                    },
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

    record_product_workflow_event(
        workflow="recommendations",
        event="hypothesis_ratification",
        outcome="success",
    )
    return JSONResponse(
        {
            "model_id": str(result.model_id),
            "action": result.action,
            "archived": result.archived,
            "trigger_id": (str(result.trigger_id) if result.trigger_id else None),
            "captured_observation_id": (
                str(result.captured_observation_id)
                if result.captured_observation_id
                else None
            ),
        },
        status_code=200,
    )


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
        return JSONResponse({"error": "invalid_recommendation_id"}, status_code=400)
    try:
        body = await _json_body_or_empty(request)
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    predicate_raw = body.get("predicate")
    if not isinstance(predicate_raw, str) or not predicate_raw.strip():
        return JSONResponse({"error": "predicate_required"}, status_code=400)
    predicate = predicate_raw.strip()

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        async with conn.transaction():
            denied = await _ensure_recommendation_visible(conn, auth, rec_id)
            if denied is not None:
                return denied
            watch_id = await create_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                predicate=predicate,
                conn=conn,
            )
            await _record_recommendation_action(
                conn,
                request=request,
                auth=auth,
                action="recommendation.watch",
                resource_id=rec_id,
                metadata={
                    "watch_id": str(watch_id),
                    "predicate_chars": _text_len(predicate),
                },
            )
    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_watch_started",
        outcome="success",
    )
    return JSONResponse(
        {
            "ok": True,
            "watch_id": str(watch_id),
            "recommendation_id": str(rec_id),
        },
        status_code=200,
    )


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
        return JSONResponse({"error": "invalid_recommendation_id"}, status_code=400)

    deps = _deps(request)
    async with deps.pool.acquire() as conn:
        async with conn.transaction():
            denied = await _ensure_recommendation_visible(conn, auth, rec_id)
            if denied is not None:
                return denied
            await clear_watch(
                tenant_id=auth.tenant_id,
                recommendation_id=rec_id,
                actor_id=auth.actor_id,
                conn=conn,
            )
            await _record_recommendation_action(
                conn,
                request=request,
                auth=auth,
                action="recommendation.unwatch",
                resource_id=rec_id,
                metadata={},
            )
    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_watch_cleared",
        outcome="success",
    )
    return JSONResponse({"ok": True}, status_code=200)


async def triage_recommendation_endpoint(
    recommendation_id: str,
    request: Request,
) -> JSONResponse:
    from services.product.recommendations.handlers import AlreadyArchivedError
    from services.product.today import TriageError, triage_recommendation

    auth = _auth(request)
    if auth is None:
        return _unauth("missing_bearer")
    try:
        rec_id = UUID(recommendation_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "invalid_recommendation_id"}, status_code=400)
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
                "reason": ("use /act for act; one of hold/route/snooze/dismiss here"),
            },
            status_code=400,
        )

    reason = body.get("reason") if isinstance(body.get("reason"), str) else None
    routed_to = (
        body.get("routed_to") if isinstance(body.get("routed_to"), str) else None
    )
    snooze_until_raw = body.get("snooze_until")
    snooze_until = None
    if isinstance(snooze_until_raw, str) and snooze_until_raw.strip():
        try:
            snooze_until = datetime.fromisoformat(snooze_until_raw)
        except ValueError:
            return JSONResponse({"error": "invalid_snooze_until"}, status_code=400)

    deps = _deps(request)
    try:
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                denied = await _ensure_recommendation_visible(
                    conn, auth, rec_id, claim_role="recommendation",
                )
                if denied is not None:
                    return denied
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
                await _record_recommendation_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="recommendation.triage",
                    resource_id=rec_id,
                    metadata={
                        "triage_action": result.action,
                        "reason_chars": _text_len(reason),
                        "routed_to_chars": _text_len(routed_to),
                        "snooze_until": (
                            snooze_until.isoformat()
                            if snooze_until is not None
                            else None
                        ),
                    },
                )
    except AlreadyArchivedError as e:
        return JSONResponse(
            {"error": "already_archived", "detail": e.to_dict()},
            status_code=409,
        )
    except TriageError as e:
        return JSONResponse({"error": e.code, "detail": e.to_dict()}, status_code=400)
    except ValidationError as e:
        return JSONResponse(
            {"error": "validation_error", "detail": e.to_dict()},
            status_code=400,
        )
    except CompanyOSError as e:
        return JSONResponse({"error": e.code, "detail": e.to_dict()}, status_code=400)

    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_triage",
        outcome="success",
    )
    return JSONResponse(
        {
            "ok": True,
            "recommendation_id": str(result.recommendation_id),
            "action": result.action,
        },
        status_code=200,
    )


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


async def _can_read_recommendation_view(
    conn: Any,
    auth: AuthContext,
    view: Any,
) -> bool:
    model_decision = await can_read_by_id(
        auth.actor_id,
        "model",
        view.id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        model_decision,
        actor_id=auth.actor_id,
        entity_type="model",
        entity_id=view.id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    if not model_decision.allowed:
        return False
    target_decision = await _target_ref_decision(
        conn, auth, view.target_act_ref,
    )
    return target_decision is None or target_decision.allowed


async def _ensure_recommendation_visible(
    conn: Any,
    auth: AuthContext,
    model_id: UUID,
    *,
    claim_role: str | None = None,
) -> JSONResponse | None:
    row = await _fetch_recommendation_access_row(
        conn,
        tenant_id=auth.tenant_id,
        model_id=model_id,
        claim_role=claim_role,
    )
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    if row["target_actor_id"] is not None and row["target_actor_id"] != auth.actor_id:
        return _forbidden("not_target_actor")

    model_decision = await can_read_by_id(
        auth.actor_id,
        "model",
        model_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        model_decision,
        actor_id=auth.actor_id,
        entity_type="model",
        entity_id=model_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    if not model_decision.allowed:
        return _forbidden(model_decision.reason)

    proposition = _coerce_jsonb(row["proposition"])
    target_decision = await _target_ref_decision(
        conn,
        auth,
        proposition.get("target_act_ref"),
    )
    if target_decision is not None and not target_decision.allowed:
        return _forbidden(target_decision.reason)
    return None


async def _fetch_recommendation_access_row(
    conn: Any,
    *,
    tenant_id: UUID,
    model_id: UUID,
    claim_role: str | None,
) -> Any:
    roles = ["recommendation", "hypothesis"] if claim_role is None else [claim_role]
    return await conn.fetchrow(
        """
        SELECT target_actor_id, proposition
        FROM models
        WHERE id = $1
          AND tenant_id = $2
          AND claim_role = ANY($3::text[])
        """,
        model_id,
        tenant_id,
        roles,
    )


async def _target_ref_decision(
    conn: Any,
    auth: AuthContext,
    target_act_ref: Any,
) -> AccessDecision | None:
    if not target_act_ref:
        return None
    if not isinstance(target_act_ref, dict):
        return AccessDecision(False, "recommendation_target_invalid")
    target_kind = target_act_ref.get("type")
    target_id_raw = target_act_ref.get("id")
    if target_kind is None or target_id_raw is None:
        return AccessDecision(False, "recommendation_target_incomplete")
    access_kind = _TARGET_ACCESS_KIND.get(str(target_kind))
    if access_kind is None:
        return AccessDecision(False, "recommendation_target_kind_unsupported")
    try:
        target_id = (
            target_id_raw
            if isinstance(target_id_raw, UUID)
            else UUID(str(target_id_raw))
        )
    except (ValueError, TypeError):
        return AccessDecision(False, "recommendation_target_id_invalid")

    decision = await can_read_by_id(
        auth.actor_id,
        access_kind,
        target_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type=access_kind,
        entity_id=target_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    return None if decision.allowed else decision


def _forbidden(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "forbidden", "reason": reason},
        status_code=status.HTTP_403_FORBIDDEN,
    )


async def _record_recommendation_action(
    conn: Any,
    *,
    request: Request,
    auth: AuthContext,
    action: str,
    resource_id: UUID,
    metadata: dict[str, Any],
) -> None:
    out: dict[str, Any] = {}
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        out["request_id"] = str(request_id)
    for key, value in metadata.items():
        if value is not None:
            out[key] = value
    await record_product_action(
        conn,
        tenant_id=auth.tenant_id,
        actor_id=auth.actor_id,
        action=action,
        resource_type="recommendation",
        resource_id=resource_id,
        metadata=out,
    )


def _text_len(value: Any) -> int:
    return len(value.strip()) if isinstance(value, str) else 0


def _coerce_jsonb(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        import json
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


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
        "is_system_hypothesis": bool(getattr(view, "is_system_hypothesis", False)),
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
