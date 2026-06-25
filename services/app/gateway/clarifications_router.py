"""Gateway routes for user-facing clarification requests."""
from __future__ import annotations

import contextlib
import json
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.app.gateway.auth import AuthContext
from services.domain.clarifications import (
    answer_clarification_request,
    dismiss_clarification_request,
    get_clarification_request,
    list_clarification_requests,
)
from services.domain.acts import commitments as commitments_svc
from services.domain.resources import repo as resources_repo
from services.domain.substrate_candidates import get_substrate_candidate
from services.domain.substrate_promotion import apply_candidate_resolution_answer
from services.domain.triggers import enqueue_trigger
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read_by_id,
)
from services.platform.access_control.roles import has_role
from services.platform.product_action_audit import record_product_action


_OBJECT_ACCESS_KINDS: dict[str, EntityKind] = {
    "observation": "observation",
    "model": "model",
    "commitment": "commitment",
    "goal": "goal",
    "decision": "decision",
    "resource": "resource",
    "customer": "resource",
}

_SAFE_ANSWER_ACTIONS = {
    "accept_candidate",
    "create_actor",
    "create_new_entity",
    "keep_provisional",
    "link_existing",
    "merge",
    "not_same_entity",
    "promote_actor",
    "promote_commitment",
    "promote_pattern_candidate",
    "promote_resource",
    "reject",
    "reject_candidate",
}

_SAFE_ENTITY_TYPES = {
    "actor",
    "commitment",
    "customer",
    "organization",
    "person",
    "project",
    "resource",
    "service",
    "system",
    "vendor",
    "workstream",
}


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
        visible_rows = []
        for row in rows:
            decision = await _clarification_access_decision(conn, auth, row)
            if decision.allowed:
                visible_rows.append(row)
    return JSONResponse(
        {
            "items": [row.to_dict() for row in visible_rows],
            "count": len(visible_rows),
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
                existing = await get_clarification_request(
                    conn,
                    tenant_id=auth.tenant_id,
                    request_id=cid,
                )
                if existing is None:
                    row = None
                    return JSONResponse({"error": "not_found"}, status_code=404)
                decision = await _clarification_access_decision(conn, auth, existing)
                if not decision.allowed:
                    row = None
                    return _forbidden(decision.reason)
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
                    await _record_clarification_action(
                        conn,
                        request=request,
                        auth=auth,
                        action="clarification.answer",
                        resource_id=cid,
                        row=row,
                        metadata=_clarification_answer_metadata(row, body.answer),
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
        async with _optional_transaction(conn):
            existing = await get_clarification_request(
                conn,
                tenant_id=auth.tenant_id,
                request_id=cid,
            )
            if existing is None:
                return JSONResponse({"error": "not_found"}, status_code=404)
            decision = await _clarification_access_decision(conn, auth, existing)
            if not decision.allowed:
                return _forbidden(decision.reason)
            row = await dismiss_clarification_request(
                conn,
                tenant_id=auth.tenant_id,
                request_id=cid,
                reason=body.reason,
                answered_by=auth.actor_id,
            )
            if row is not None:
                await _record_clarification_action(
                    conn,
                    request=request,
                    auth=auth,
                    action="clarification.dismiss",
                    resource_id=cid,
                    row=row,
                    metadata={"reason_chars": _text_len(body.reason)},
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


async def _record_clarification_action(
    conn: Any,
    *,
    request: Request,
    auth: AuthContext,
    action: str,
    resource_id: UUID,
    row: Any,
    metadata: dict[str, Any],
) -> None:
    out = _clarification_base_metadata(request, row)
    for key, value in metadata.items():
        if value is not None:
            out[key] = value
    await record_product_action(
        conn,
        tenant_id=auth.tenant_id,
        actor_id=auth.actor_id,
        action=action,
        resource_type="clarification_request",
        resource_id=resource_id,
        metadata=out,
    )


def _clarification_base_metadata(request: Request, row: Any) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        metadata["request_id"] = str(request_id)

    kind = _safe_token(getattr(row, "kind", None))
    if kind:
        metadata["clarification_kind"] = kind
    object_kind = _safe_token(getattr(row, "object_kind", None))
    if object_kind:
        metadata["object_kind"] = object_kind

    metadata["has_source_observation"] = _coerce_uuid(
        getattr(row, "source_observation_id", None)
    ) is not None
    metadata["has_model"] = _coerce_uuid(getattr(row, "model_id", None)) is not None
    metadata["has_object"] = _coerce_uuid(getattr(row, "object_id", None)) is not None
    return metadata


def _clarification_answer_metadata(
    row: Any,
    answer: dict[str, Any],
) -> dict[str, Any]:
    normalized = _normalized_answer(answer)
    metadata: dict[str, Any] = {
        "answer_keys": _safe_answer_keys(normalized),
    }
    action = _safe_token(normalized.get("action"), allowed=_SAFE_ANSWER_ACTIONS)
    if action:
        metadata["answer_action"] = action
    entity_type = _safe_token(
        normalized.get("entity_type")
        or normalized.get("canonical_type")
        or normalized.get("type")
        or normalized.get("kind"),
        allowed=_SAFE_ENTITY_TYPES,
    )
    if entity_type:
        metadata["entity_type"] = entity_type
    metadata["answer_value_is_nested"] = isinstance(answer.get("value"), dict)
    metadata["created_side_effects"] = getattr(row, "status", None) == "answered"
    return metadata


def _safe_answer_keys(answer: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    saw_other = False
    for raw_key in sorted(answer.keys(), key=str)[:16]:
        safe = _safe_token(raw_key)
        if safe and safe != "other":
            keys.append(safe)
        else:
            saw_other = True
    if saw_other:
        keys.append("other")
    return keys


def _safe_token(value: Any, *, allowed: set[str] | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().casefold().replace("-", "_")
    if not text:
        return None
    if allowed is not None:
        return text if text in allowed else "other"
    safe_chars = set("abcdefghijklmnopqrstuvwxyz0123456789_.")
    if len(text) <= 80 and all(char in safe_chars for char in text):
        return text
    return "other"


def _text_len(value: Any) -> int:
    return len(value.strip()) if isinstance(value, str) else 0


async def _clarification_access_decision(
    conn: Any,
    auth: AuthContext,
    row: Any,
) -> AccessDecision:
    decisions: list[AccessDecision] = []
    source_observation_id = _coerce_uuid(row.source_observation_id)
    if source_observation_id is not None:
        decisions.append(
            await _entity_access_decision(
                conn,
                auth,
                "observation",
                source_observation_id,
            )
        )

    model_id = _coerce_uuid(row.model_id)
    if model_id is not None:
        decisions.append(
            await _entity_access_decision(conn, auth, "model", model_id)
        )

    object_kind = str(row.object_kind or "").strip().casefold()
    object_id = _coerce_uuid(row.object_id)
    if object_id is not None and object_kind == "substrate_candidate":
        decisions.extend(
            await _substrate_candidate_access_decisions(conn, auth, object_id)
        )
    elif object_id is not None:
        entity_kind = _OBJECT_ACCESS_KINDS.get(object_kind)
        if entity_kind is not None:
            decisions.append(
                await _entity_access_decision(conn, auth, entity_kind, object_id)
            )

    if not decisions:
        return await _targetless_clarification_decision(conn, auth, row)
    for decision in decisions:
        if not decision.allowed:
            return decision
    return AccessDecision(True, "clarification_anchors_visible")


async def _entity_access_decision(
    conn: Any,
    auth: AuthContext,
    entity_kind: EntityKind,
    entity_id: UUID,
) -> AccessDecision:
    decision = await can_read_by_id(
        auth.actor_id,
        entity_kind,
        entity_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    await record_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type=entity_kind,
        entity_id=entity_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )
    return decision


async def _substrate_candidate_access_decisions(
    conn: Any,
    auth: AuthContext,
    candidate_id: UUID,
) -> list[AccessDecision]:
    candidate = await get_substrate_candidate(
        conn,
        tenant_id=auth.tenant_id,
        candidate_id=candidate_id,
    )
    if candidate is None:
        return [AccessDecision(False, "clarification_object_not_found")]

    decisions: list[AccessDecision] = []
    for observation_id in candidate.evidence_observation_ids:
        decisions.append(
            await _entity_access_decision(conn, auth, "observation", observation_id)
        )
    for model_id in candidate.evidence_model_ids:
        decisions.append(await _entity_access_decision(conn, auth, "model", model_id))
    return decisions


async def _targetless_clarification_decision(
    conn: Any,
    auth: AuthContext,
    row: Any,
) -> AccessDecision:
    if await has_role(auth.actor_id, "admin", conn=conn, tenant_id=auth.tenant_id):
        decision = AccessDecision(True, "admin_override", override_applied=True)
        await record_override_if_needed(
            decision,
            actor_id=auth.actor_id,
            entity_type="clarification_request",
            entity_id=_coerce_uuid(row.id),
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
        decision = AccessDecision(True, "leadership_override", override_applied=True)
        await record_override_if_needed(
            decision,
            actor_id=auth.actor_id,
            entity_type="clarification_request",
            entity_id=_coerce_uuid(row.id),
            conn=conn,
            tenant_id=auth.tenant_id,
        )
        return decision
    return AccessDecision(False, "clarification_without_visible_anchor")


def _forbidden(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "forbidden", "reason": reason},
        status_code=status.HTTP_403_FORBIDDEN,
    )


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

    if row.kind == "entity_resolution" and row.object_kind == "entity_review":
        await _apply_entity_resolution_answer(
            conn,
            row=row,
            answer=answer,
            tenant_id=tenant_id,
            answered_by=answered_by,
        )


async def _apply_entity_resolution_answer(
    conn: Any,
    *,
    row: Any,
    answer: dict[str, Any],
    tenant_id: UUID,
    answered_by: UUID | None,
) -> None:
    normalized = _normalized_answer(answer)
    action = str(normalized.get("action") or "").strip()
    if action == "accept_candidate":
        payload = row.payload or {}
        phrase = str(payload.get("phrase") or "").strip()
        canonical_ref = normalized.get("canonical_ref") or _first_candidate_ref(payload)
        if not phrase:
            raise ValidationError("entity resolution answer missing phrase")
        if not isinstance(canonical_ref, dict) or not canonical_ref.get("type"):
            raise ValidationError("entity resolution answer missing canonical_ref")
        confidence = float(
            normalized.get("confidence")
            or _first_candidate_confidence(payload)
            or 1.0
        )
        await _finalize_entity_resolution(
            conn,
            row=row,
            tenant_id=tenant_id,
            answered_by=answered_by,
            phrase=phrase,
            canonical_ref=canonical_ref,
            confidence=confidence,
        )
        return
    if action in {"reject_candidate", "not_same_entity"}:
        await _mark_entity_review_dismissed(
            conn,
            review_id=row.object_id,
            tenant_id=tenant_id,
            answered_by=answered_by,
            reason=action,
        )
        return
    if action == "create_new_entity":
        payload = row.payload or {}
        phrase = str(
            normalized.get("label")
            or normalized.get("identity")
            or normalized.get("display_name")
            or payload.get("phrase")
            or ""
        ).strip()
        if not phrase:
            raise ValidationError("entity creation answer missing label or phrase")
        canonical_ref = normalized.get("canonical_ref")
        if not isinstance(canonical_ref, dict):
            canonical_ref = await _create_new_entity_from_answer(
                conn,
                row=row,
                tenant_id=tenant_id,
                answer=normalized,
                phrase=phrase,
            )
        elif not canonical_ref.get("type"):
            raise ValidationError("entity creation canonical_ref missing type")
        confidence = float(normalized.get("confidence") or 1.0)
        await _finalize_entity_resolution(
            conn,
            row=row,
            tenant_id=tenant_id,
            answered_by=answered_by,
            phrase=phrase,
            canonical_ref=canonical_ref,
            confidence=confidence,
        )
        return
    raise ValidationError("entity resolution answer action is invalid")


def _normalized_answer(answer: dict[str, Any]) -> dict[str, Any]:
    value = answer.get("value")
    if isinstance(value, dict):
        merged = dict(value)
        merged.update({k: v for k, v in answer.items() if k != "value"})
        return merged
    return dict(answer)


def _first_candidate_ref(payload: dict[str, Any]) -> dict[str, Any] | None:
    candidates = payload.get("candidates") or []
    if not candidates:
        return None
    ref = candidates[0].get("canonical_ref") if isinstance(candidates[0], dict) else None
    return dict(ref) if isinstance(ref, dict) else None


def _first_candidate_confidence(payload: dict[str, Any]) -> float | None:
    candidates = payload.get("candidates") or []
    if not candidates or not isinstance(candidates[0], dict):
        return None
    raw = candidates[0].get("confidence")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


async def _finalize_entity_resolution(
    conn: Any,
    *,
    row: Any,
    tenant_id: UUID,
    answered_by: UUID | None,
    phrase: str,
    canonical_ref: dict[str, Any],
    confidence: float,
) -> None:
    observation_id = _coerce_uuid(row.source_observation_id)
    await _insert_manual_entity_alias(
        conn,
        tenant_id=tenant_id,
        phrase=phrase,
        canonical_ref=canonical_ref,
        confidence=confidence,
        source_event_id=observation_id,
    )
    await _mark_entity_review_resolved(
        conn,
        review_id=row.object_id,
        tenant_id=tenant_id,
        answered_by=answered_by,
        chosen_ref=canonical_ref,
    )
    if observation_id is not None:
        await _append_entity_to_observation(
            conn,
            tenant_id=tenant_id,
            observation_id=observation_id,
            entity_ref=canonical_ref,
        )
        await _emit_entity_resolution_state_change(
            conn,
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            entity_ref=canonical_ref,
            confidence=confidence,
        )
        await _maybe_enqueue_entity_resolution_trigger(
            conn,
            tenant_id=tenant_id,
            observation_id=observation_id,
            entity_ref=canonical_ref,
        )


async def _create_new_entity_from_answer(
    conn: Any,
    *,
    row: Any,
    tenant_id: UUID,
    answer: dict[str, Any],
    phrase: str,
) -> dict[str, Any]:
    entity_type = _new_entity_type(answer)
    if entity_type == "actor":
        return await _create_actor_entity(
            conn,
            tenant_id=tenant_id,
            answer=answer,
            phrase=phrase,
            source_observation_id=_coerce_uuid(row.source_observation_id),
        )
    if entity_type in {"customer", "vendor", "system", "workstream", "resource"}:
        return await _create_resource_entity(
            conn,
            tenant_id=tenant_id,
            answer=answer,
            phrase=phrase,
            source_observation_id=_require_source_observation(row),
            semantic_kind=entity_type,
        )
    if entity_type == "commitment":
        return await _create_commitment_entity(
            conn,
            tenant_id=tenant_id,
            answer=answer,
            phrase=phrase,
            source_observation_id=_require_source_observation(row),
        )
    raise ValidationError(
        "entity creation answer has unsupported entity_type",
        field="answer.entity_type",
        value=entity_type,
    )


def _new_entity_type(answer: dict[str, Any]) -> str:
    raw = (
        answer.get("entity_type")
        or answer.get("canonical_type")
        or answer.get("type")
        or answer.get("kind")
        or ""
    )
    value = str(raw).strip().casefold().replace("-", "_")
    aliases = {
        "person": "actor",
        "human": "actor",
        "user": "actor",
        "organization": "customer",
        "account": "customer",
        "project": "workstream",
        "service": "system",
    }
    value = aliases.get(value, value)
    if not value:
        raise ValidationError(
            "create_new_entity answers require entity_type",
            field="answer.entity_type",
        )
    return value


async def _create_actor_entity(
    conn: Any,
    *,
    tenant_id: UUID,
    answer: dict[str, Any],
    phrase: str,
    source_observation_id: UUID | None,
) -> dict[str, Any]:
    actor_type = str(answer.get("actor_type") or "human_internal").strip()
    if actor_type not in {"human_internal", "human_external", "ai_agent"}:
        raise ValidationError(
            "entity creation actor_type is invalid",
            field="answer.actor_type",
            value=actor_type,
        )
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email,
            status, metadata, specification_id,
            created_at, last_seen_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            'active', $6::jsonb, NULL,
            now(), NULL
        )
        """,
        actor_id,
        tenant_id,
        actor_type,
        str(answer.get("display_name") or phrase).strip(),
        answer.get("email"),
        json.dumps(
            {
                "source": "entity_resolution_clarification",
                "source_observation_id": (
                    str(source_observation_id) if source_observation_id else None
                ),
            },
            sort_keys=True,
            default=str,
        ),
    )
    return {"type": "actor", "id": str(actor_id)}


async def _create_resource_entity(
    conn: Any,
    *,
    tenant_id: UUID,
    answer: dict[str, Any],
    phrase: str,
    source_observation_id: UUID,
    semantic_kind: str,
) -> dict[str, Any]:
    resource_kind = str(
        answer.get("resource_kind") or _resource_kind_for_entity_type(semantic_kind)
    )
    resource = await resources_repo.create(
        kind=resource_kind,
        identity=str(answer.get("identity") or phrase).strip(),
        description=str(
            answer.get("description")
            or f"Canonical {semantic_kind} created from entity clarification."
        ),
        current_value={
            "semantic_kind": semantic_kind,
            "label": phrase,
            "source": "entity_resolution_clarification",
        },
        valuation_confidence=float(answer.get("confidence") or 1.0),
        metadata={
            "source": "entity_resolution_clarification",
            "semantic_kind": semantic_kind,
        },
        created_by_event_id=source_observation_id,
        tenant_id=tenant_id,
        conn=conn,
    )
    if semantic_kind == "customer":
        return {
            "type": "customer",
            "id": str(resource.id),
            "resource_id": str(resource.id),
        }
    return {
        "type": "resource",
        "id": str(resource.id),
        "semantic_kind": semantic_kind,
    }


def _resource_kind_for_entity_type(entity_type: str) -> str:
    if entity_type in {"customer", "vendor"}:
        return "relational"
    if entity_type == "system":
        return "infrastructure"
    if entity_type == "workstream":
        return "capacity"
    return "relational"


async def _create_commitment_entity(
    conn: Any,
    *,
    tenant_id: UUID,
    answer: dict[str, Any],
    phrase: str,
    source_observation_id: UUID,
) -> dict[str, Any]:
    commitment = await commitments_svc.create(
        title=str(answer.get("title") or phrase).strip(),
        description=str(
            answer.get("description")
            or "Proposed commitment created from entity clarification."
        ),
        initial_state=str(answer.get("initial_state") or "proposed"),
        ambition_level=str(answer.get("ambition_level") or "base"),
        priority=int(answer.get("priority") or 5),
        success_criteria={
            "source": "entity_resolution_clarification",
            "phrase": phrase,
        },
        estimated_capacity={
            "source": "entity_resolution_clarification",
            "maintenance": True,
        },
        is_maintenance=True,
        created_by_event_id=source_observation_id,
        tenant_id=tenant_id,
        conn=conn,
    )
    return {"type": "commitment", "id": str(commitment.id)}


def _require_source_observation(row: Any) -> UUID:
    observation_id = _coerce_uuid(row.source_observation_id)
    if observation_id is None:
        raise ValidationError(
            "entity creation requires source_observation_id",
            field="source_observation_id",
        )
    return observation_id


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError):
        return None


async def _insert_manual_entity_alias(
    conn: Any,
    *,
    tenant_id: UUID,
    phrase: str,
    canonical_ref: dict[str, Any],
    confidence: float,
    source_event_id: UUID | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO entity_aliases (
          id, tenant_id, alias_text, actor_id, resolved_entity_ref,
          is_canonical, entity_metadata, confidence, confirmed_count,
          contested_count, source_event_id
        )
        SELECT $1, $2, $3, NULL, $4::jsonb,
               false, $5::jsonb, $6, 1, 0, $7
        WHERE NOT EXISTS (
          SELECT 1
          FROM entity_aliases
          WHERE tenant_id = $2
            AND alias_text = $3
            AND actor_id IS NULL
            AND resolved_entity_ref = $4::jsonb
        )
        """,
        uuid7(),
        tenant_id,
        phrase,
        json.dumps(canonical_ref, sort_keys=True, default=str),
        json.dumps({"source": "manual", "clarification_kind": "entity_resolution"}),
        max(0.0, min(1.0, confidence)),
        source_event_id,
    )


async def _mark_entity_review_resolved(
    conn: Any,
    *,
    review_id: UUID,
    tenant_id: UUID,
    answered_by: UUID | None,
    chosen_ref: dict[str, Any],
) -> None:
    await conn.execute(
        """
        UPDATE entity_review_queue
        SET resolved_at = now(),
            resolved_by = $3,
            chosen_ref = $4::jsonb,
            dismissed_reason = NULL
        WHERE id = $1 AND tenant_id = $2 AND resolved_at IS NULL
        """,
        review_id,
        tenant_id,
        answered_by,
        json.dumps(chosen_ref, sort_keys=True, default=str),
    )


async def _mark_entity_review_dismissed(
    conn: Any,
    *,
    review_id: UUID,
    tenant_id: UUID,
    answered_by: UUID | None,
    reason: str,
) -> None:
    await conn.execute(
        """
        UPDATE entity_review_queue
        SET resolved_at = now(),
            resolved_by = $3,
            dismissed_reason = $4
        WHERE id = $1 AND tenant_id = $2 AND resolved_at IS NULL
        """,
        review_id,
        tenant_id,
        answered_by,
        reason,
    )


async def _append_entity_to_observation(
    conn: Any,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    entity_ref: dict[str, Any],
) -> None:
    await conn.execute(
        """
        UPDATE observations
        SET entities_mentioned = (
            CASE
              WHEN entities_mentioned @> $3::jsonb THEN entities_mentioned
              ELSE COALESCE(entities_mentioned, '[]'::jsonb) || $3::jsonb
            END
        )
        WHERE id = $1 AND tenant_id = $2
        """,
        observation_id,
        tenant_id,
        json.dumps([entity_ref], sort_keys=True, default=str),
    )


async def _emit_entity_resolution_state_change(
    conn: Any,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    entity_ref: dict[str, Any],
    confidence: float,
) -> None:
    content = {
        "_state_change_kind": "entity_late_resolution",
        "phrase": phrase,
        "entity_ref": entity_ref,
        "confidence": confidence,
        "source_observation_id": str(observation_id),
        "source": "clarification_answer",
    }
    content_text = (
        f"phrase {phrase!r} resolved to type={entity_ref.get('type')} "
        f"id={entity_ref.get('id')} (conf={confidence:.2f})"
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, trust_tier, cause_id
        ) VALUES (
          $1, $2, now(), 'state_change', 'internal:state_change',
          $3::jsonb, $4, 'authoritative', $5
        )
        """,
        uuid7(),
        tenant_id,
        json.dumps(content, sort_keys=True, default=str),
        content_text,
        observation_id,
    )


async def _maybe_enqueue_entity_resolution_trigger(
    conn: Any,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    entity_ref: dict[str, Any],
) -> None:
    if entity_ref.get("type") not in {"customer", "commitment", "goal"}:
        return
    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = 'think_trigger_queue'
              AND c.relkind IN ('r', 'p')
        )
        """
    )
    if not exists:
        return
    await enqueue_trigger(
        conn,
        tenant_id=tenant_id,
        trigger_kind="T1",
        trigger_subkind="entity_resolved_late",
        observation_id=observation_id,
        payload={"entity_ref": entity_ref},
    )


def _unauth(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "reason": reason},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


__all__ = ["build_clarifications_router"]
