"""Gateway routes for user-facing clarification requests."""
from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import ValidationError
from lib.shared.entity_phrases import phrase_requires_context
from lib.shared.ids import uuid7
from services.app.gateway.auth import AuthContext
from services.domain.clarifications import (
    answer_clarification_request,
    dismiss_clarification_request,
    list_clarification_requests,
)
from services.domain.entity_aliases.repo import insert_alias_with_connection
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.source_semantics.repo import SourceSemanticRepo
from services.domain.acts import commitments as commitments_svc
from services.domain.resources import repo as resources_repo
from services.domain.substrate_candidates import get_substrate_candidate
from services.domain.substrate_promotion import apply_candidate_resolution_answer
from services.domain.triggers import enqueue_trigger


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
        reviewed_candidate = _select_reviewed_candidate(
            payload,
            proposed_ref=normalized.get("canonical_ref"),
        )
        canonical_ref = reviewed_candidate["canonical_ref"]
        if not phrase:
            raise ValidationError("entity resolution answer missing phrase")
        confidence = float(
            normalized.get("confidence")
            or reviewed_candidate.get("confidence")
            or 1.0
        )
        resolution_scope = _resolution_scope(normalized, phrase=phrase)
        await _authorize_resolution_scope(
            conn,
            tenant_id=tenant_id,
            answered_by=answered_by,
            resolution_scope=resolution_scope,
            explicitly_confirmed=bool(
                normalized.get("confirm_tenant_global_reuse")
            ),
        )
        await _finalize_entity_resolution(
            conn,
            row=row,
            tenant_id=tenant_id,
            answered_by=answered_by,
            phrase=phrase,
            canonical_ref=canonical_ref,
            confidence=confidence,
            resolution_scope=resolution_scope,
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
        resolution_scope = _resolution_scope(normalized, phrase=phrase)
        await _authorize_resolution_scope(
            conn,
            tenant_id=tenant_id,
            answered_by=answered_by,
            resolution_scope=resolution_scope,
            explicitly_confirmed=bool(
                normalized.get("confirm_tenant_global_reuse")
            ),
        )
        await _finalize_entity_resolution(
            conn,
            row=row,
            tenant_id=tenant_id,
            answered_by=answered_by,
            phrase=phrase,
            canonical_ref=canonical_ref,
            confidence=confidence,
            resolution_scope=resolution_scope,
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


def _select_reviewed_candidate(
    payload: dict[str, Any],
    *,
    proposed_ref: Any,
) -> dict[str, Any]:
    candidates = [
        item
        for item in (payload.get("candidates") or [])
        if isinstance(item, dict) and isinstance(item.get("canonical_ref"), dict)
    ]
    if not candidates:
        raise ValidationError("entity resolution answer has no reviewed candidates")
    wanted = proposed_ref or candidates[0]["canonical_ref"]
    if not isinstance(wanted, dict):
        raise ValidationError("entity resolution answer missing canonical_ref")

    def identity(ref: dict[str, Any]) -> tuple[str, str, int] | None:
        entity_type = str(ref.get("type") or "").strip()
        entity_id = str(ref.get("id") or "").strip()
        if not entity_type or not entity_id:
            return None
        try:
            version = int(ref.get("version", 1))
        except (TypeError, ValueError):
            return None
        return entity_type, entity_id, version

    wanted_identity = identity(wanted)
    for candidate in candidates:
        candidate_ref = candidate["canonical_ref"]
        if wanted_identity is not None and identity(candidate_ref) == wanted_identity:
            return candidate
    raise ValidationError(
        "accepted canonical_ref must exactly match a reviewed candidate"
    )


def _resolution_scope(answer: dict[str, Any], *, phrase: str) -> str:
    scope = str(
        answer.get("resolution_scope") or "source_context_only"
    ).strip()
    if scope not in {"source_context_only", "tenant_global_exact"}:
        raise ValidationError("entity resolution scope is invalid")
    if scope == "tenant_global_exact" and phrase_requires_context(phrase):
        raise ValidationError(
            "context-dependent phrases cannot become tenant-global aliases"
        )
    return scope


async def _authorize_resolution_scope(
    conn: Any,
    *,
    tenant_id: UUID,
    answered_by: UUID | None,
    resolution_scope: str,
    explicitly_confirmed: bool,
) -> None:
    if resolution_scope != "tenant_global_exact":
        return
    if answered_by is None or not explicitly_confirmed:
        raise ValidationError(
            "tenant-global alias reuse requires an explicit privileged confirmation"
        )
    authorized = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM actors actor
          JOIN actor_roles role
            ON role.tenant_id=actor.tenant_id
           AND role.actor_id=actor.id
           AND role.entity_type='tenant'
           AND role.entity_id IS NULL
           AND role.role IN ('admin', 'leadership')
           AND role.revoked_at IS NULL
          WHERE actor.tenant_id=$1
            AND actor.id=$2
            AND actor.status='active'
        )
        """,
        tenant_id,
        answered_by,
    )
    if not authorized:
        raise ValidationError(
            "answerer lacks tenant-global identity-adjudication authority"
        )


async def _finalize_entity_resolution(
    conn: Any,
    *,
    row: Any,
    tenant_id: UUID,
    answered_by: UUID | None,
    phrase: str,
    canonical_ref: dict[str, Any],
    confidence: float,
    resolution_scope: str,
) -> None:
    observation_id = _coerce_uuid(row.source_observation_id)
    payload = row.payload or {}
    feedback_lineage = payload.get("feedback_lineage")
    if not isinstance(feedback_lineage, dict):
        feedback_lineage = {}
    clarification_request_id = _coerce_uuid(row.id)
    await _insert_manual_entity_alias(
        conn,
        tenant_id=tenant_id,
        phrase=phrase,
        canonical_ref=canonical_ref,
        confidence=confidence,
        source_event_id=observation_id,
        clarification_request_id=clarification_request_id,
        answered_by=answered_by,
        feedback_lineage=feedback_lineage,
        resolution_scope=resolution_scope,
    )
    original_trace_id = _coerce_uuid(feedback_lineage.get("grounding_trace_id"))
    if (
        original_trace_id is not None
        and clarification_request_id is not None
        and observation_id is not None
    ):
        successor_trace_id = await EntityGroundingRepo.append_adjudicated_successor(
            conn,
            tenant_id=tenant_id,
            original_trace_id=original_trace_id,
            clarification_request_id=clarification_request_id,
            source_observation_id=observation_id,
            phrase=phrase,
            expected_lineage=feedback_lineage,
            canonical_ref=canonical_ref,
            now=datetime.now(timezone.utc),
        )
        await SourceSemanticRepo().enqueue_work(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=successor_trace_id,
            now=datetime.now(timezone.utc),
        )
    await _mark_entity_review_resolved(
        conn,
        review_id=row.object_id,
        tenant_id=tenant_id,
        answered_by=answered_by,
        chosen_ref=canonical_ref,
    )
    if observation_id is not None:
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
    clarification_request_id: UUID | None,
    answered_by: UUID | None,
    feedback_lineage: dict[str, Any],
    resolution_scope: str,
) -> None:
    clarification_ref = (
        f"clarification-request:{clarification_request_id}"
        if clarification_request_id is not None
        else "clarification-request:unknown"
    )
    answer_digest = canonical_sha256(
        {
            "tenant_id": str(tenant_id),
            "clarification_request_id": str(clarification_request_id),
            "phrase": phrase,
            "canonical_ref": canonical_ref,
            "resolution_scope": resolution_scope,
            "answered_by": str(answered_by) if answered_by is not None else None,
            "feedback_lineage": feedback_lineage,
        }
    )
    await insert_alias_with_connection(
        conn,
        phrase=phrase,
        resolved_entity_ref=canonical_ref,
        source="manual",
        confidence=max(0.0, min(1.0, confidence)),
        tenant_id=tenant_id,
        source_event_id=source_event_id,
        extra_metadata={
            "clarification_kind": "entity_resolution",
            "clarification_request_id": (
                str(clarification_request_id)
                if clarification_request_id is not None
                else None
            ),
            "adjudicated_by": str(answered_by) if answered_by is not None else None,
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": clarification_ref,
            "adjudication_state": "active",
            "adjudication_answer_digest": answer_digest,
            "resolution_scope": resolution_scope,
            "autonomous_replay_eligible": (
                resolution_scope == "tenant_global_exact"
            ),
            "replay_policy_version": "governed-exact-alias-replay-v1",
            "grounding_feedback_lineage": feedback_lineage,
        },
        adjudicated=True,
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
