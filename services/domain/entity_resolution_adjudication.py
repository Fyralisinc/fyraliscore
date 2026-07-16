"""Adjudicate answered entity-resolution clarifications.

This module owns the domain transition from one reviewed clarification answer
to canonical entity state. Transport layers may record the answer, then call
the public operation here inside the same transaction.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.shared.entity_phrases import phrase_requires_context
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.domain.acts import commitments as commitments_svc
from services.domain.clarifications import ClarificationRequest
from services.domain.entity_aliases.repo import insert_alias_with_connection
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.resources import repo as resources_repo
from services.domain.source_semantics.repo import SourceSemanticRepo


async def adjudicate_entity_resolution_clarification(
    conn: asyncpg.Connection,
    *,
    clarification: ClarificationRequest,
    answer: dict[str, Any],
    tenant_id: UUID,
    answered_by: UUID | None,
) -> None:
    """Apply one answered entity-resolution clarification atomically.

    ``clarification`` is the answered row returned by
    :func:`answer_clarification_request`. The caller owns the surrounding
    transaction so recording the answer and applying its canonical side
    effects succeed or fail together.
    """

    if clarification.kind != "entity_resolution" or clarification.object_kind not in {
        "entity_review",
        "grounding_trace",
    }:
        raise ValidationError(
            "clarification is not an entity-resolution adjudication"
        )

    normalized = _normalized_answer(answer)
    action = str(normalized.get("action") or "").strip()
    if action == "accept_candidate":
        payload = clarification.payload or {}
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
            clarification=clarification,
            tenant_id=tenant_id,
            answered_by=answered_by,
            phrase=phrase,
            canonical_ref=canonical_ref,
            confidence=confidence,
            resolution_scope=resolution_scope,
        )
        return
    if action in {"reject_candidate", "not_same_entity"}:
        if (
            clarification.object_kind == "entity_review"
            and clarification.object_id is not None
        ):
            await _mark_entity_review_dismissed(
                conn,
                review_id=clarification.object_id,
                tenant_id=tenant_id,
                answered_by=answered_by,
                reason=action,
            )
        return
    if action == "create_new_entity":
        payload = clarification.payload or {}
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
                clarification=clarification,
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
            clarification=clarification,
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
        merged.update({key: value for key, value in answer.items() if key != "value"})
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
    scope = str(answer.get("resolution_scope") or "source_context_only").strip()
    if scope not in {"source_context_only", "tenant_global_exact"}:
        raise ValidationError("entity resolution scope is invalid")
    if scope == "tenant_global_exact" and phrase_requires_context(phrase):
        raise ValidationError(
            "context-dependent phrases cannot become tenant-global aliases"
        )
    return scope


async def _authorize_resolution_scope(
    conn: asyncpg.Connection,
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
    conn: asyncpg.Connection,
    *,
    clarification: ClarificationRequest,
    tenant_id: UUID,
    answered_by: UUID | None,
    phrase: str,
    canonical_ref: dict[str, Any],
    confidence: float,
    resolution_scope: str,
) -> None:
    observation_id = _coerce_uuid(clarification.source_observation_id)
    payload = clarification.payload or {}
    feedback_lineage = payload.get("feedback_lineage")
    if not isinstance(feedback_lineage, dict):
        feedback_lineage = {}
    clarification_request_id = _coerce_uuid(clarification.id)
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
    if clarification.object_kind == "entity_review" and clarification.object_id is not None:
        await _mark_entity_review_resolved(
            conn,
            review_id=clarification.object_id,
            tenant_id=tenant_id,
            answered_by=answered_by,
            chosen_ref=canonical_ref,
        )


async def _create_new_entity_from_answer(
    conn: asyncpg.Connection,
    *,
    clarification: ClarificationRequest,
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
            source_observation_id=_coerce_uuid(clarification.source_observation_id),
        )
    if entity_type in {"customer", "vendor", "system", "workstream", "resource"}:
        return await _create_resource_entity(
            conn,
            tenant_id=tenant_id,
            answer=answer,
            phrase=phrase,
            source_observation_id=_require_source_observation(clarification),
            semantic_kind=entity_type,
        )
    if entity_type == "commitment":
        return await _create_commitment_entity(
            conn,
            tenant_id=tenant_id,
            answer=answer,
            phrase=phrase,
            source_observation_id=_require_source_observation(clarification),
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
    conn: asyncpg.Connection,
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
    conn: asyncpg.Connection,
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
    conn: asyncpg.Connection,
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


def _require_source_observation(clarification: ClarificationRequest) -> UUID:
    observation_id = _coerce_uuid(clarification.source_observation_id)
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
    conn: asyncpg.Connection,
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
            "autonomous_replay_eligible": resolution_scope == "tenant_global_exact",
            "replay_policy_version": "governed-exact-alias-replay-v1",
            "grounding_feedback_lineage": feedback_lineage,
        },
        adjudicated=True,
    )


async def _mark_entity_review_resolved(
    conn: asyncpg.Connection,
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
    conn: asyncpg.Connection,
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


__all__ = ["adjudicate_entity_resolution_clarification"]
