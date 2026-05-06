"""
services/recommendations/handlers.py — write-side state changes for the
recommendation surface: act, dismiss.

These handlers wrap the existing Acts modification entry points
(`services.acts.{goals,commitments,decisions}` + `services.resources.repo`)
and the Models archive path. The intent is: a CEO clicks "Act on this"
in the action list; we apply the structured `proposed_change` exactly
once, archive the recommendation, and write an audit-trail
`state_change` Observation that ties the recommendation, the actor,
and the resulting Act-layer mutation together.

All work happens inside a single asyncpg transaction owned by the
caller. Failure of the underlying Act modification rolls the whole
unit back — the recommendation stays active and the user can retry.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from services.acts import commitments as commitments_svc
from services.acts import decisions as decisions_svc
from services.acts import goals as goals_svc
from services.observations.state_change import emit_state_change
from services.resources import repo as resources_repo


class RecommendationStateError(CompanyOSError):
    default_code = "recommendation_state_error"


class AlreadyArchivedError(RecommendationStateError):
    """The recommendation has already been acted on or dismissed."""
    default_code = "recommendation_already_archived"


@dataclass
class ActResult:
    recommendation_id: UUID
    target_act_change_kind: str
    target_act_change_id: UUID
    archived_recommendation_proposition: dict[str, Any]
    archived_recommendation_natural: str


@dataclass
class DismissResult:
    recommendation_id: UUID
    reason: str


_REF_TYPE_TO_TABLE: dict[str, str] = {
    "goal": "goals",
    "commitment": "commitments",
    "decision": "decisions",
    "resource": "resources",
}


# ---------------------------------------------------------------------
# Loading + state checks
# ---------------------------------------------------------------------


async def _load_active_recommendation(
    *,
    recommendation_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, born_from_event_id, proposition,
               "natural" AS natural, status, archived_at, archive_reason,
               target_actor_id
        FROM models
        WHERE id = $1 AND tenant_id = $2
          AND proposition_kind = 'recommendation'
        """,
        recommendation_id,
        tenant_id,
    )
    if row is None:
        raise ValidationError(
            f"recommendation {recommendation_id} not found",
            recommendation_id=str(recommendation_id),
        )
    if row["archived_at"] is not None or row["status"] != "active":
        raise AlreadyArchivedError(
            f"recommendation {recommendation_id} already archived",
            archive_reason=row["archive_reason"],
            archived_at=str(row["archived_at"]),
        )
    proposition = _coerce_jsonb(row["proposition"])
    return {
        "id": row["id"],
        "tenant_id": row["tenant_id"],
        "born_from_event_id": row["born_from_event_id"],
        "proposition": proposition,
        "natural": row["natural"],
        "target_actor_id": row["target_actor_id"],
    }


# ---------------------------------------------------------------------
# Act on a recommendation — apply proposed_change + archive + emit
# ---------------------------------------------------------------------


async def act_on_recommendation(
    *,
    recommendation_id: UUID,
    actor_id: UUID,
    tenant_id: UUID,
    notes: str | None,
    conn: asyncpg.Connection,
) -> ActResult:
    """
    Apply the recommendation's `proposed_change` and archive the Model.

    Caller owns the transaction. On any error inside, the caller's
    transaction must roll back so neither the Act-layer change nor
    the recommendation archive lands.
    """
    rec = await _load_active_recommendation(
        recommendation_id=recommendation_id,
        tenant_id=tenant_id,
        conn=conn,
    )

    proposition = rec["proposition"]
    target_ref = proposition.get("target_act_ref") or {}
    proposed_change = proposition.get("proposed_change") or {}
    op = proposed_change.get("operation")
    payload = proposed_change.get("payload") or {}
    ref_type = target_ref.get("type")
    ref_id_raw = target_ref.get("id")
    if op not in ("create", "update", "archive", "transition"):
        raise ValidationError(
            f"recommendation has unknown proposed_change.operation {op!r}",
            field="proposed_change.operation",
        )
    if ref_type not in _REF_TYPE_TO_TABLE:
        raise ValidationError(
            f"recommendation has unknown target_act_ref.type {ref_type!r}",
            field="target_act_ref.type",
        )

    # Born_from_event used for cause linkage on the resulting Act change.
    cause_event_id = rec["born_from_event_id"]

    change_kind, change_id = await _apply_proposed_change(
        ref_type=ref_type,
        ref_id_raw=ref_id_raw,
        operation=op,
        payload=payload,
        tenant_id=tenant_id,
        cause_event_id=cause_event_id,
        conn=conn,
    )

    # Archive the recommendation, capturing the resulting Act change id
    # and any user notes for audit traceability.
    archive_metadata: dict[str, Any] = {
        "actor_id": str(actor_id),
        "target_act_change_kind": change_kind,
        "target_act_change_id": str(change_id),
    }
    if notes is not None and notes.strip():
        archive_metadata["notes"] = notes.strip()

    await conn.execute(
        """
        UPDATE models
        SET status              = 'archived',
            archived_at         = $2,
            archive_reason      = 'acted_upon',
            caused_act_change_id = $3
        WHERE id = $1
        """,
        recommendation_id,
        datetime.now(timezone.utc),
        change_id,
    )

    await emit_state_change(
        conn,
        kind="recommendation_acted_upon",
        entity_id=recommendation_id,
        tenant_id=tenant_id,
        cause_event_id=cause_event_id,
        actor_id=actor_id,
        entity_kind="model",
        metadata=archive_metadata,
    )

    # Demo SSE: tell any open action-list streams the card should
    # disappear. Cheap fan-out — no-op when no subscribers are
    # connected (which is the production case).
    from services.demo.sse import publish_recommendation_event

    target_actor = rec.get("target_actor_id") or actor_id
    await publish_recommendation_event(
        tenant_id=tenant_id,
        actor_id=target_actor,
        event="archived",
        recommendation_id=recommendation_id,
        summary={"reason": "acted_upon",
                 "target_act_change_id": str(change_id)},
    )

    return ActResult(
        recommendation_id=recommendation_id,
        target_act_change_kind=change_kind,
        target_act_change_id=change_id,
        archived_recommendation_proposition=proposition,
        archived_recommendation_natural=rec["natural"],
    )


# ---------------------------------------------------------------------
# Dismiss — archive without applying any change
# ---------------------------------------------------------------------


async def dismiss_recommendation(
    *,
    recommendation_id: UUID,
    actor_id: UUID,
    tenant_id: UUID,
    reason: str,
    conn: asyncpg.Connection,
) -> DismissResult:
    if not reason or not reason.strip():
        raise ValidationError("dismiss reason is required", field="reason")

    rec = await _load_active_recommendation(
        recommendation_id=recommendation_id,
        tenant_id=tenant_id,
        conn=conn,
    )

    await conn.execute(
        """
        UPDATE models
        SET status         = 'archived',
            archived_at    = $2,
            archive_reason = 'dismissed_by_user'
        WHERE id = $1
        """,
        recommendation_id,
        datetime.now(timezone.utc),
    )

    await emit_state_change(
        conn,
        kind="recommendation_dismissed",
        entity_id=recommendation_id,
        tenant_id=tenant_id,
        cause_event_id=rec["born_from_event_id"],
        actor_id=actor_id,
        entity_kind="model",
        metadata={
            "actor_id": str(actor_id),
            "reason": reason.strip(),
        },
    )

    from services.demo.sse import publish_recommendation_event

    target_actor = rec.get("target_actor_id") or actor_id
    await publish_recommendation_event(
        tenant_id=tenant_id,
        actor_id=target_actor,
        event="archived",
        recommendation_id=recommendation_id,
        summary={"reason": "dismissed_by_user"},
    )

    return DismissResult(
        recommendation_id=recommendation_id,
        reason=reason.strip(),
    )


# ---------------------------------------------------------------------
# Internal: dispatch proposed_change to the Acts modification services
# ---------------------------------------------------------------------


async def _apply_proposed_change(
    *,
    ref_type: str,
    ref_id_raw: Any,
    operation: str,
    payload: dict[str, Any],
    tenant_id: UUID,
    cause_event_id: UUID | None,
    conn: asyncpg.Connection,
) -> tuple[str, UUID]:
    """
    Apply the structured `proposed_change` by calling the existing
    Acts service entry points. Returns (kind_label, resulting_entity_id).

    Operation/target combinations supported by v1:
      - create on goal / commitment
      - transition on goal / commitment / decision
      - archive on decision (state machine: active|revisited -> archived)
      - update on resource (delegates to resources.repo.update_attributes)

    Anything else returns 400 via ValidationError.
    """
    if operation == "create":
        if ref_type == "goal":
            row = await goals_svc.create(
                title=_required_str(payload, "title"),
                description=payload.get("description"),
                parent_goal_id=_optional_uuid(payload.get("parent_goal_id")),
                altitude=payload.get("altitude", "operational"),
                success_criteria=payload.get("success_criteria"),
                target_date=_optional_dt(payload.get("target_date")),
                created_by_event_id=_required_event_id(cause_event_id),
                tenant_id=tenant_id,
                conn=conn,
            )
            return ("create_goal", row.id)
        if ref_type == "commitment":
            contributes_to: list[UUID] = []
            for g in payload.get("contributes_to_goal_ids") or []:
                gid = _optional_uuid(g)
                if gid is not None:
                    contributes_to.append(gid)
            contributors: list[tuple[UUID, str | None]] = []
            for c in payload.get("contributors") or []:
                if isinstance(c, dict):
                    cid = _optional_uuid(c.get("actor_id"))
                    role = c.get("role") if isinstance(c.get("role"), str) else None
                else:
                    cid = _optional_uuid(c)
                    role = None
                if cid is not None:
                    contributors.append((cid, role))
            row = await commitments_svc.create(
                title=_required_str(payload, "title"),
                description=payload.get("description"),
                initial_state=payload.get("initial_state", "proposed"),
                owner_id=_optional_uuid(payload.get("owner_id")),
                due_date=_optional_dt(payload.get("due_date")),
                ambition_level=payload.get("ambition_level", "base"),
                priority=int(payload.get("priority", 5)),
                success_criteria=payload.get("success_criteria"),
                contributes_to_goal_ids=contributes_to or None,
                contributors=contributors or None,
                is_maintenance=payload.get("is_maintenance"),
                created_by_event_id=_required_event_id(cause_event_id),
                tenant_id=tenant_id,
                conn=conn,
            )
            customer_resource_id = _optional_uuid(payload.get("customer_resource_id"))
            if customer_resource_id is not None:
                from services.resources import customer_commitments as cc_svc

                await cc_svc.link_commitment(
                    customer_resource_id=customer_resource_id,
                    commitment_id=row.id,
                    tenant_id=tenant_id,
                    conn=conn,
                )
            return ("create_commitment", row.id)
        raise ValidationError(
            f"create operation not supported on {ref_type}",
            field="proposed_change.operation",
        )

    # All non-create ops need a concrete target id.
    target_id = _optional_uuid(ref_id_raw)
    if target_id is None:
        raise ValidationError(
            "target_act_ref.id is required for this operation",
            field="target_act_ref.id",
        )

    if operation == "transition":
        new_state = _required_str(payload, "new_state")
        # Same-state "transition" is a reaffirm: the user is endorsing
        # the recommendation without changing the underlying Act.
        # Look up current state; if it already matches, treat as a no-op
        # so the recommendation can still be archived (acted_upon).
        if ref_type == "goal":
            cur = await conn.fetchval(
                "SELECT state FROM goals WHERE id = $1 AND tenant_id = $2",
                target_id, tenant_id,
            )
            if cur == new_state:
                return ("reaffirm_goal", target_id)
            row = await goals_svc.transition(
                target_id, new_state, cause_event_id=cause_event_id, conn=conn,
            )
            return ("transition_goal", row.id)
        if ref_type == "commitment":
            cur = await conn.fetchval(
                "SELECT state FROM commitments WHERE id = $1 AND tenant_id = $2",
                target_id, tenant_id,
            )
            if cur == new_state:
                return ("reaffirm_commitment", target_id)
            row = await commitments_svc.transition(
                target_id,
                new_state,
                resolved_by_event_ids=None,
                cause_event_id=cause_event_id,
                conn=conn,
            )
            return ("transition_commitment", row.id)
        if ref_type == "decision":
            cur = await conn.fetchval(
                "SELECT state FROM decisions WHERE id = $1 AND tenant_id = $2",
                target_id, tenant_id,
            )
            if cur == new_state:
                return ("reaffirm_decision", target_id)
            row = await decisions_svc.transition(
                target_id,
                new_state,
                cause_event_id=cause_event_id,
                conn=conn,
            )
            return ("transition_decision", row.id)
        raise ValidationError(
            f"transition operation not supported on {ref_type}",
            field="proposed_change.operation",
        )

    if operation == "archive":
        if ref_type == "decision":
            row = await decisions_svc.transition(
                target_id, "archived", cause_event_id=cause_event_id, conn=conn,
            )
            return ("archive_decision", row.id)
        raise ValidationError(
            f"archive operation not supported on {ref_type}",
            field="proposed_change.operation",
        )

    if operation == "update":
        if ref_type == "resource":
            row = await resources_repo.update_attributes(
                target_id,
                patch=payload.get("current_value"),
                metadata_patch=payload.get("metadata"),
                description=payload.get("description"),
                last_updated_by_event_id=_required_event_id(cause_event_id),
                conn=conn,
            )
            return ("update_resource", row.id)
        raise ValidationError(
            f"update operation not supported on {ref_type}",
            field="proposed_change.operation",
        )

    raise ValidationError(
        f"unknown proposed_change.operation {operation!r}",
        field="proposed_change.operation",
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _required_str(payload: dict[str, Any], field: str) -> str:
    v = payload.get(field)
    if not isinstance(v, str) or not v.strip():
        raise ValidationError(
            f"proposed_change.payload.{field} is required",
            field=f"proposed_change.payload.{field}",
        )
    return v


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


def _required_event_id(cause_event_id: UUID | None) -> UUID:
    if cause_event_id is None:
        raise ValidationError(
            "create operations require a cause_event_id "
            "(recommendation has no born_from_event_id)",
            field="cause_event_id",
        )
    return cause_event_id


def _optional_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


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
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


__all__ = [
    "ActResult",
    "DismissResult",
    "AlreadyArchivedError",
    "RecommendationStateError",
    "act_on_recommendation",
    "dismiss_recommendation",
]
