"""
services/product/recommendations/handlers.py — write-side state changes for the
recommendation surface: act, dismiss, ratify.

These handlers wrap the existing Acts modification entry points
(`services.domain.acts.{goals,commitments,decisions}` + `services.domain.resources.repo`)
and the Models archive path. The intent is: a CEO clicks "Act on this"
in the action list; we apply the structured `proposed_change` exactly
once, archive the recommendation, and write an audit-trail
`state_change` Observation that ties the recommendation, the actor,
and the resulting Act-layer mutation together.

For hypothesis Models (imaginary-node pattern), `ratify_hypothesis`
dispatches to one of four actions: approve, correct, other, dismiss.
Approve/Correct/Other emit a T2 trigger so Think's deterministic
handlers do the actual state mutation (archive + insert / confidence
bump) inside the validate/reconcile/apply pipeline. Dismiss is a
direct archive — there's no new state to create, so going through
Think would be pure overhead.

All work happens inside a single asyncpg transaction owned by the
caller. Failure of the underlying Act modification rolls the whole
unit back — the recommendation stays active and the user can retry.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from services.domain.acts import commitments as commitments_svc
from services.domain.acts import decisions as decisions_svc
from services.domain.acts import goals as goals_svc
from services.domain.observations.state_change import emit_state_change
from services.domain.resources import repo as resources_repo
from services.product.recommendations.feedback import (
    bump_supporting_model_confirmations,
    record_recommendation_feedback,
)


# Ratification action vocabulary for hypothesis Models.
RatifyAction = Literal["approve", "correct", "other", "dismiss"]

# Distinct archive_reason values used by the ratification surface.
# `models.archive_reason` is unconstrained TEXT (per migration 0001), so
# we can introduce these without a migration. Downstream telemetry can
# group "hypothesis_*" reasons under the imaginary-node lineage.
ARCHIVE_REASON_HYPOTHESIS_DISMISSED = "hypothesis_dismissed_by_user"
ARCHIVE_REASON_HYPOTHESIS_APPROVED = "hypothesis_user_approved"
ARCHIVE_REASON_HYPOTHESIS_CORRECTED = "hypothesis_user_corrected"
ARCHIVE_REASON_HYPOTHESIS_OTHER = "hypothesis_user_other"


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


@dataclass
class RatifyResult:
    """Result of a hypothesis Model ratification action.

    `trigger_id` is set when the action emitted a T2 trigger for Think
    to process asynchronously (approve / correct / other). `archived`
    is True only for dismiss, which mutates state inline.
    """

    model_id: UUID
    action: RatifyAction
    trigger_id: UUID | None
    archived: bool
    captured_observation_id: UUID | None = None


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
               target_actor_id, supporting_model_ids
        FROM models
        WHERE id = $1 AND tenant_id = $2
          AND claim_role = 'recommendation'
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
        "supporting_model_ids": list(row["supporting_model_ids"] or []),
    }


async def _load_active_hypothesis(
    *,
    model_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    """Counterpart of `_load_active_recommendation` for hypothesis Models.

    Distinct function rather than parameterized so the SQL filter is
    explicit (claim_role='hypothesis' vs 'recommendation') and downstream
    callers can't accidentally cross-route a recommendation into a
    ratify flow that expects hypothesis semantics."""
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, born_from_event_id, proposition,
               "natural" AS natural, status, archived_at, archive_reason,
               target_actor_id, confidence, scope_actors, scope_entities,
               scope_temporal, supporting_model_ids
        FROM models
        WHERE id = $1 AND tenant_id = $2
          AND claim_role = 'hypothesis'
        """,
        model_id,
        tenant_id,
    )
    if row is None:
        raise ValidationError(
            f"hypothesis model {model_id} not found",
            model_id=str(model_id),
        )
    if row["archived_at"] is not None or row["status"] != "active":
        raise AlreadyArchivedError(
            f"hypothesis {model_id} already archived",
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
        "confidence": float(row["confidence"]),
        "scope_actors": list(row["scope_actors"] or []),
        "scope_entities": _coerce_jsonb_list_local(row["scope_entities"]),
        "scope_temporal": _coerce_jsonb(row["scope_temporal"]),
        "supporting_model_ids": list(row["supporting_model_ids"] or []),
    }


def _coerce_jsonb_list_local(value: Any) -> list[dict[str, Any]]:
    """Local list coercer (the module's _coerce_jsonb only handles
    dicts). Used by the hypothesis loader to surface scope_entities."""
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


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

    feedback_actor_id = rec.get("target_actor_id") or actor_id
    pattern_key = await record_recommendation_feedback(
        conn,
        tenant_id=tenant_id,
        target_actor_id=feedback_actor_id,
        proposition=proposition,
        action="acted",
        reason=notes.strip() if notes and notes.strip() else None,
    )
    confirmed_supporters = await bump_supporting_model_confirmations(
        conn,
        tenant_id=tenant_id,
        supporting_model_ids=rec.get("supporting_model_ids") or [],
    )
    archive_metadata["feedback_pattern_key"] = pattern_key
    archive_metadata["supporting_models_confirmed"] = confirmed_supporters

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

    # Announce the recommendation was acted upon so any open action-list
    # streams drop the card. Cheap fan-out via the process-local event bus —
    # no-op when nothing is subscribed (the production case).
    from lib.shared.events import publish as publish_event

    target_actor = rec.get("target_actor_id") or actor_id
    await publish_event(
        "recommendation.event",
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

    feedback_actor_id = rec.get("target_actor_id") or actor_id
    pattern_key = await record_recommendation_feedback(
        conn,
        tenant_id=tenant_id,
        target_actor_id=feedback_actor_id,
        proposition=rec["proposition"],
        action="dismissed",
        reason=reason.strip(),
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
            "feedback_pattern_key": pattern_key,
        },
    )

    from lib.shared.events import publish as publish_event

    target_actor = rec.get("target_actor_id") or actor_id
    await publish_event(
        "recommendation.event",
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
                from services.domain.resources import customer_commitments as cc_svc

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
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.max, tzinfo=timezone.utc)
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
                dt = datetime.combine(
                    date.fromisoformat(raw),
                    time.max,
                    tzinfo=timezone.utc,
                )
            else:
                dt = datetime.fromisoformat(raw)
        except (ValueError, TypeError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


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


# =====================================================================
# Hypothesis ratification — the imaginary-node Approve / Correct / Other
# / Dismiss surface.
# =====================================================================


_VALID_RATIFY_ACTIONS: tuple[RatifyAction, ...] = (
    "approve", "correct", "other", "dismiss",
)


async def ratify_hypothesis(
    *,
    model_id: UUID,
    actor_id: UUID,
    tenant_id: UUID,
    action: RatifyAction,
    explanation: str | None = None,
    correction: dict[str, Any] | None = None,
    conn: asyncpg.Connection,
) -> RatifyResult:
    """Dispatch one of four ratification actions on a hypothesis Model.

    - **approve**: emits T2:hypothesis_approved; the deterministic
      handler bumps confidence into the user-ratified band and adds a
      ratification signal_readings entry.
    - **correct**: requires a `correction` payload carrying at minimum
      a `natural` text. Emits T2:hypothesis_corrected; the handler
      archives the hypothesis and inserts a new fact-Model whose
      `was_system_hypothesis=True` provenance flag preserves the
      lineage.
    - **other**: captures the user's free-form `explanation` as an
      `actor_explanation` observation, then emits T2:hypothesis_other;
      the handler archives the hypothesis. The explanation observation
      sits in the substrate so future Think runs can extract structured
      claims from it if useful.
    - **dismiss**: direct archive — no new state, so going through
      Think would be pure overhead. Mirrors `dismiss_recommendation`.

    Caller owns the transaction. On any failure, callers must roll back
    so the hypothesis stays active and the user can retry.
    """
    if action not in _VALID_RATIFY_ACTIONS:
        raise ValidationError(
            f"unknown ratify action {action!r}; "
            f"expected one of {list(_VALID_RATIFY_ACTIONS)}",
            field="action",
        )

    hypothesis = await _load_active_hypothesis(
        model_id=model_id, tenant_id=tenant_id, conn=conn,
    )

    if action == "dismiss":
        return await _dismiss_hypothesis(
            hypothesis=hypothesis,
            actor_id=actor_id,
            tenant_id=tenant_id,
            explanation=explanation,
            conn=conn,
        )
    if action == "approve":
        return await _emit_hypothesis_ratification_trigger(
            hypothesis=hypothesis,
            actor_id=actor_id,
            tenant_id=tenant_id,
            action="approve",
            subkind="hypothesis_approved",
            payload_extras={"explanation": explanation},
            conn=conn,
        )
    if action == "correct":
        natural_raw = (
            correction.get("natural")
            if isinstance(correction, dict) else None
        )
        if (
            not isinstance(correction, dict)
            or not isinstance(natural_raw, str)
            or not natural_raw.strip()
        ):
            raise ValidationError(
                "correct action requires a `correction` payload with at "
                "least a non-empty `natural` field",
                field="correction",
            )
        # The corrected fact-Model needs an authored observation as its
        # born_from_event_id (substrate insert requires one). We capture
        # the user's correction as an authored observation here so the
        # T2 handler has a stable cause_id to point the new Model at.
        captured_obs_id = await emit_state_change(
            conn,
            kind="hypothesis_correction_authored",
            entity_id=model_id,
            tenant_id=tenant_id,
            cause_event_id=hypothesis["born_from_event_id"],
            actor_id=actor_id,
            entity_kind="model",
            metadata={
                "actor_id": str(actor_id),
                "correction_natural": correction["natural"][:1000],
            },
        )
        return await _emit_hypothesis_ratification_trigger(
            hypothesis=hypothesis,
            actor_id=actor_id,
            tenant_id=tenant_id,
            action="correct",
            subkind="hypothesis_corrected",
            payload_extras={
                "correction": {
                    "natural": str(correction["natural"])[:2000],
                    "proposition_overrides": (
                        correction.get("proposition_overrides") or {}
                    ),
                },
                "captured_observation_id": str(captured_obs_id),
            },
            captured_observation_id=captured_obs_id,
            conn=conn,
        )
    # action == "other"
    if not explanation or not explanation.strip():
        raise ValidationError(
            "other action requires a non-empty explanation",
            field="explanation",
        )
    captured_obs_id = await emit_state_change(
        conn,
        kind="hypothesis_other_explanation",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=hypothesis["born_from_event_id"],
        actor_id=actor_id,
        entity_kind="model",
        metadata={
            "actor_id": str(actor_id),
            "explanation": explanation.strip()[:2000],
        },
    )
    return await _emit_hypothesis_ratification_trigger(
        hypothesis=hypothesis,
        actor_id=actor_id,
        tenant_id=tenant_id,
        action="other",
        subkind="hypothesis_other",
        payload_extras={
            "explanation": explanation.strip()[:2000],
            "captured_observation_id": str(captured_obs_id),
        },
        captured_observation_id=captured_obs_id,
        conn=conn,
    )


async def _dismiss_hypothesis(
    *,
    hypothesis: dict[str, Any],
    actor_id: UUID,
    tenant_id: UUID,
    explanation: str | None,
    conn: asyncpg.Connection,
) -> RatifyResult:
    """Direct archive — mirrors `dismiss_recommendation` but with the
    hypothesis-specific archive_reason."""
    model_id = hypothesis["id"]
    metadata: dict[str, Any] = {"actor_id": str(actor_id)}
    if explanation and explanation.strip():
        metadata["explanation"] = explanation.strip()[:2000]
    await conn.execute(
        """
        UPDATE models
        SET status         = 'archived',
            archived_at    = $2,
            archive_reason = $3
        WHERE id = $1
        """,
        model_id,
        datetime.now(timezone.utc),
        ARCHIVE_REASON_HYPOTHESIS_DISMISSED,
    )
    await emit_state_change(
        conn,
        kind="hypothesis_dismissed",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=hypothesis["born_from_event_id"],
        actor_id=actor_id,
        entity_kind="model",
        metadata=metadata,
    )
    try:
        from lib.shared.events import publish as publish_event
        target_actor = hypothesis.get("target_actor_id") or actor_id
        await publish_event(
            "recommendation.event",
            tenant_id=tenant_id,
            actor_id=target_actor,
            event="archived",
            recommendation_id=model_id,
            summary={"reason": ARCHIVE_REASON_HYPOTHESIS_DISMISSED},
        )
    except Exception:  # pragma: no cover — SSE is best-effort
        pass
    return RatifyResult(
        model_id=model_id,
        action="dismiss",
        trigger_id=None,
        archived=True,
    )


async def _emit_hypothesis_ratification_trigger(
    *,
    hypothesis: dict[str, Any],
    actor_id: UUID,
    tenant_id: UUID,
    action: RatifyAction,
    subkind: str,
    payload_extras: dict[str, Any],
    captured_observation_id: UUID | None = None,
    conn: asyncpg.Connection,
) -> RatifyResult:
    """Enqueue a T2 trigger that the deterministic handler will pick up.

    The trigger's `model_id` is the hypothesis Model. `payload` carries
    everything the deterministic handler needs (action-specific kwargs
    + actor identity). The deterministic handler then runs the actual
    state mutation through validate → reconcile → apply.

    A `state_change` observation is emitted here so the audit chain
    captures "user ratified" even before Think processes the trigger.
    The chain: ratification observation → trigger → Think run → Model
    mutation → cascade.
    """
    model_id = hypothesis["id"]
    trig_id = uuid7()

    # Ratification audit-trail observation. The deterministic handler
    # uses this as the cause_event_id for downstream updates so the
    # chain remains intact even if the user closes the browser before
    # Think runs.
    ratification_obs_id = await emit_state_change(
        conn,
        kind=f"{subkind}_ratified",
        entity_id=model_id,
        tenant_id=tenant_id,
        cause_event_id=hypothesis["born_from_event_id"],
        actor_id=actor_id,
        entity_kind="model",
        metadata={
            "actor_id": str(actor_id),
            "subkind": subkind,
            "trigger_id": str(trig_id),
        },
    )

    payload: dict[str, Any] = {
        "trigger_id": str(trig_id),
        "actor_id": str(actor_id),
        "ratification_observation_id": str(ratification_obs_id),
        "seed_entity_ids": [{"type": "model", "id": str(model_id)}],
    }
    for k, v in payload_extras.items():
        if v is None:
            continue
        payload[k] = v

    await conn.execute(
        """
        INSERT INTO think_trigger_queue (
          id, tenant_id, trigger_kind, trigger_subkind,
          observation_id, model_id, payload
        ) VALUES ($1, $2, 'T2', $3, $4, $5, $6::jsonb)
        """,
        trig_id, tenant_id, subkind,
        ratification_obs_id, model_id,
        json.dumps(payload, default=str),
    )

    return RatifyResult(
        model_id=model_id,
        action=action,
        trigger_id=trig_id,
        archived=False,
        captured_observation_id=captured_observation_id,
    )


__all__ = [
    "ARCHIVE_REASON_HYPOTHESIS_APPROVED",
    "ARCHIVE_REASON_HYPOTHESIS_CORRECTED",
    "ARCHIVE_REASON_HYPOTHESIS_DISMISSED",
    "ARCHIVE_REASON_HYPOTHESIS_OTHER",
    "ActResult",
    "AlreadyArchivedError",
    "DismissResult",
    "RatifyAction",
    "RatifyResult",
    "RecommendationStateError",
    "act_on_recommendation",
    "dismiss_recommendation",
    "ratify_hypothesis",
]
