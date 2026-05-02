"""services/today/triage.py — generic triage handler.

Wraps services.recommendations.handlers for the four "soft" triage
actions the Today UI exposes: hold, route, snooze, dismiss. Act has its
own dedicated handler that applies the proposed_change.

Soft triage records the user's intent in archive metadata. v1 archives
these recommendations with `archive_reason='manual'` so they don't
re-surface; v1.1 will introduce a proper `held` lifecycle so the Hold
nav surface can show them again.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from services.observations.state_change import emit_state_change
from services.recommendations.handlers import (
    AlreadyArchivedError,
    DismissResult,
    _load_active_recommendation,
    dismiss_recommendation,
)


SoftAction = Literal["hold", "route", "snooze"]
TriageAction = Literal["act", "hold", "route", "snooze", "dismiss"]


class TriageError(CompanyOSError):
    default_code = "triage_error"


@dataclass
class TriageResult:
    recommendation_id: UUID
    action: TriageAction
    reason: str | None = None


_SOFT_ACTIONS = ("hold", "route", "snooze")


async def triage_recommendation(
    *,
    recommendation_id: UUID,
    actor_id: UUID,
    tenant_id: UUID,
    action: TriageAction,
    reason: str | None,
    routed_to: str | None,
    snooze_until: datetime | None,
    conn: asyncpg.Connection,
) -> TriageResult:
    """Apply a soft triage. Returns the action that was applied.

    `act` is delegated to act_on_recommendation by the route handler.
    `dismiss` is delegated to dismiss_recommendation.
    `hold`/`route`/`snooze` archive with `archive_reason='manual'` and
    record the actor's intent in the audit-trail Observation.
    """
    if action == "act":
        raise TriageError(
            "act is handled by the dedicated /act endpoint",
            field="action",
        )
    if action == "dismiss":
        if not reason or not reason.strip():
            raise ValidationError(
                "dismiss reason is required", field="reason",
            )
        await dismiss_recommendation(
            recommendation_id=recommendation_id,
            actor_id=actor_id,
            tenant_id=tenant_id,
            reason=reason,
            conn=conn,
        )
        return TriageResult(
            recommendation_id=recommendation_id,
            action="dismiss",
            reason=reason.strip(),
        )

    if action not in _SOFT_ACTIONS:
        raise ValidationError(
            f"unknown triage action {action!r}", field="action",
        )

    rec = await _load_active_recommendation(
        recommendation_id=recommendation_id,
        tenant_id=tenant_id,
        conn=conn,
    )

    metadata: dict[str, str] = {
        "actor_id": str(actor_id),
        "triage_action": action,
    }
    if reason and reason.strip():
        metadata["reason"] = reason.strip()
    if action == "route" and routed_to and routed_to.strip():
        metadata["routed_to"] = routed_to.strip()
    if action == "snooze" and snooze_until is not None:
        metadata["snooze_until"] = snooze_until.isoformat()

    await conn.execute(
        """
        UPDATE models
        SET status         = 'archived',
            archived_at    = $2,
            archive_reason = 'manual'
        WHERE id = $1
        """,
        recommendation_id,
        datetime.now(timezone.utc),
    )

    await emit_state_change(
        conn,
        kind=f"recommendation_{action}",
        entity_id=recommendation_id,
        tenant_id=tenant_id,
        cause_event_id=rec["born_from_event_id"],
        actor_id=actor_id,
        entity_kind="model",
        metadata=metadata,
    )

    return TriageResult(
        recommendation_id=recommendation_id,
        action=action,
        reason=reason.strip() if reason and reason.strip() else None,
    )


__all__ = [
    "TriageAction",
    "TriageError",
    "TriageResult",
    "triage_recommendation",
    "AlreadyArchivedError",
    "DismissResult",
]
