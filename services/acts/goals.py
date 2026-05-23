"""
services/acts/goals.py — Goal creation, transitions, acyclicity, and
health recomputation (direct-children only for Wave 1; full cascade
is Wave 3-B).

See ARCHITECTURE-FINAL.md §3.1 and SCHEMA-LOCK.md S3.1 / S3.2.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import GoalAltitude, GoalRow, GoalState

from services.acts import invariants as inv
from services.acts.retry import with_deadlock_retry
from services.acts.state_machines import can_transition


async def _emit_state_change(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    entity_kind: str,
    entity_id: UUID,
    from_state: str | None,
    to_state: str,
    cause_event_id: UUID | None,
    trust_tier: str = "authoritative",
) -> UUID:
    """
    Insert a state_change observation row directly.

    Deviation documented in BUILD-LOG: services/observations/state_change.py
    (Agent 1-A's helper) does not exist yet, so we inline the INSERT.
    Shape mirrors what Agent 1-A will expose — swapping to the helper
    later is a mechanical import change.
    """
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    content = {
        "entity_kind": entity_kind,
        "entity_id": str(entity_id),
        "from_state": from_state,
        "to_state": to_state,
    }
    content_text = (
        f"{entity_kind} {entity_id} state changed "
        f"{from_state!r} -> {to_state!r}"
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, ingested_at, kind, source_channel,
          content, content_text, trust_tier, cause_id, entities_mentioned
        ) VALUES (
          $1, $2, $3, $3, 'state_change', 'internal:state_change',
          $4::jsonb, $5, $6, $7,
          $8::jsonb
        )
        """,
        obs_id,
        tenant_id,
        now,
        json.dumps(content),
        content_text,
        trust_tier,
        cause_event_id,
        json.dumps([{"type": entity_kind, "id": str(entity_id)}]),
    )
    return obs_id


# =====================================================================
# Create
# =====================================================================

async def create(
    *,
    title: str,
    description: str | None = None,
    parent_goal_id: UUID | None = None,
    altitude: GoalAltitude = "operational",
    success_criteria: dict[str, Any] | None = None,
    target_date: datetime | None = None,
    created_by_event_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> GoalRow:
    """
    Create a new Goal in state='active' (per S3.1 default).

    - Validates parent exists in same tenant and is active (G2).
    - Initial cached_health='healthy'.
    - Returns the hydrated GoalRow.

    `conn` lets callers pass an existing transaction. If None, a new
    transaction is opened.
    """
    if not title or not title.strip():
        raise ValidationError("goal title is required", field="title")

    async def _do() -> GoalRow:
        async with transaction() as tx:
            return await _create_inner(
                tx,
                title=title,
                description=description,
                parent_goal_id=parent_goal_id,
                altitude=altitude,
                success_criteria=success_criteria,
                target_date=target_date,
                created_by_event_id=created_by_event_id,
                tenant_id=tenant_id,
            )

    if conn is None:
        return await with_deadlock_retry(_do)
    return await _create_inner(
        conn,
        title=title,
        description=description,
        parent_goal_id=parent_goal_id,
        altitude=altitude,
        success_criteria=success_criteria,
        target_date=target_date,
        created_by_event_id=created_by_event_id,
        tenant_id=tenant_id,
    )


async def _create_inner(
    tx: asyncpg.Connection,
    *,
    title: str,
    description: str | None,
    parent_goal_id: UUID | None,
    altitude: GoalAltitude,
    success_criteria: dict[str, Any] | None,
    target_date: datetime | None,
    created_by_event_id: UUID,
    tenant_id: UUID,
) -> GoalRow:
    # Validate parent.
    if parent_goal_id is not None:
        parent = await tx.fetchrow(
            "SELECT state, tenant_id FROM goals WHERE id = $1",
            parent_goal_id,
        )
        if parent is None:
            raise ValidationError(
                "parent_goal_id does not exist",
                parent_goal_id=str(parent_goal_id),
            )
        if parent["tenant_id"] != tenant_id:
            raise ValidationError(
                "parent_goal_id belongs to a different tenant",
                parent_goal_id=str(parent_goal_id),
            )
        if parent["state"] != "active":
            raise ValidationError(
                "parent goal must be in 'active' state",
                parent_goal_id=str(parent_goal_id),
                parent_state=parent["state"],
            )

    goal_id = uuid7()
    sc_json = json.dumps(success_criteria) if success_criteria is not None else None
    row = await tx.fetchrow(
        """
        INSERT INTO goals (
          id, tenant_id, title, description, state, target_date,
          parent_goal_id, altitude, success_criteria, cached_health,
          cached_health_computed_at, created_by_event_id
        ) VALUES (
          $1, $2, $3, $4, 'active', $5, $6, $7, $8::jsonb, 'healthy',
          now(), $9
        )
        RETURNING *
        """,
        goal_id,
        tenant_id,
        title,
        description,
        target_date,
        parent_goal_id,
        altitude,
        sc_json,
        created_by_event_id,
    )
    # Emit birth state_change observation.
    await _emit_state_change(
        tx,
        tenant_id=tenant_id,
        entity_kind="goal",
        entity_id=goal_id,
        from_state=None,
        to_state="active",
        cause_event_id=created_by_event_id,
    )
    return GoalRow.model_validate(dict(row))


# =====================================================================
# Acyclicity
# =====================================================================

async def validate_acyclic(
    goal_id: UUID,
    parent_goal_id: UUID | None,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    """
    Pre-INSERT / pre-UPDATE G2 guard. Raises InvariantViolation("G2", ...)
    when assigning `parent_goal_id` as the parent of `goal_id` would
    create a cycle.
    """
    runner = conn
    if runner is None:
        async with transaction() as tx:
            violations = await inv.check_g2_tree_acyclic(
                tx, goal_id, parent_goal_id
            )
            if violations:
                raise violations[0]
            return
    violations = await inv.check_g2_tree_acyclic(
        runner, goal_id, parent_goal_id
    )
    if violations:
        raise violations[0]


# =====================================================================
# Cached health
# =====================================================================

async def recompute_cached_health(
    goal_id: UUID,
    tx: asyncpg.Connection,
) -> str:
    """
    G3 worst-of-critical-path. Updates the row in-place and returns
    the new value.
    """
    new_health = await inv.compute_worst_of_health(tx, goal_id)
    await tx.execute(
        """
        UPDATE goals
        SET cached_health = $2, cached_health_computed_at = now()
        WHERE id = $1
        """,
        goal_id,
        new_health,
    )
    return new_health


# =====================================================================
# Transition
# =====================================================================

async def transition(
    goal_id: UUID,
    new_state: GoalState,
    *,
    cause_event_id: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> GoalRow:
    """
    Move a Goal to `new_state`. Enforces the §3.1 state machine.

    For `achieved`: validates G4 (direct-children critical-path all
    doneverified). Full cascade / sub-goal recursion is Wave 3-B.
    """
    async def _do() -> GoalRow:
        async with transaction() as tx:
            return await _transition_inner(
                tx, goal_id, new_state, cause_event_id=cause_event_id
            )

    if conn is None:
        return await with_deadlock_retry(_do)
    return await _transition_inner(
        conn, goal_id, new_state, cause_event_id=cause_event_id
    )


async def _transition_inner(
    tx: asyncpg.Connection,
    goal_id: UUID,
    new_state: GoalState,
    *,
    cause_event_id: UUID | None,
) -> GoalRow:
    # Lock the row to prevent concurrent transitions.
    row = await tx.fetchrow(
        "SELECT * FROM goals WHERE id = $1 FOR UPDATE",
        goal_id,
    )
    if row is None:
        raise ValidationError(
            "goal not found", goal_id=str(goal_id)
        )
    current_state: str = row["state"]
    ok, reason = can_transition(current_state, new_state, "goal")
    if not ok:
        raise InvariantViolation(
            "G_STATE",
            reason,
            goal_id=str(goal_id),
            from_state=current_state,
            to_state=new_state,
        )

    # G4 check: achieved requires all critical-path doneverified
    # (direct children only for Wave 1).
    if new_state == "achieved":
        bad = await tx.fetchval(
            """
            SELECT COUNT(*) FROM contributes_to ct
            JOIN commitments c ON c.id = ct.commitment_id
            WHERE ct.goal_id = $1
              AND ct.is_critical_path = TRUE
              AND c.state <> 'doneverified'
            """,
            goal_id,
        )
        if bad and bad > 0:
            raise InvariantViolation(
                "G4",
                "achieved requires all critical-path commitments doneverified",
                goal_id=str(goal_id),
                incomplete_count=int(bad),
            )

    archived_at_update = (
        ", archived_at = now()" if new_state in ("achieved", "abandoned") else ""
    )
    updated = await tx.fetchrow(
        f"""
        UPDATE goals
        SET state = $2,
            last_state_change_at = now(){archived_at_update}
        WHERE id = $1
        RETURNING *
        """,
        goal_id,
        new_state,
    )
    await _emit_state_change(
        tx,
        tenant_id=row["tenant_id"],
        entity_kind="goal",
        entity_id=goal_id,
        from_state=current_state,
        to_state=new_state,
        cause_event_id=cause_event_id,
    )
    return GoalRow.model_validate(dict(updated))


async def get(
    goal_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> GoalRow | None:
    q = "SELECT * FROM goals WHERE id = $1"
    if conn is not None:
        row = await conn.fetchrow(q, goal_id)
    else:
        async with transaction() as tx:
            row = await tx.fetchrow(q, goal_id)
    return GoalRow.model_validate(dict(row)) if row else None


__all__ = [
    "create",
    "transition",
    "validate_acyclic",
    "recompute_cached_health",
    "get",
]
