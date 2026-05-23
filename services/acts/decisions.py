"""
services/acts/decisions.py — Decision creation and transitions.

See ARCHITECTURE-FINAL.md §3.3 and SCHEMA-LOCK.md S3.6 / S3.7.

State machine:
  drafted   → active
  active    → revisited / archived
  revisited → active / archived
  archived  (terminal)
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import DecisionRow, DecisionState

from services.acts.goals import _emit_state_change
from services.acts.retry import with_deadlock_retry
from services.acts.state_machines import can_transition


# =====================================================================
# Create
# =====================================================================

async def create(
    *,
    title: str,
    decision_text: str,
    rationale: str | None = None,
    state: DecisionState = "drafted",
    scope: dict[str, Any] | None = None,
    revisit_triggers: dict[str, Any] | None = None,
    created_by_event_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> DecisionRow:
    if not title or not title.strip():
        raise ValidationError("decision title is required", field="title")
    if not decision_text or not decision_text.strip():
        raise ValidationError(
            "decision_text is required", field="decision_text"
        )
    if state not in ("drafted", "active"):
        # You can only CREATE a Decision in drafted or active. Revisited
        # / archived must be transitioned into.
        raise ValidationError(
            f"decision cannot be created in state {state!r}",
            field="state",
        )

    async def _do(tx: asyncpg.Connection) -> DecisionRow:
        decision_id = uuid7()
        scope_json = json.dumps(scope) if scope is not None else None
        rt_json = (
            json.dumps(revisit_triggers)
            if revisit_triggers is not None
            else None
        )
        row = await tx.fetchrow(
            """
            INSERT INTO decisions (
              id, tenant_id, title, decision_text, rationale,
              state, scope, revisit_triggers, created_by_event_id
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9
            )
            RETURNING *
            """,
            decision_id,
            tenant_id,
            title,
            decision_text,
            rationale,
            state,
            scope_json,
            rt_json,
            created_by_event_id,
        )
        await _emit_state_change(
            tx,
            tenant_id=tenant_id,
            entity_kind="decision",
            entity_id=decision_id,
            from_state=None,
            to_state=state,
            cause_event_id=created_by_event_id,
        )
        return DecisionRow.model_validate(dict(row))

    if conn is None:
        async def _run() -> DecisionRow:
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


# =====================================================================
# Transition
# =====================================================================

async def transition(
    decision_id: UUID,
    new_state: DecisionState,
    *,
    cause_event_id: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> DecisionRow:
    async def _do(tx: asyncpg.Connection) -> DecisionRow:
        row = await tx.fetchrow(
            "SELECT * FROM decisions WHERE id = $1 FOR UPDATE",
            decision_id,
        )
        if row is None:
            raise ValidationError(
                "decision not found", decision_id=str(decision_id)
            )
        ok, reason = can_transition(row["state"], new_state, "decision")
        if not ok:
            raise InvariantViolation(
                "D_STATE",
                reason,
                decision_id=str(decision_id),
                from_state=row["state"],
                to_state=new_state,
            )
        archived_clause = (
            ", archived_at = now()" if new_state == "archived" else ""
        )
        updated = await tx.fetchrow(
            f"""
            UPDATE decisions
            SET state = $2, last_state_change_at = now(){archived_clause}
            WHERE id = $1
            RETURNING *
            """,
            decision_id,
            new_state,
        )
        await _emit_state_change(
            tx,
            tenant_id=row["tenant_id"],
            entity_kind="decision",
            entity_id=decision_id,
            from_state=row["state"],
            to_state=new_state,
            cause_event_id=cause_event_id,
        )
        return DecisionRow.model_validate(dict(updated))

    if conn is None:
        async def _run() -> DecisionRow:
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


async def get(
    decision_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> DecisionRow | None:
    q = "SELECT * FROM decisions WHERE id = $1"
    if conn is not None:
        row = await conn.fetchrow(q, decision_id)
    else:
        async with transaction() as tx:
            row = await tx.fetchrow(q, decision_id)
    return DecisionRow.model_validate(dict(row)) if row else None


__all__ = ["create", "transition", "get"]
