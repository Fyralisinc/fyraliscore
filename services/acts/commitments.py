"""
services/acts/commitments.py — Commitment creation, transitions,
contributor management, and edge management (contributes_to /
depends_on / constrained_by).

Per ARCHITECTURE-FINAL.md §3.2 and SCHEMA-LOCK.md S3.3-S3.5 / S3.8-S3.11.

All writes are atomic across commitments + commitment_contributors +
contributes_to + depends_on + constrained_by. Invariants C1, C2, C5,
C6, C9, C10 are enforced at INSERT and transition time. C3, C4, C8
are enforced at transition time. C7 is enforced by DB NOT NULL.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import (
    AmbitionLevel,
    CommitmentRow,
    CommitmentState,
    CommitmentContributorRow,
    ContributesToEdge,
    DependsOnEdge,
    ConstrainedByEdge,
)

from services.acts import invariants as inv
from services.acts.goals import _emit_state_change
from services.acts.retry import with_deadlock_retry
from services.acts.state_machines import (
    COMMITMENT_TERMINAL,
    can_transition,
    is_terminal,
)


EdgeKind = Literal["contributes_to", "depends_on", "constrained_by"]


# =====================================================================
# Create
# =====================================================================

async def create(
    *,
    title: str,
    description: str | None = None,
    initial_state: CommitmentState = "proposed",
    owner_id: UUID | None = None,
    due_date: datetime | None = None,
    ambition_level: AmbitionLevel = "base",
    priority: int = 5,
    success_criteria: dict[str, Any] | None = None,
    contributes_to_goal_ids: list[UUID | tuple[UUID, bool]] | None = None,
    depends_on_commitment_ids: list[UUID] | None = None,
    constrained_by_decision_ids: list[UUID] | None = None,
    contributors: list[tuple[UUID, str | None]] | None = None,
    external_counterparty_ref: dict[str, Any] | None = None,
    estimated_capacity: dict[str, Any] | None = None,
    is_maintenance: bool | None = None,
    created_by_event_id: UUID,
    last_confidence_basis: UUID | None = None,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> CommitmentRow:
    """
    Atomically INSERT a Commitment + all its edges + its contributors.

    Invariant checks:
      - C1: owner_id required if initial_state is non-proposed.
      - C5: owner and all contributors must be active Actors.
      - C9: due_date must be > now() at creation.
      - C10: must have >=1 contributes_to OR maintenance flag
             (estimated_capacity.maintenance == true).
      - C6: each depends_on insert must not close a cycle.

    Auto-block: if initial_state='active' and any depends_on dep is
    not doneverified, the commitment lands in state 'blocked' with
    the auto-transition recorded via state_change emission.
    """
    if not title or not title.strip():
        raise ValidationError(
            "commitment title is required", field="title"
        )
    contributes_to_goal_ids = contributes_to_goal_ids or []
    depends_on_commitment_ids = depends_on_commitment_ids or []
    constrained_by_decision_ids = constrained_by_decision_ids or []
    contributors = contributors or []

    # C1 pre-check: non-proposed needs owner.
    if initial_state != "proposed" and owner_id is None:
        raise InvariantViolation(
            "C1",
            f"initial_state {initial_state!r} requires owner_id",
            initial_state=initial_state,
        )

    # C9 pre-check: creation requires future due_date.
    now = datetime.now(timezone.utc)
    if due_date is not None and due_date <= now:
        raise InvariantViolation(
            "C9",
            "due_date at creation must be in the future",
            due_date=due_date.isoformat(),
            now=now.isoformat(),
        )

    # Resolve is_maintenance. Preference order per AUDIT-REVIEW-1-FIXES I1:
    #   1. explicit `is_maintenance` keyword — new typed column (spec-canonical).
    #   2. legacy `estimated_capacity["maintenance"] is True` — older callers.
    # When both are set they must agree; disagreement is a caller bug.
    legacy_maintenance = bool(
        isinstance(estimated_capacity, dict)
        and estimated_capacity.get("maintenance") is True
    )
    if is_maintenance is None:
        maintenance = legacy_maintenance
    else:
        if legacy_maintenance and not is_maintenance:
            raise ValidationError(
                "is_maintenance=False conflicts with "
                "estimated_capacity.maintenance=True",
            )
        maintenance = bool(is_maintenance)

    # Mutual exclusion per spec C10: is_maintenance=True cannot coexist
    # with contributes_to edges.
    if maintenance and contributes_to_goal_ids:
        raise InvariantViolation(
            "C10",
            "is_maintenance=True cannot have contributes_to edges",
            n_edges=len(contributes_to_goal_ids),
        )

    # C10 pre-check: active (or any non-proposed non-terminal) requires
    # >=1 contributes_to OR maintenance flag.
    non_terminal_non_proposed = initial_state not in (
        "proposed", "doneverified", "closed"
    )
    if (
        non_terminal_non_proposed
        and not maintenance
        and not contributes_to_goal_ids
    ):
        raise InvariantViolation(
            "C10",
            "active commitment needs >=1 contributes_to or maintenance flag",
            initial_state=initial_state,
        )

    async def _do(tx: asyncpg.Connection) -> CommitmentRow:
        # C5 pre-check: owner and contributors are active actors.
        if owner_id is not None:
            await _require_active_actor(tx, owner_id, role="owner")
        for actor_id, _role in contributors:
            await _require_active_actor(
                tx, actor_id, role="contributor"
            )

        # Validate that referenced goals / dependencies / decisions exist
        # and share the tenant — FKs would catch the first but tenant
        # isolation is our responsibility.
        for item in contributes_to_goal_ids:
            goal_id = item if isinstance(item, UUID) else item[0]
            await _require_tenant_goal(tx, goal_id, tenant_id)
        for dep_id in depends_on_commitment_ids:
            await _require_tenant_commitment(tx, dep_id, tenant_id)
        for dec_id in constrained_by_decision_ids:
            await _require_tenant_decision(tx, dec_id, tenant_id)

        commitment_id = uuid7()
        # Possibly auto-block: if initial_state is 'active' and any
        # depends_on is not doneverified, start in 'blocked'.
        effective_initial = initial_state
        if initial_state == "active" and depends_on_commitment_ids:
            unsatisfied = 0
            for dep_id in depends_on_commitment_ids:
                if await inv.is_unsatisfied_dependency(tx, dep_id):
                    unsatisfied += 1
            if unsatisfied > 0:
                effective_initial = "blocked"

        sc_json = (
            json.dumps(success_criteria) if success_criteria is not None else None
        )
        ec_json = (
            json.dumps(estimated_capacity) if estimated_capacity is not None else None
        )
        ec_json = (
            json.dumps(estimated_capacity) if estimated_capacity is not None else None
        )
        ex_json = (
            json.dumps(external_counterparty_ref)
            if external_counterparty_ref is not None
            else None
        )

        await tx.execute(
            """
            INSERT INTO commitments (
              id, tenant_id, title, description, state, owner_id,
              due_date, ambition_level, priority, success_criteria,
              external_counterparty_ref, estimated_capacity,
              is_maintenance,
              created_by_event_id, last_confidence_basis
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9,
              $10::jsonb, $11::jsonb, $12::jsonb,
              $13,
              $14, $15
            )
            """,
            commitment_id,
            tenant_id,
            title,
            description,
            effective_initial,
            owner_id,
            due_date,
            ambition_level,
            priority,
            sc_json,
            ex_json,
            ec_json,
            maintenance,
            created_by_event_id,
            last_confidence_basis,
        )

        # Contributors.
        for actor_id, role in contributors:
            await tx.execute(
                """
                INSERT INTO commitment_contributors (
                  commitment_id, actor_id, role
                ) VALUES ($1, $2, $3)
                ON CONFLICT (commitment_id, actor_id) DO NOTHING
                """,
                commitment_id,
                actor_id,
                role,
            )

        # contributes_to edges.
        for item in contributes_to_goal_ids:
            if isinstance(item, UUID):
                goal_id, is_cp = item, False
            else:
                goal_id, is_cp = item[0], bool(item[1])
            await tx.execute(
                """
                INSERT INTO contributes_to (
                  commitment_id, goal_id, is_critical_path
                ) VALUES ($1, $2, $3)
                ON CONFLICT (commitment_id, goal_id) DO NOTHING
                """,
                commitment_id,
                goal_id,
                is_cp,
            )

        # depends_on edges — acyclicity check per edge.
        for dep_id in depends_on_commitment_ids:
            viol = await inv.check_c6_depends_on_acyclic(
                tx, commitment_id, dep_id
            )
            if viol:
                raise viol[0]
            await tx.execute(
                """
                INSERT INTO depends_on (
                  dependent_commitment_id, dependency_commitment_id
                ) VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                commitment_id,
                dep_id,
            )

        # constrained_by edges.
        for dec_id in constrained_by_decision_ids:
            await tx.execute(
                """
                INSERT INTO constrained_by (
                  commitment_id, decision_id
                ) VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                commitment_id,
                dec_id,
            )

        # Birth state_change.
        await _emit_state_change(
            tx,
            tenant_id=tenant_id,
            entity_kind="commitment",
            entity_id=commitment_id,
            from_state=None,
            to_state=effective_initial,
            cause_event_id=created_by_event_id,
        )
        # If auto-blocked, emit the additional active→blocked transition.
        if effective_initial != initial_state:
            # Note: we don't pre-insert active then transition; we only
            # ever store 'blocked'. The birth record above reflects the
            # final state so downstream consumers see a single event.
            pass

        row = await tx.fetchrow(
            "SELECT * FROM commitments WHERE id = $1", commitment_id
        )
        return CommitmentRow.model_validate(dict(row))

    if conn is None:
        async def _run() -> CommitmentRow:
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


async def _require_active_actor(
    tx: asyncpg.Connection, actor_id: UUID, *, role: str
) -> None:
    status = await tx.fetchval(
        "SELECT status FROM actors WHERE id = $1", actor_id
    )
    if status is None:
        raise InvariantViolation(
            "C5",
            f"{role} actor does not exist",
            actor_id=str(actor_id),
        )
    if status != "active":
        raise InvariantViolation(
            "C5",
            f"{role} actor status is {status!r}, must be 'active'",
            actor_id=str(actor_id),
            actor_status=status,
        )


async def _require_tenant_goal(
    tx: asyncpg.Connection, goal_id: UUID, tenant_id: UUID
) -> None:
    t = await tx.fetchval(
        "SELECT tenant_id FROM goals WHERE id = $1", goal_id
    )
    if t is None:
        raise ValidationError(
            "contributes_to goal_id does not exist",
            goal_id=str(goal_id),
        )
    if t != tenant_id:
        raise ValidationError(
            "contributes_to goal belongs to different tenant",
            goal_id=str(goal_id),
        )


async def _require_tenant_commitment(
    tx: asyncpg.Connection, commitment_id: UUID, tenant_id: UUID
) -> None:
    t = await tx.fetchval(
        "SELECT tenant_id FROM commitments WHERE id = $1", commitment_id
    )
    if t is None:
        raise ValidationError(
            "depends_on commitment does not exist",
            commitment_id=str(commitment_id),
        )
    if t != tenant_id:
        raise ValidationError(
            "depends_on commitment belongs to different tenant",
            commitment_id=str(commitment_id),
        )


async def _require_tenant_decision(
    tx: asyncpg.Connection, decision_id: UUID, tenant_id: UUID
) -> None:
    t = await tx.fetchval(
        "SELECT tenant_id FROM decisions WHERE id = $1", decision_id
    )
    if t is None:
        raise ValidationError(
            "constrained_by decision does not exist",
            decision_id=str(decision_id),
        )
    if t != tenant_id:
        raise ValidationError(
            "constrained_by decision belongs to different tenant",
            decision_id=str(decision_id),
        )


# =====================================================================
# Transition
# =====================================================================

async def transition(
    commitment_id: UUID,
    new_state: CommitmentState,
    *,
    resolved_by_event_ids: list[UUID] | None = None,
    last_confidence_basis: UUID | None = None,
    cause_event_id: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> CommitmentRow:
    """
    Move a Commitment to `new_state`. Enforces §3.2 state machine plus
    C1 (owner required for non-proposed targets), C2 (blocked needs
    an unsatisfied dep or a revisited constraining decision), C3
    (doneverified needs >=1 resolved_by_event_id), C4 (transition
    requires cause_event_id), C8 (terminals can't transition out —
    enforced via can_transition).

    `resolved_by_event_ids`: appended to the existing array on
    transition to 'doneverified' (or passed once). If None and target
    is doneverified, C3 will fail.
    """
    async def _do(tx: asyncpg.Connection) -> CommitmentRow:
        row = await tx.fetchrow(
            "SELECT * FROM commitments WHERE id = $1 FOR UPDATE",
            commitment_id,
        )
        if row is None:
            raise ValidationError(
                "commitment not found", commitment_id=str(commitment_id)
            )
        current_state = row["state"]

        ok, reason = can_transition(current_state, new_state, "commitment")
        if not ok:
            # Terminal-state exit attempts surface as C8.
            if current_state in COMMITMENT_TERMINAL:
                raise InvariantViolation(
                    "C8",
                    reason,
                    commitment_id=str(commitment_id),
                    from_state=current_state,
                    to_state=new_state,
                )
            raise InvariantViolation(
                "C_STATE",
                reason,
                commitment_id=str(commitment_id),
                from_state=current_state,
                to_state=new_state,
            )

        # C4: transition requires a cause_event_id.
        if cause_event_id is None:
            raise InvariantViolation(
                "C4",
                "state transition requires cause_event_id",
                commitment_id=str(commitment_id),
                from_state=current_state,
                to_state=new_state,
            )

        # C1: non-proposed target requires owner.
        owner_id = row["owner_id"]
        if new_state in (
            "active", "blocked", "paused", "doneunverified"
        ) and owner_id is None:
            raise InvariantViolation(
                "C1",
                f"transition to {new_state!r} requires owner_id",
                commitment_id=str(commitment_id),
                to_state=new_state,
            )

        # C5: owner must still be an active actor on transition.
        if owner_id is not None and new_state != "closed":
            await _require_active_actor(tx, owner_id, role="owner")

        # C3: doneverified requires resolved_by_event_ids.
        merged_resolved = list(row["resolved_by_event_ids"] or [])
        if resolved_by_event_ids:
            for eid in resolved_by_event_ids:
                if eid not in merged_resolved:
                    merged_resolved.append(eid)
        if new_state == "doneverified" and len(merged_resolved) == 0:
            raise InvariantViolation(
                "C3",
                "doneverified requires >=1 resolved_by_event_id",
                commitment_id=str(commitment_id),
            )

        # C2: blocked requires unsatisfied dep OR revisited decision.
        if new_state == "blocked":
            n_deps = await inv.count_unsatisfied_dependencies(
                tx, commitment_id
            )
            n_rev = await inv.count_revisited_constraining_decisions(
                tx, commitment_id
            )
            if n_deps == 0 and n_rev == 0:
                raise InvariantViolation(
                    "C2",
                    "blocked requires unsatisfied dependency OR revisited "
                    "constraining decision",
                    commitment_id=str(commitment_id),
                )

        # Perform update.
        terminal = is_terminal(new_state, "commitment")
        new_basis = (
            last_confidence_basis
            if last_confidence_basis is not None
            else row["last_confidence_basis"]
        )

        updated = await tx.fetchrow(
            """
            UPDATE commitments
            SET state = $2,
                last_state_change_at = now(),
                resolved_by_event_ids = $3,
                last_confidence_basis = $4,
                terminal_at = CASE WHEN $5::boolean THEN now() ELSE terminal_at END
            WHERE id = $1
            RETURNING *
            """,
            commitment_id,
            new_state,
            merged_resolved,
            new_basis,
            terminal,
        )

        # C10 re-check if we're landing in an active-family state: an
        # existing commitment moving into 'active'/'blocked'/'paused'/
        # 'doneunverified' still needs contributes_to or maintenance.
        if new_state in ("active", "blocked", "paused", "doneunverified"):
            viols = await inv._check_c10_contributes_or_maintenance(
                tx, commitment_id
            )
            if viols:
                raise viols[0]

        await _emit_state_change(
            tx,
            tenant_id=row["tenant_id"],
            entity_kind="commitment",
            entity_id=commitment_id,
            from_state=current_state,
            to_state=new_state,
            cause_event_id=cause_event_id,
        )

        return CommitmentRow.model_validate(dict(updated))

    if conn is None:
        async def _run() -> CommitmentRow:
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


# =====================================================================
# Contributors
# =====================================================================

async def add_contributor(
    commitment_id: UUID,
    actor_id: UUID,
    role: str | None = None,
    *,
    conn: asyncpg.Connection | None = None,
) -> CommitmentContributorRow:
    async def _do(tx: asyncpg.Connection) -> CommitmentContributorRow:
        # C5
        await _require_active_actor(tx, actor_id, role="contributor")
        row = await tx.fetchrow(
            """
            INSERT INTO commitment_contributors (
              commitment_id, actor_id, role
            ) VALUES ($1, $2, $3)
            ON CONFLICT (commitment_id, actor_id)
            DO UPDATE SET role = EXCLUDED.role
            RETURNING *
            """,
            commitment_id,
            actor_id,
            role,
        )
        return CommitmentContributorRow.model_validate(dict(row))

    if conn is None:
        async def _run() -> CommitmentContributorRow:
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


async def remove_contributor(
    commitment_id: UUID,
    actor_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> bool:
    q = """
        DELETE FROM commitment_contributors
        WHERE commitment_id = $1 AND actor_id = $2
        """
    if conn is None:
        async with transaction() as tx:
            result = await tx.execute(q, commitment_id, actor_id)
    else:
        result = await conn.execute(q, commitment_id, actor_id)
    # asyncpg returns 'DELETE N' — N=0 means nothing was removed.
    return result.endswith(" 1") or result.endswith(" 2")


# =====================================================================
# Edges
# =====================================================================

async def add_edge(
    kind: EdgeKind,
    /,
    *,
    commitment_id: UUID | None = None,
    goal_id: UUID | None = None,
    dependent_commitment_id: UUID | None = None,
    dependency_commitment_id: UUID | None = None,
    decision_id: UUID | None = None,
    is_critical_path: bool = False,
    conn: asyncpg.Connection | None = None,
) -> ContributesToEdge | DependsOnEdge | ConstrainedByEdge:
    """
    Add one edge of any of the three kinds. For `depends_on` inserts
    the C6 acyclicity guard runs before the row is written.

    Idempotent: ON CONFLICT DO NOTHING. When the edge already exists
    the existing row is returned.
    """
    async def _do(tx: asyncpg.Connection):
        if kind == "contributes_to":
            if commitment_id is None or goal_id is None:
                raise ValidationError(
                    "contributes_to requires commitment_id and goal_id"
                )
            await tx.execute(
                """
                INSERT INTO contributes_to (
                  commitment_id, goal_id, is_critical_path
                ) VALUES ($1, $2, $3)
                ON CONFLICT (commitment_id, goal_id) DO NOTHING
                """,
                commitment_id,
                goal_id,
                is_critical_path,
            )
            row = await tx.fetchrow(
                """
                SELECT * FROM contributes_to
                WHERE commitment_id = $1 AND goal_id = $2
                """,
                commitment_id,
                goal_id,
            )
            return ContributesToEdge.model_validate(dict(row))

        if kind == "depends_on":
            if dependent_commitment_id is None or dependency_commitment_id is None:
                raise ValidationError(
                    "depends_on requires dependent_commitment_id and "
                    "dependency_commitment_id"
                )
            # C6 — acyclicity.
            violations = await inv.check_c6_depends_on_acyclic(
                tx, dependent_commitment_id, dependency_commitment_id
            )
            if violations:
                raise violations[0]
            await tx.execute(
                """
                INSERT INTO depends_on (
                  dependent_commitment_id, dependency_commitment_id
                ) VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                dependent_commitment_id,
                dependency_commitment_id,
            )
            row = await tx.fetchrow(
                """
                SELECT * FROM depends_on
                WHERE dependent_commitment_id = $1
                  AND dependency_commitment_id = $2
                """,
                dependent_commitment_id,
                dependency_commitment_id,
            )
            return DependsOnEdge.model_validate(dict(row))

        if kind == "constrained_by":
            if commitment_id is None or decision_id is None:
                raise ValidationError(
                    "constrained_by requires commitment_id and decision_id"
                )
            await tx.execute(
                """
                INSERT INTO constrained_by (commitment_id, decision_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                commitment_id,
                decision_id,
            )
            row = await tx.fetchrow(
                """
                SELECT * FROM constrained_by
                WHERE commitment_id = $1 AND decision_id = $2
                """,
                commitment_id,
                decision_id,
            )
            return ConstrainedByEdge.model_validate(dict(row))

        raise ValidationError(f"unknown edge kind: {kind!r}")

    if conn is None:
        async def _run():
            async with transaction() as tx:
                return await _do(tx)
        return await with_deadlock_retry(_run)
    return await _do(conn)


async def get(
    commitment_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> CommitmentRow | None:
    q = "SELECT * FROM commitments WHERE id = $1"
    if conn is not None:
        row = await conn.fetchrow(q, commitment_id)
    else:
        async with transaction() as tx:
            row = await tx.fetchrow(q, commitment_id)
    return CommitmentRow.model_validate(dict(row)) if row else None


__all__ = [
    "create",
    "transition",
    "add_contributor",
    "remove_contributor",
    "add_edge",
    "get",
    "EdgeKind",
]
