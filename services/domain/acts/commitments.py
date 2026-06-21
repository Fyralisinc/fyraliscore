"""
services/domain/acts/commitments.py — Commitment creation, transitions,
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
from dataclasses import dataclass
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

from services.domain.acts import invariants as inv
from services.domain.acts.goals import _emit_state_change
from services.domain.acts.retry import with_deadlock_retry
from services.domain.acts.state_machines import (
    COMMITMENT_TERMINAL,
    can_transition,
    is_terminal,
)


EdgeKind = Literal["contributes_to", "depends_on", "constrained_by"]


@dataclass(frozen=True)
class _CommitmentCreateInput:
    title: str
    description: str | None
    initial_state: CommitmentState
    owner_id: UUID | None
    due_date: datetime | None
    ambition_level: AmbitionLevel
    priority: int
    success_criteria: dict[str, Any] | None
    contributes_to_goal_ids: list[UUID | tuple[UUID, bool]]
    depends_on_commitment_ids: list[UUID]
    constrained_by_decision_ids: list[UUID]
    contributors: list[tuple[UUID, str | None]]
    external_counterparty_ref: dict[str, Any] | None
    estimated_capacity: dict[str, Any] | None
    is_maintenance: bool
    created_by_event_id: UUID
    last_confidence_basis: UUID | None
    tenant_id: UUID


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
    create_input = _prepare_commitment_create_input(
        title=title,
        description=description,
        initial_state=initial_state,
        owner_id=owner_id,
        due_date=due_date,
        ambition_level=ambition_level,
        priority=priority,
        success_criteria=success_criteria,
        contributes_to_goal_ids=contributes_to_goal_ids,
        depends_on_commitment_ids=depends_on_commitment_ids,
        constrained_by_decision_ids=constrained_by_decision_ids,
        contributors=contributors,
        external_counterparty_ref=external_counterparty_ref,
        estimated_capacity=estimated_capacity,
        is_maintenance=is_maintenance,
        created_by_event_id=created_by_event_id,
        last_confidence_basis=last_confidence_basis,
        tenant_id=tenant_id,
    )
    return await _run_commitment_create(create_input, conn)


def _prepare_commitment_create_input(
    *,
    title: str,
    description: str | None,
    initial_state: CommitmentState,
    owner_id: UUID | None,
    due_date: datetime | None,
    ambition_level: AmbitionLevel,
    priority: int,
    success_criteria: dict[str, Any] | None,
    contributes_to_goal_ids: list[UUID | tuple[UUID, bool]] | None,
    depends_on_commitment_ids: list[UUID] | None,
    constrained_by_decision_ids: list[UUID] | None,
    contributors: list[tuple[UUID, str | None]] | None,
    external_counterparty_ref: dict[str, Any] | None,
    estimated_capacity: dict[str, Any] | None,
    is_maintenance: bool | None,
    created_by_event_id: UUID,
    last_confidence_basis: UUID | None,
    tenant_id: UUID,
) -> _CommitmentCreateInput:
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

    return _CommitmentCreateInput(
        title=title,
        description=description,
        initial_state=initial_state,
        owner_id=owner_id,
        due_date=due_date,
        ambition_level=ambition_level,
        priority=priority,
        success_criteria=success_criteria,
        contributes_to_goal_ids=contributes_to_goal_ids,
        depends_on_commitment_ids=depends_on_commitment_ids,
        constrained_by_decision_ids=constrained_by_decision_ids,
        contributors=contributors,
        external_counterparty_ref=external_counterparty_ref,
        estimated_capacity=estimated_capacity,
        is_maintenance=maintenance,
        created_by_event_id=created_by_event_id,
        last_confidence_basis=last_confidence_basis,
        tenant_id=tenant_id,
    )


async def _run_commitment_create(
    create_input: _CommitmentCreateInput,
    conn: asyncpg.Connection | None,
) -> CommitmentRow:
    if conn is None:
        async def _run() -> CommitmentRow:
            async with transaction() as tx:
                return await _create_inner(tx, create_input)
        return await with_deadlock_retry(_run)
    return await _create_inner(conn, create_input)


async def _create_inner(
    tx: asyncpg.Connection,
    create_input: _CommitmentCreateInput,
) -> CommitmentRow:
    await _validate_commitment_create_references(tx, create_input)
    commitment_id = uuid7()
    effective_initial = await _resolve_effective_initial_state(tx, create_input)

    await _insert_commitment(tx, create_input, commitment_id, effective_initial)
    await _insert_commitment_contributors(tx, commitment_id, create_input)
    await _insert_contributes_to_edges(tx, commitment_id, effective_initial, create_input)
    await _insert_depends_on_edges(tx, commitment_id, create_input)
    await _insert_constrained_by_edges(tx, commitment_id, create_input)
    await _emit_state_change(
        tx,
        tenant_id=create_input.tenant_id,
        entity_kind="commitment",
        entity_id=commitment_id,
        from_state=None,
        to_state=effective_initial,
        cause_event_id=create_input.created_by_event_id,
    )
    row = await tx.fetchrow(
        "SELECT * FROM commitments WHERE id = $1", commitment_id
    )
    return _commitment_row_from_record(row)


async def _validate_commitment_create_references(
    tx: asyncpg.Connection,
    create_input: _CommitmentCreateInput,
) -> None:
    if create_input.owner_id is not None:
        await _require_active_actor(tx, create_input.owner_id, role="owner")
    for actor_id, _role in create_input.contributors:
        await _require_active_actor(tx, actor_id, role="contributor")
    for item in create_input.contributes_to_goal_ids:
        goal_id = item if isinstance(item, UUID) else item[0]
        await _require_tenant_goal(tx, goal_id, create_input.tenant_id)
    for dep_id in create_input.depends_on_commitment_ids:
        await _require_tenant_commitment(tx, dep_id, create_input.tenant_id)
    for dec_id in create_input.constrained_by_decision_ids:
        await _require_tenant_decision(tx, dec_id, create_input.tenant_id)


async def _resolve_effective_initial_state(
    tx: asyncpg.Connection,
    create_input: _CommitmentCreateInput,
) -> CommitmentState:
    if (
        create_input.initial_state != "active"
        or not create_input.depends_on_commitment_ids
    ):
        return create_input.initial_state

    for dep_id in create_input.depends_on_commitment_ids:
        if await inv.is_unsatisfied_dependency(tx, dep_id):
            return "blocked"
    return create_input.initial_state


async def _insert_commitment(
    tx: asyncpg.Connection,
    create_input: _CommitmentCreateInput,
    commitment_id: UUID,
    effective_initial: CommitmentState,
) -> None:
    sc_json = _json_or_none(create_input.success_criteria)
    ex_json = _json_or_none(create_input.external_counterparty_ref)
    ec_json = _json_or_none(create_input.estimated_capacity)
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
        create_input.tenant_id,
        create_input.title,
        create_input.description,
        effective_initial,
        create_input.owner_id,
        create_input.due_date,
        create_input.ambition_level,
        create_input.priority,
        sc_json,
        ex_json,
        ec_json,
        create_input.is_maintenance,
        create_input.created_by_event_id,
        create_input.last_confidence_basis,
    )


async def _insert_commitment_contributors(
    tx: asyncpg.Connection,
    commitment_id: UUID,
    create_input: _CommitmentCreateInput,
) -> None:
    for actor_id, role in create_input.contributors:
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


async def _insert_contributes_to_edges(
    tx: asyncpg.Connection,
    commitment_id: UUID,
    effective_initial: CommitmentState,
    create_input: _CommitmentCreateInput,
) -> None:
    for item in create_input.contributes_to_goal_ids:
        goal_id, is_critical_path = _contributes_to_edge_parts(item)
        await _require_goal_can_accept_critical_path(
            tx,
            goal_id=goal_id,
            commitment_state=effective_initial,
            is_critical_path=is_critical_path,
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


def _contributes_to_edge_parts(
    item: UUID | tuple[UUID, bool],
) -> tuple[UUID, bool]:
    if isinstance(item, UUID):
        return item, False
    return item[0], bool(item[1])


async def _insert_depends_on_edges(
    tx: asyncpg.Connection,
    commitment_id: UUID,
    create_input: _CommitmentCreateInput,
) -> None:
    for dep_id in create_input.depends_on_commitment_ids:
        violations = await inv.check_c6_depends_on_acyclic(
            tx, commitment_id, dep_id
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
            commitment_id,
            dep_id,
        )


async def _insert_constrained_by_edges(
    tx: asyncpg.Connection,
    commitment_id: UUID,
    create_input: _CommitmentCreateInput,
) -> None:
    for dec_id in create_input.constrained_by_decision_ids:
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


def _json_or_none(value: dict[str, Any] | None) -> str | None:
    return json.dumps(value) if value is not None else None


def _commitment_row_from_record(row: asyncpg.Record | dict[str, Any]) -> CommitmentRow:
    data = dict(row)
    for key in ("success_criteria", "external_counterparty_ref", "estimated_capacity"):
        data[key] = _json_obj_or_none(data.get(key))
    return CommitmentRow.model_validate(data)


def _json_obj_or_none(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if decoded is None:
            return None
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


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


async def _require_goal_can_accept_critical_path(
    tx: asyncpg.Connection,
    *,
    goal_id: UUID,
    commitment_state: str,
    is_critical_path: bool,
) -> None:
    if not is_critical_path:
        return
    state = await tx.fetchval(
        "SELECT state FROM goals WHERE id = $1 FOR UPDATE",
        goal_id,
    )
    if state == "achieved" and commitment_state != "doneverified":
        raise InvariantViolation(
            "G4",
            "achieved goal cannot receive an unfinished critical-path commitment",
            goal_id=str(goal_id),
            commitment_state=commitment_state,
        )


async def _require_contributes_to_allowed(
    tx: asyncpg.Connection,
    *,
    commitment_id: UUID,
    goal_id: UUID,
    is_critical_path: bool,
) -> None:
    c_row = await tx.fetchrow(
        "SELECT tenant_id, state FROM commitments WHERE id = $1 FOR UPDATE",
        commitment_id,
    )
    if c_row is None:
        raise ValidationError(
            "contributes_to commitment_id does not exist",
            commitment_id=str(commitment_id),
        )
    g_row = await tx.fetchrow(
        "SELECT tenant_id, state FROM goals WHERE id = $1 FOR UPDATE",
        goal_id,
    )
    if g_row is None:
        raise ValidationError(
            "contributes_to goal_id does not exist",
            goal_id=str(goal_id),
        )
    if c_row["tenant_id"] != g_row["tenant_id"]:
        raise ValidationError(
            "contributes_to entities belong to different tenants",
            commitment_id=str(commitment_id),
            goal_id=str(goal_id),
        )
    if (
        is_critical_path
        and g_row["state"] == "achieved"
        and c_row["state"] != "doneverified"
    ):
        raise InvariantViolation(
            "G4",
            "achieved goal cannot receive an unfinished critical-path commitment",
            goal_id=str(goal_id),
            commitment_id=str(commitment_id),
            commitment_state=c_row["state"],
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

        return _commitment_row_from_record(updated)

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
            await _require_contributes_to_allowed(
                tx,
                commitment_id=commitment_id,
                goal_id=goal_id,
                is_critical_path=is_critical_path,
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
    return _commitment_row_from_record(row) if row else None


__all__ = [
    "create",
    "transition",
    "add_contributor",
    "remove_contributor",
    "add_edge",
    "get",
    "EdgeKind",
]
