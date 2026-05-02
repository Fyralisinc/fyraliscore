"""
services/models/recommendations.py — async cross-field validation for
recommendation-kind Models.

The Pydantic schema in `services.models.propositions.RecommendationProposition`
enforces shape (target_act_ref/proposed_change/expected_impact/
qualitative_impact/target_actor_id are present and well-formed). Two
checks need a live DB connection and live state-machine knowledge, so
they live here:

  1. **target_act_ref existence**: the referenced Goal / Commitment /
     Decision / Resource must exist in the same tenant. Recommendations
     about deleted entities are dead-on-arrival.

  2. **proposed_change.operation == 'transition' reachability**: when a
     recommendation says "transition this Commitment to <state>", the
     Commitment state machine in `services.acts.state_machines` must
     allow `current_state -> new_state`.

Both run inside the caller's transaction (the Models repo INSERT
pipeline). Failure raises `ValidationError` and the surrounding
transaction rolls back — the recommendation never lands.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from services.acts.state_machines import can_transition


_REF_TYPE_TO_TABLE: dict[str, str] = {
    "goal": "goals",
    "commitment": "commitments",
    "decision": "decisions",
    "resource": "resources",
}


async def validate_recommendation(
    proposition: dict[str, Any],
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> None:
    """
    Validate a recommendation proposition against live DB state.

    Caller must already have validated the proposition shape via
    `services.models.propositions.validate_proposition`. This function
    only runs the cross-field, DB-backed checks.

    Raises:
      - ValidationError if the target entity does not exist in the
        tenant, or the entity is in a state that makes the proposed
        change unreachable.
    """
    ref = proposition["target_act_ref"]
    ref_type = ref["type"]
    ref_id_raw = ref["id"]
    try:
        ref_id = UUID(str(ref_id_raw))
    except (ValueError, TypeError) as e:
        raise ValidationError(
            f"target_act_ref.id is not a valid UUID: {ref_id_raw!r}",
            field="target_act_ref.id",
        ) from e

    table = _REF_TYPE_TO_TABLE[ref_type]
    row = await conn.fetchrow(
        f"SELECT id, tenant_id FROM {table} WHERE id = $1",
        ref_id,
    )
    if row is None:
        raise ValidationError(
            f"target_act_ref points at non-existent {ref_type} {ref_id}",
            field="target_act_ref",
            ref_type=ref_type,
            ref_id=str(ref_id),
        )
    if row["tenant_id"] != tenant_id:
        raise ValidationError(
            f"target_act_ref points at {ref_type} {ref_id} "
            f"in a different tenant",
            field="target_act_ref",
        )

    change = proposition["proposed_change"]
    op = change["operation"]
    if op != "transition":
        return

    payload = change.get("payload") or {}
    new_state = payload.get("new_state")
    if not isinstance(new_state, str):
        raise ValidationError(
            "proposed_change.payload.new_state must be a string for "
            "operation='transition'",
            field="proposed_change.payload.new_state",
        )

    # State-machine reachability. Goals / Commitments / Decisions all
    # carry a `state` column; Resources don't have a state machine, so
    # transition operations on Resources are illegal.
    if ref_type == "resource":
        raise ValidationError(
            "Resources do not have a state machine; "
            "proposed_change.operation='transition' is not valid",
            field="proposed_change.operation",
        )

    state_row = await conn.fetchrow(
        f"SELECT state FROM {table} WHERE id = $1",
        ref_id,
    )
    if state_row is None:
        # Belt-and-braces; we already checked existence above.
        raise ValidationError(
            f"target {ref_type} {ref_id} disappeared during validation",
            field="target_act_ref",
        )
    current_state = state_row["state"]

    ok, reason = can_transition(current_state, new_state, ref_type)  # type: ignore[arg-type]
    if not ok:
        raise ValidationError(
            f"recommendation proposes unreachable transition: {reason}",
            field="proposed_change.payload.new_state",
            current_state=current_state,
            new_state=new_state,
            ref_type=ref_type,
            ref_id=str(ref_id),
        )


__all__ = ["validate_recommendation"]
