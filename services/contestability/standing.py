"""
services/contestability/standing.py — standing-on-Model checks.

Spec §11 `actor_has_standing_on_model` has three clauses:

  1. Scope-based — actor is in `model.scope_actors`.
  2. Owner-based — actor owns (or contributes to) a commitment
     referenced in `model.scope_entities` (entries where
     `type == 'commitment'`).
  3. Role-based (manager chain) — actor manages any actor in
     `model.scope_actors`. Wave 4 stub: always returns False.
     Wave 5-A wires real org-chart lookups.

Clauses 1+2 fully implemented; clause 3 returns False for Wave 4.

Public surface
--------------
`actor_has_standing_on_model(conn, *, actor_id, model_id) -> StandingResult`

StandingResult is an immutable dataclass exposing:
  * `granted: bool`
  * `basis: Literal['scope', 'owner', 'contributor', 'manager_chain'] | None`

This keeps the HTTP layer's 403 path correct AND records which clause
granted standing for the audit log.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import asyncpg


StandingBasis = Literal["scope", "owner", "contributor", "manager_chain"]


@dataclass(frozen=True)
class StandingResult:
    granted: bool
    basis: StandingBasis | None


async def actor_has_standing_on_model(
    conn: asyncpg.Connection,
    *,
    actor_id: UUID,
    model_id: UUID,
) -> StandingResult:
    """
    Compute standing for (actor_id, model_id).

    Single SELECT against `models` + targeted lookups against
    `commitments` / `commitment_contributors`. No Pydantic hydration —
    we only need `scope_actors` and `scope_entities`.
    """
    row = await conn.fetchrow(
        """
        SELECT scope_actors, scope_entities, tenant_id
        FROM models
        WHERE id = $1
        """,
        model_id,
    )
    if row is None:
        return StandingResult(granted=False, basis=None)

    scope_actors: list[UUID] = list(row["scope_actors"] or [])
    if actor_id in scope_actors:
        return StandingResult(granted=True, basis="scope")

    entities = row["scope_entities"]
    if isinstance(entities, (bytes, bytearray)):
        import json
        entities = json.loads(entities.decode())
    elif isinstance(entities, str):
        import json
        entities = json.loads(entities)
    if not isinstance(entities, list):
        entities = []

    for ent in entities:
        if not isinstance(ent, dict):
            continue
        if ent.get("type") != "commitment":
            continue
        cid = ent.get("id")
        if cid is None:
            continue
        try:
            commit_id = UUID(str(cid))
        except (ValueError, TypeError):
            continue
        c = await conn.fetchrow(
            "SELECT owner_id FROM commitments WHERE id = $1", commit_id
        )
        if c is None:
            continue
        if c["owner_id"] == actor_id:
            return StandingResult(granted=True, basis="owner")
        # Contributors
        contrib = await conn.fetchval(
            """
            SELECT 1 FROM commitment_contributors
            WHERE commitment_id = $1 AND actor_id = $2
            """,
            commit_id, actor_id,
        )
        if contrib:
            return StandingResult(granted=True, basis="contributor")

    # Manager chain — Wave 5-A real lookup. For every actor in the
    # Model's scope_actors, walk the metadata.manager_id chain and
    # check if the contestor is an ancestor.
    from services.access_control.hierarchy import (
        is_in_manager_chain as _is_mgr,
    )

    tenant_id = row["tenant_id"]
    for scoped_actor in scope_actors:
        if not isinstance(scoped_actor, UUID):
            try:
                scoped_actor = UUID(str(scoped_actor))
            except (ValueError, TypeError):
                continue
        if await _is_mgr(
            scoped_actor, actor_id, conn=conn, tenant_id=tenant_id,
        ):
            return StandingResult(granted=True, basis="manager_chain")
    return StandingResult(granted=False, basis=None)


__all__ = ["StandingResult", "StandingBasis", "actor_has_standing_on_model"]
