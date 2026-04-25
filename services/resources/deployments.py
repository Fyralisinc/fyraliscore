"""services/resources/deployments.py — resource_deployments.

BUILD-PLAN.md §3 Prompt 2.C item 3:
    deploy(resource_id, commitment_id, quantity, started_at) →
    ResourceDeploymentRow, record_transaction(...,'deploy',...),
    INSERT resource_deployments. Raise InvariantViolation(R1) if
    available_units < quantity.units for capacity resources.

    release(deployment_id_or_pair, released_at, actual_quantity=None)
    — composite PK per SCHEMA-LOCK S4.3, so identifier is
    (resource_id, commitment_id). Reverses the capacity numbers via
    record_transaction('release', ...).

    active_deployments_for(resource_id) → list WHERE released_at IS NULL.

Invariants:
  - R1: capacity sufficiency (capacity resources only). Enforced by
    `record_transaction` via apply_delta math.
  - R2: positive deploy quantity.
  - R3: released actual_quantity <= deployed_quantity when the
    deployment is capacity-tracked (we treat underrelease as the
    caller's choice; partial release ≤ deployed only).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.types import ResourceDeploymentRow
from services.resources.transactions import record_transaction


async def deploy(
    resource_id: UUID,
    commitment_id: UUID,
    *,
    quantity: dict[str, Any],
    started_at: datetime | None = None,
    source_event_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> ResourceDeploymentRow:
    """
    Atomic: record the transaction (which FOR UPDATEs the resource +
    applies the delta), then INSERT the deployments row. Rolls back
    on R1 (insufficient capacity).

    `quantity` shape for capacity: `{'units': N}`. For non-capacity
    kinds the quantity is stored verbatim on the deployments row for
    audit; no delta math is applied unless the caller passes a
    capacity-shaped delta via `quantity` (which we only forward to
    `record_transaction` for `capacity` kinds).
    """
    if not isinstance(quantity, dict):
        raise ValidationError(
            "quantity must be a dict", field="quantity"
        )
    units = quantity.get("units")
    # R2: positive deploy quantity.
    if units is not None and (not isinstance(units, (int, float)) or units <= 0):
        raise InvariantViolation(
            "R2",
            "deploy quantity.units must be > 0",
            resource_id=str(resource_id),
            commitment_id=str(commitment_id),
            units=units,
        )
    if started_at is None:
        started_at = datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    async def _do(tx: asyncpg.Connection) -> ResourceDeploymentRow:
        # Peek kind so we only pass capacity-math deltas.
        r = await tx.fetchrow(
            "SELECT kind, current_value FROM resources WHERE id = $1",
            resource_id,
        )
        if r is None:
            raise ValidationError(
                "resource not found", resource_id=str(resource_id)
            )
        kind = r["kind"]
        if kind == "capacity":
            if units is None:
                raise InvariantViolation(
                    "R2",
                    "capacity deploy requires quantity.units",
                    resource_id=str(resource_id),
                )
            # Pre-flight availability check (so we raise R1 at our level
            # rather than the generic `R1 insufficient capacity` from
            # apply_delta — same code, clearer context).
            available = int((r["current_value"] or {}).get("available_units", 0))
            if available < int(units):
                raise InvariantViolation(
                    "R1",
                    "insufficient capacity",
                    resource_id=str(resource_id),
                    available=available,
                    requested=int(units),
                )
            await record_transaction(
                resource_id,
                kind="deploy",
                delta={"deployed_units": int(units)},
                occurred_at=started_at,
                source_event_id=source_event_id,
                conn=tx,
            )
        # Insert deployments row regardless of kind (audit trail).
        # Composite PK (resource_id, commitment_id) means a second deploy
        # against the same commitment is rejected by the PK unless the
        # prior deployment was released AND we accept overwrite via
        # ON CONFLICT — we choose to raise rather than silently merge.
        existing = await tx.fetchval(
            """
            SELECT released_at FROM resource_deployments
            WHERE resource_id = $1 AND commitment_id = $2
            """,
            resource_id,
            commitment_id,
        )
        if existing is not None:
            # A prior row exists. Allow re-deploy only if prior is released.
            # We keep the semantic simple: one row per (resource, commitment)
            # — re-deploys require a fresh resource_deployments row. Per
            # schema this is impossible without an id column, so we return
            # a descriptive error.
            if existing is None:
                # prior row is active; raise
                pass
            raise InvariantViolation(
                "R5",
                "deployment for (resource_id, commitment_id) already exists",
                resource_id=str(resource_id),
                commitment_id=str(commitment_id),
            )

        row = await tx.fetchrow(
            """
            INSERT INTO resource_deployments (
              resource_id, commitment_id, deployed_quantity, deployed_at
            ) VALUES ($1, $2, $3::jsonb, $4)
            RETURNING *
            """,
            resource_id,
            commitment_id,
            json.dumps(quantity, default=str),
            started_at,
        )
        return ResourceDeploymentRow.model_validate(dict(row))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


async def release(
    deployment_key: tuple[UUID, UUID],
    *,
    released_at: datetime | None = None,
    actual_quantity: dict[str, Any] | None = None,
    source_event_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> ResourceDeploymentRow:
    """
    Mark a deployment released and reverse the capacity math via
    `record_transaction('release', ...)`.

    `deployment_key` is `(resource_id, commitment_id)` — matches the
    composite PK from SCHEMA-LOCK S4.3.

    `actual_quantity` defaults to the deployed_quantity; if provided
    it must be <= deployed_quantity on `units` for capacity resources
    (R3).
    """
    resource_id, commitment_id = deployment_key
    if released_at is None:
        released_at = datetime.now(timezone.utc)
    if released_at.tzinfo is None:
        released_at = released_at.replace(tzinfo=timezone.utc)

    async def _do(tx: asyncpg.Connection) -> ResourceDeploymentRow:
        row = await tx.fetchrow(
            """
            SELECT d.*, r.kind AS r_kind
            FROM resource_deployments d
            JOIN resources r ON r.id = d.resource_id
            WHERE d.resource_id = $1 AND d.commitment_id = $2
            FOR UPDATE OF d
            """,
            resource_id,
            commitment_id,
        )
        if row is None:
            raise ValidationError(
                "deployment not found",
                resource_id=str(resource_id),
                commitment_id=str(commitment_id),
            )
        if row["released_at"] is not None:
            # Idempotent: already released.
            return ResourceDeploymentRow.model_validate(
                {k: v for k, v in dict(row).items() if k != "r_kind"}
            )

        deployed_q = dict(row["deployed_quantity"] or {})
        if actual_quantity is None:
            release_q = deployed_q
        else:
            release_q = dict(actual_quantity)
            # R3: actual_quantity.units must be <= deployed_quantity.units.
            if (
                row["r_kind"] == "capacity"
                and "units" in release_q
                and "units" in deployed_q
                and release_q["units"] > deployed_q["units"]
            ):
                raise InvariantViolation(
                    "R3",
                    "release cannot exceed deployed quantity",
                    resource_id=str(resource_id),
                    deployed_units=deployed_q["units"],
                    release_units=release_q["units"],
                )

        if row["r_kind"] == "capacity" and "units" in release_q:
            await record_transaction(
                resource_id,
                kind="release",
                delta={"deployed_units": int(release_q["units"])},
                occurred_at=released_at,
                source_event_id=source_event_id,
                conn=tx,
            )

        updated = await tx.fetchrow(
            """
            UPDATE resource_deployments
            SET released_at = $3
            WHERE resource_id = $1 AND commitment_id = $2
            RETURNING *
            """,
            resource_id,
            commitment_id,
            released_at,
        )
        return ResourceDeploymentRow.model_validate(dict(updated))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


async def active_deployments_for(
    resource_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> list[ResourceDeploymentRow]:
    q = (
        "SELECT * FROM resource_deployments "
        "WHERE resource_id = $1 AND released_at IS NULL "
        "ORDER BY deployed_at DESC"
    )
    if conn is not None:
        rows = await conn.fetch(q, resource_id)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, resource_id)
    return [ResourceDeploymentRow.model_validate(dict(r)) for r in rows]


async def deployments_for_commitment(
    commitment_id: UUID,
    *,
    include_released: bool = False,
    conn: asyncpg.Connection | None = None,
) -> list[ResourceDeploymentRow]:
    q = "SELECT * FROM resource_deployments WHERE commitment_id = $1"
    if not include_released:
        q += " AND released_at IS NULL"
    q += " ORDER BY deployed_at DESC"
    if conn is not None:
        rows = await conn.fetch(q, commitment_id)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, commitment_id)
    return [ResourceDeploymentRow.model_validate(dict(r)) for r in rows]


__all__ = [
    "deploy",
    "release",
    "active_deployments_for",
    "deployments_for_commitment",
]
