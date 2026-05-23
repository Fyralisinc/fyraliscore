"""services/resources/transactions.py — `resource_transactions` writer.

BUILD-PLAN.md §3 Prompt 2.C item 2:
    record_transaction(resource_id, kind, delta, occurred_at,
    source_event_id) atomically:
      1. INSERT resource_transactions (uuid7 id, caller's occurred_at).
         Ensure partition covering occurred_at exists.
      2. UPDATE resources.current_value via apply_delta(...) per §4
         pseudo-code — six kinds, six delta shapes.
      3. Recompute utilization_state for `capacity` resources.
      4. UPDATE resources.last_updated_*.
      5. Emit state_change via `emit_state_change`.

Serialization:
  Concurrent deploys race on the same Capacity Resource's
  `available_units`. We take `SELECT ... FOR UPDATE` on the
  `resources` row before computing the new value — the row lock
  serializes concurrent transactors. Under `InvariantViolation('R1',
  ...)` for insufficient capacity the transaction rolls back and no
  partial state lands. This is the documented policy (see BUILD-LOG
  deviation (c)).

Spec §4 — `strengthen` / `weaken` / `expire` / `spend` / `acquire` /
`release` / `deploy`:
  - acquire +: `capacity.available_units += delta.units`;
    `financial.amount_cents += delta.amount_cents`;
    otherwise metadata merge.
  - deploy -: `capacity.deployed_units += delta.deployed_units;
    available_units -= delta.deployed_units`.
  - release +: capacity inverse of deploy.
  - spend: financial: `amount_cents -= delta.amount_cents` (or +
    when delta.amount_cents is negative per the spec's `+= delta`
    convention; we follow the `amount_cents` sign the caller passes).
  - strengthen/weaken (relational): shift `strength` along
    `['at_risk','weakening','moderate','strong']` by `strength_delta`;
    or adjust `arr_cents` by `arr_delta_cents`.
  - expire: marks time-limited resource as expired (utilization_state
    change); ip/infrastructure/regulatory merges delta into metadata.
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
from lib.shared.types import (
    ResourceTransactionRow,
    ResourceTransactionType,
)
from services.observations.state_change import emit_state_change
from services.resources.partitions import ensure_partition_for


VALID_TRANSACTION_TYPES: tuple[str, ...] = (
    "acquire", "deploy", "release", "spend", "strengthen", "weaken", "expire",
)

STRENGTH_LADDER: list[str] = ["at_risk", "weakening", "moderate", "strong"]


# =====================================================================
# apply_delta
# =====================================================================

def apply_delta(
    current_value: dict[str, Any],
    delta: dict[str, Any],
    kind: str,
    tx_type: str,
) -> dict[str, Any]:
    """
    Pure function: given current JSONB value, delta shape, and
    (kind, tx_type), compute the updated JSONB. Never mutates inputs.
    """
    cv = dict(current_value or {})

    if kind == "financial":
        amt = delta.get("amount_cents", 0)
        if tx_type == "spend":
            # spend always decreases regardless of sign; treat amount_cents
            # as magnitude if positive. Callers passing negative are
            # treated as the spec's += convention.
            amt = -abs(amt) if amt != 0 else 0
        elif tx_type == "release" or tx_type == "acquire":
            amt = abs(amt) if amt != 0 else 0
        # strengthen/weaken/expire on financial: metadata merge only (no amount touch).
        if tx_type in ("strengthen", "weaken", "expire"):
            # ignore amount_cents; treat delta as a metadata merge
            cv.update({k: v for k, v in delta.items() if k != "amount_cents"})
            return cv
        cv["amount_cents"] = int(cv.get("amount_cents", 0)) + int(amt)
        # Copy non-numeric scalars like currency/account if present.
        for k, v in delta.items():
            if k != "amount_cents":
                cv[k] = v
        return cv

    if kind == "capacity":
        units = int(delta.get("deployed_units", delta.get("units", 0)) or 0)
        deployed = int(cv.get("deployed_units", 0))
        available = int(cv.get("available_units", 0))
        if tx_type == "deploy":
            cv["deployed_units"] = deployed + units
            cv["available_units"] = available - units
        elif tx_type == "release":
            # Inverse of deploy.
            cv["deployed_units"] = max(0, deployed - units)
            cv["available_units"] = available + units
        elif tx_type == "acquire":
            # Grow both total and available.
            cv["total_units"] = int(cv.get("total_units", 0)) + units
            cv["available_units"] = available + units
        elif tx_type == "spend":
            # Consume permanently: reduce total + available.
            cv["total_units"] = max(0, int(cv.get("total_units", 0)) - units)
            cv["available_units"] = max(0, available - units)
        elif tx_type == "expire":
            # Zero out available; total stays for audit.
            cv["available_units"] = 0
        elif tx_type in ("strengthen", "weaken"):
            # Not meaningful for capacity; treat as metadata merge.
            cv.update({k: v for k, v in delta.items() if k not in ("units", "deployed_units")})
        return cv

    if kind == "relational":
        # strength_delta shifts along the ladder.
        sd = delta.get("strength_delta")
        if sd is not None:
            current = STRENGTH_LADDER.index(cv.get("strength", "moderate"))
            new = max(0, min(len(STRENGTH_LADDER) - 1, current + int(sd)))
            cv["strength"] = STRENGTH_LADDER[new]
        # strengthen/weaken without explicit delta assume +1 / -1.
        elif tx_type == "strengthen":
            current = STRENGTH_LADDER.index(cv.get("strength", "moderate"))
            cv["strength"] = STRENGTH_LADDER[min(len(STRENGTH_LADDER) - 1, current + 1)]
        elif tx_type == "weaken":
            current = STRENGTH_LADDER.index(cv.get("strength", "moderate"))
            cv["strength"] = STRENGTH_LADDER[max(0, current - 1)]
        # arr_delta_cents adjusts ARR.
        arr_d = delta.get("arr_delta_cents")
        if arr_d is not None:
            cv["arr_cents"] = int(cv.get("arr_cents", 0)) + int(arr_d)
        # Pass through other keys (contract_state, renewal_date, etc.).
        for k, v in delta.items():
            if k in ("strength_delta", "arr_delta_cents"):
                continue
            cv[k] = v
        return cv

    # ip / infrastructure / regulatory: metadata merges, with `expire`
    # flipping an `expired` flag.
    cv.update(delta)
    if tx_type == "expire":
        cv["expired"] = True
    return cv


def compute_utilization(
    current_value: dict[str, Any],
    kind: str,
    current_state: str,
) -> str:
    """
    Per spec §4: `capacity` resources derive utilization_state from
    deployed vs total. Other kinds keep caller-supplied / existing
    utilization_state unless the tx is an explicit `expire`.
    """
    if kind == "capacity":
        deployed = int(current_value.get("deployed_units", 0))
        total = int(current_value.get("total_units", 0))
        if total <= 0:
            # If there's no capacity pool at all, treat as depleted if
            # anything is deployed, else available.
            return "depleted" if deployed > 0 else "available"
        if deployed >= total:
            return "depleted"
        if deployed > 0:
            return "deployed"
        return "available"
    return current_state


# =====================================================================
# record_transaction
# =====================================================================

async def record_transaction(
    resource_id: UUID,
    *,
    kind: ResourceTransactionType,
    delta: dict[str, Any],
    occurred_at: datetime,
    source_event_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> ResourceTransactionRow:
    """
    Atomic single-writer. Caller supplies the occurred_at so historical
    replays stay in the correct partition.

    Steps (all inside one tx):
      1. Ensure the partition exists (idempotent DDL).
      2. SELECT ... FOR UPDATE the resource row (serializes concurrent
         writers; documented as the concurrency policy — BUILD-LOG
         deviation (c)).
      3. Compute new current_value via apply_delta.
      4. Compute new utilization_state via compute_utilization.
      5. INSERT resource_transactions.
      6. UPDATE resources.
      7. Emit state_change observation.
    """
    if kind not in VALID_TRANSACTION_TYPES:
        raise ValidationError(
            f"invalid transaction_type {kind!r}",
            transaction_type=kind,
            valid=list(VALID_TRANSACTION_TYPES),
        )
    if not isinstance(delta, dict):
        raise ValidationError(
            "delta must be a dict", field="delta", got=type(delta).__name__
        )
    if occurred_at.tzinfo is None:
        # Normalize naive -> UTC. Partition boundaries are UTC.
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)

    async def _do(tx: asyncpg.Connection) -> ResourceTransactionRow:
        await ensure_partition_for(occurred_at, pool_or_conn=tx)

        row = await tx.fetchrow(
            "SELECT * FROM resources WHERE id = $1 FOR UPDATE",
            resource_id,
        )
        if row is None:
            raise ValidationError(
                "resource not found", resource_id=str(resource_id)
            )
        if row["archived_at"] is not None:
            raise InvariantViolation(
                "R4",
                "cannot transact against archived resource",
                resource_id=str(resource_id),
            )

        current_cv = dict(row["current_value"] or {})
        new_cv = apply_delta(current_cv, delta, row["kind"], kind)

        # R1 post-apply: capacity can't go negative on available_units
        # (except release which restores; the delta path can only push
        # available negative on `deploy` or `spend`).
        if row["kind"] == "capacity" and new_cv.get("available_units", 0) < 0:
            raise InvariantViolation(
                "R1",
                "insufficient capacity: available_units would go negative",
                resource_id=str(resource_id),
                requested=int(delta.get("deployed_units", delta.get("units", 0)) or 0),
                available=int(current_cv.get("available_units", 0)),
            )

        new_util = compute_utilization(
            new_cv, row["kind"], row["utilization_state"]
        )
        if kind == "expire":
            new_util = "expired"

        tx_id = uuid7()
        await tx.execute(
            """
            INSERT INTO resource_transactions (
              id, resource_id, tenant_id, transaction_type,
              delta, occurred_at, source_event_id
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
            """,
            tx_id,
            resource_id,
            row["tenant_id"],
            kind,
            json.dumps(delta, default=str),
            occurred_at,
            source_event_id,
        )
        await tx.execute(
            """
            UPDATE resources
            SET current_value = $2::jsonb,
                utilization_state = $3,
                last_updated_at = now(),
                last_updated_by_event_id = $4
            WHERE id = $1
            """,
            resource_id,
            json.dumps(new_cv, default=str),
            new_util,
            source_event_id,
        )
        await emit_state_change(
            tx,
            kind=f"resource_{kind}",
            entity_id=resource_id,
            tenant_id=row["tenant_id"],
            cause_event_id=source_event_id,
            entity_kind="resource",
            metadata={
                "transaction_id": str(tx_id),
                "transaction_type": kind,
                "resource_kind": row["kind"],
                "new_utilization_state": new_util,
            },
        )
        tx_row = await tx.fetchrow(
            "SELECT * FROM resource_transactions WHERE id = $1 AND occurred_at = $2",
            tx_id,
            occurred_at,
        )
        return ResourceTransactionRow.model_validate(dict(tx_row))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


async def list_transactions(
    resource_id: UUID,
    *,
    limit: int = 50,
    conn: asyncpg.Connection | None = None,
) -> list[ResourceTransactionRow]:
    q = (
        "SELECT * FROM resource_transactions "
        "WHERE resource_id = $1 "
        "ORDER BY occurred_at DESC LIMIT $2"
    )
    if conn is not None:
        rows = await conn.fetch(q, resource_id, int(limit))
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, resource_id, int(limit))
    return [ResourceTransactionRow.model_validate(dict(r)) for r in rows]


__all__ = [
    "VALID_TRANSACTION_TYPES",
    "STRENGTH_LADDER",
    "apply_delta",
    "compute_utilization",
    "record_transaction",
    "list_transactions",
]
