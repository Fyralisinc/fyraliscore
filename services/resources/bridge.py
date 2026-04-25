"""services/resources/bridge.py — Bridge PRIMITIVES (Wave 2-C scope).

BUILD-PLAN.md §3 Prompt 2.C item 5. Full Bridge queries are Wave 5-B.

Three primitives:

1. `revenue_at_risk_for_customer(customer_resource_id)` — for a
   Customer Resource (kind='relational'), if ANY linked Commitment is
   in `blocked` | `paused` | `doneunverified`, return the customer's
   ARR (`current_value.arr_cents`) converted to USD. Else return 0.

   Wave 2-C simplification (documented in BUILD-LOG deviation (d)):
   treat any `doneunverified` as at-risk regardless of age. Wave 5-B
   will refine this using a configurable `unresolved > N days`
   threshold.

2. `capability_at_risk(tenant_id)` — capacity resources with
   `utilization_state='depleted'` OR deployed_units/total_units > 0.95.
   Join deploying Commitments. Return [{resource, deploying_commitments,
   utilization}, ...].

3. `feasibility_check(proposed_commitment, tx)` — returns
   `{feasible, reasons, warnings}`. Checks:
     (a) owner capacity: warn if owner already owns > 5 active
         commitments (via commitment_contributors role='owner' OR
         commitments.owner_id).
     (b) required resources: iterate
         proposed_commitment.estimated_capacity.deploys list of
         (resource_id, qty.units); check available_units >= units.
     (c) constrained_by Decisions in 'revisited' state -> warning.

Per SCHEMA-QUESTION.md Q2 — §27 columns (revenue_at_risk_usd,
relationship_kind, criticality) NOT referenced. We compute from
resources.current_value.arr_cents and commitments.state live.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.types import CommitmentRow, ResourceRow


AT_RISK_COMMITMENT_STATES: frozenset[str] = frozenset(
    {"blocked", "paused", "doneunverified"}
)

OWNER_COMMITMENT_WARN_THRESHOLD = 5
CAPACITY_UTILIZATION_WARN_RATIO = 0.95


# =====================================================================
# revenue_at_risk_for_customer
# =====================================================================

async def revenue_at_risk_for_customer(
    customer_resource_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> Decimal:
    """
    Return the customer's ARR in USD (Decimal) if ANY served_by
    Commitment is in {blocked, paused, doneunverified}, else Decimal('0').
    Zero commitments → Decimal('0'). Non-existent customer → Decimal('0').
    """

    async def _do(c: asyncpg.Connection) -> Decimal:
        r = await c.fetchrow(
            """
            SELECT kind, current_value
            FROM resources
            WHERE id = $1 AND archived_at IS NULL
            """,
            customer_resource_id,
        )
        if r is None:
            return Decimal("0")
        if r["kind"] != "relational":
            return Decimal("0")
        cv = dict(r["current_value"] or {})
        arr_cents = int(cv.get("arr_cents", 0) or 0)
        if arr_cents == 0:
            return Decimal("0")

        at_risk = await c.fetchval(
            """
            SELECT 1
            FROM customer_commitments cc
            JOIN commitments cm ON cm.id = cc.commitment_id
            WHERE cc.customer_resource_id = $1
              AND cm.state = ANY($2::text[])
            LIMIT 1
            """,
            customer_resource_id,
            list(AT_RISK_COMMITMENT_STATES),
        )
        if at_risk is None:
            return Decimal("0")
        # Convert cents to USD with two-decimal precision.
        return (Decimal(arr_cents) / Decimal(100)).quantize(Decimal("0.01"))

    if conn is not None:
        return await _do(conn)
    from lib.shared.db import get_pool
    pool = get_pool()
    async with pool.acquire() as c:
        return await _do(c)


async def revenue_at_risk_all(
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> Decimal:
    """
    Bulk aggregate across every Customer Resource in a tenant. Used by
    the hot-path benchmark test and by future Wave 5-B dashboards.

    Single SQL: sum(arr_cents)/100 over relational resources that have
    AT LEAST ONE served_by Commitment in the at-risk states.
    """

    async def _do(c: asyncpg.Connection) -> Decimal:
        total_cents = await c.fetchval(
            """
            SELECT COALESCE(SUM(
              (r.current_value ->> 'arr_cents')::bigint
            ), 0)
            FROM resources r
            WHERE r.tenant_id = $1
              AND r.kind = 'relational'
              AND r.archived_at IS NULL
              AND r.current_value ? 'arr_cents'
              AND EXISTS (
                SELECT 1
                FROM customer_commitments cc
                JOIN commitments cm ON cm.id = cc.commitment_id
                WHERE cc.customer_resource_id = r.id
                  AND cm.state = ANY($2::text[])
              )
            """,
            tenant_id,
            list(AT_RISK_COMMITMENT_STATES),
        )
        return (Decimal(int(total_cents or 0)) / Decimal(100)).quantize(Decimal("0.01"))

    if conn is not None:
        return await _do(conn)
    from lib.shared.db import get_pool
    pool = get_pool()
    async with pool.acquire() as c:
        return await _do(c)


# =====================================================================
# capability_at_risk
# =====================================================================

async def capability_at_risk(
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> list[dict[str, Any]]:
    """
    Capacity Resources whose `utilization_state='depleted'` OR
    computed deployed/total > 0.95, paired with the Commitments
    currently deploying them (active deployments only).
    """

    async def _do(c: asyncpg.Connection) -> list[dict[str, Any]]:
        rows = await c.fetch(
            """
            SELECT *
            FROM resources
            WHERE tenant_id = $1
              AND kind = 'capacity'
              AND archived_at IS NULL
              AND (
                utilization_state = 'depleted'
                OR (
                  COALESCE((current_value ->> 'total_units')::float, 0) > 0
                  AND COALESCE((current_value ->> 'deployed_units')::float, 0)
                    / NULLIF((current_value ->> 'total_units')::float, 0) > $2
                )
              )
            ORDER BY created_at DESC
            """,
            tenant_id,
            CAPACITY_UTILIZATION_WARN_RATIO,
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            rd = dict(r)
            resource = ResourceRow.model_validate(rd)
            cv = resource.current_value
            total = float(cv.get("total_units", 0) or 0)
            deployed = float(cv.get("deployed_units", 0) or 0)
            util = deployed / total if total > 0 else (1.0 if deployed > 0 else 0.0)
            deploy_rows = await c.fetch(
                """
                SELECT cm.*
                FROM resource_deployments rd
                JOIN commitments cm ON cm.id = rd.commitment_id
                WHERE rd.resource_id = $1 AND rd.released_at IS NULL
                """,
                resource.id,
            )
            commitments = [
                CommitmentRow.model_validate(dict(row)) for row in deploy_rows
            ]
            out.append(
                {
                    "resource": resource,
                    "deploying_commitments": commitments,
                    "utilization": util,
                }
            )
        return out

    if conn is not None:
        return await _do(conn)
    from lib.shared.db import get_pool
    pool = get_pool()
    async with pool.acquire() as c:
        return await _do(c)


# =====================================================================
# feasibility_check
# =====================================================================

async def feasibility_check(
    proposed_commitment: dict[str, Any],
    tx: asyncpg.Connection,
    *,
    tenant_id: UUID | None = None,
) -> dict[str, Any]:
    """
    Synchronous check against current DB state. Expected shape of
    `proposed_commitment`:

        {
          'owner_id': UUID | None,
          'estimated_capacity': {
             'deploys': [{'resource_id': UUID, 'units': int}, ...]
          },
          'constrained_by_decision_ids': [UUID, ...]
        }

    Returns `{'feasible': bool, 'reasons': [...], 'warnings': [...]}`.
    A missing field is treated as "no such check applies" — the
    function never raises on a well-formed dict.
    """
    reasons: list[str] = []
    warnings: list[str] = []

    owner_id = proposed_commitment.get("owner_id")
    if owner_id is not None:
        # Warn if owner already has > threshold active commitments.
        # Active-family states from the commitment state machine.
        active_states = ("proposed", "active", "blocked", "paused", "doneunverified")
        n_owned = await tx.fetchval(
            """
            SELECT COUNT(*)
            FROM commitments
            WHERE owner_id = $1
              AND state = ANY($2::text[])
            """,
            owner_id,
            list(active_states),
        )
        n_owned = int(n_owned or 0)
        if n_owned > OWNER_COMMITMENT_WARN_THRESHOLD:
            warnings.append(
                f"owner already owns {n_owned} active commitments "
                f"(> {OWNER_COMMITMENT_WARN_THRESHOLD})"
            )

    deploys = (
        (proposed_commitment.get("estimated_capacity") or {}).get("deploys") or []
    )
    for d in deploys:
        rid = d.get("resource_id")
        units = int(d.get("units", 0) or 0)
        if rid is None:
            reasons.append("deploy entry missing resource_id")
            continue
        row = await tx.fetchrow(
            """
            SELECT kind, current_value, archived_at
            FROM resources WHERE id = $1
            """,
            rid,
        )
        if row is None:
            reasons.append(f"resource {rid} does not exist")
            continue
        if row["archived_at"] is not None:
            reasons.append(f"resource {rid} is archived")
            continue
        if row["kind"] != "capacity":
            # Non-capacity resources don't have availability math — skip.
            continue
        available = int((row["current_value"] or {}).get("available_units", 0) or 0)
        if available < units:
            reasons.append(
                f"resource {rid} has {available} available units, "
                f"need {units}"
            )

    dec_ids = proposed_commitment.get("constrained_by_decision_ids") or []
    for dec_id in dec_ids:
        state = await tx.fetchval(
            "SELECT state FROM decisions WHERE id = $1", dec_id
        )
        if state is None:
            warnings.append(f"constrained_by decision {dec_id} not found")
            continue
        if state == "revisited":
            warnings.append(
                f"decision {dec_id} is in state 'revisited' — review before proceeding"
            )

    return {
        "feasible": len(reasons) == 0,
        "reasons": reasons,
        "warnings": warnings,
    }


__all__ = [
    "AT_RISK_COMMITMENT_STATES",
    "OWNER_COMMITMENT_WARN_THRESHOLD",
    "CAPACITY_UTILIZATION_WARN_RATIO",
    "revenue_at_risk_for_customer",
    "revenue_at_risk_all",
    "capability_at_risk",
    "feasibility_check",
]
