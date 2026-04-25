"""services/resources/customer_commitments.py — the Bridge spine.

Wave 5-B update (Q2 resolved — migration 0014 applied, SCHEMA-LOCK.md
W5.Q2). The table now carries the superset §27 shape:

    id UUID PK, tenant_id NOT NULL, customer_resource_id, commitment_id,
    served_description, relationship_kind, revenue_at_risk_usd,
    criticality, created_at

`link_commitment` now accepts `tenant_id` (required kw-only),
`relationship_kind`, `revenue_at_risk_usd`, `criticality` in addition
to `served_description`. The idempotency key is still the composite
(customer_resource_id, commitment_id) via the named UNIQUE constraint.

`unlink` now also requires `tenant_id` to prevent cross-tenant deletion.
`commitments_for_customer` now returns `(CommitmentRow, CustomerCommitmentRow)`
tuples so callers see the linkage row (carrying revenue/criticality/kind).

All functions scope writes/reads to `tenant_id` defensively.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import (
    CommitmentRow,
    CustomerCommitmentCriticality,
    CustomerCommitmentRelationshipKind,
    CustomerCommitmentRow,
)


async def link_commitment(
    customer_resource_id: UUID,
    commitment_id: UUID,
    *,
    tenant_id: UUID,
    relationship_kind: CustomerCommitmentRelationshipKind = "delivers",
    revenue_at_risk_usd: Decimal | None = None,
    criticality: CustomerCommitmentCriticality = "medium",
    served_description: str | None = None,
    conn: asyncpg.Connection | None = None,
) -> CustomerCommitmentRow:
    """
    Upsert a (customer_resource_id, commitment_id) row. Re-linking with
    new risk/criticality/kind/description fields overwrites the old
    values. Validates that the customer_resource_id points at a
    `kind='relational'` Resource in the passed tenant and the
    commitment exists in the same tenant.
    """

    async def _do(tx: asyncpg.Connection) -> CustomerCommitmentRow:
        # Validate the customer Resource belongs to the tenant and is relational.
        r_row = await tx.fetchrow(
            "SELECT kind, tenant_id FROM resources WHERE id = $1",
            customer_resource_id,
        )
        if r_row is None:
            raise ValidationError(
                "customer_resource_id does not reference any resource",
                customer_resource_id=str(customer_resource_id),
            )
        if r_row["kind"] != "relational":
            raise ValidationError(
                "customer_commitments.customer_resource_id must point "
                "to a kind='relational' resource",
                customer_resource_id=str(customer_resource_id),
                actual_kind=r_row["kind"],
            )
        if r_row["tenant_id"] != tenant_id:
            raise ValidationError(
                "customer_resource tenant mismatch",
                customer_resource_id=str(customer_resource_id),
                expected_tenant=str(tenant_id),
                actual_tenant=str(r_row["tenant_id"]),
            )
        c_row = await tx.fetchrow(
            "SELECT tenant_id FROM commitments WHERE id = $1",
            commitment_id,
        )
        if c_row is None:
            raise ValidationError(
                "commitment does not exist",
                commitment_id=str(commitment_id),
            )
        if c_row["tenant_id"] != tenant_id:
            raise ValidationError(
                "commitment tenant mismatch",
                commitment_id=str(commitment_id),
                expected_tenant=str(tenant_id),
                actual_tenant=str(c_row["tenant_id"]),
            )
        row = await tx.fetchrow(
            """
            INSERT INTO customer_commitments (
              id, tenant_id, customer_resource_id, commitment_id,
              served_description, relationship_kind,
              revenue_at_risk_usd, criticality
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (customer_resource_id, commitment_id)
            DO UPDATE SET
              served_description = EXCLUDED.served_description,
              relationship_kind = EXCLUDED.relationship_kind,
              revenue_at_risk_usd = EXCLUDED.revenue_at_risk_usd,
              criticality = EXCLUDED.criticality
            RETURNING *
            """,
            uuid7(),
            tenant_id,
            customer_resource_id,
            commitment_id,
            served_description,
            relationship_kind,
            revenue_at_risk_usd,
            criticality,
        )
        return CustomerCommitmentRow.model_validate(dict(row))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


async def unlink(
    customer_resource_id: UUID,
    commitment_id: UUID,
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> bool:
    """Return True if a row was removed, False if none existed. Tenant-scoped."""
    q = (
        "DELETE FROM customer_commitments "
        "WHERE customer_resource_id = $1 AND commitment_id = $2 "
        "  AND tenant_id = $3"
    )
    if conn is None:
        async with transaction() as tx:
            result = await tx.execute(q, customer_resource_id, commitment_id, tenant_id)
    else:
        result = await conn.execute(q, customer_resource_id, commitment_id, tenant_id)
    return result.endswith(" 1")


async def commitments_for_customer(
    customer_resource_id: UUID,
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> list[tuple[CommitmentRow, CustomerCommitmentRow]]:
    """
    Return every Commitment linked to this Customer Resource, paired
    with the full CustomerCommitmentRow (which carries served_description,
    relationship_kind, revenue_at_risk_usd, criticality). Tenant-scoped.
    """
    q = """
        SELECT
          c.id AS c_id, c.tenant_id AS c_tenant_id, c.title, c.description,
          c.state, c.owner_id, c.due_date, c.ambition_level, c.priority,
          c.success_criteria, c.resolved_by_event_ids,
          c.external_counterparty_ref, c.estimated_capacity,
          c.created_at AS c_created_at, c.last_state_change_at,
          c.terminal_at, c.created_by_event_id, c.last_confidence_basis,
          cc.id AS cc_id, cc.tenant_id AS cc_tenant_id,
          cc.customer_resource_id, cc.commitment_id,
          cc.served_description, cc.relationship_kind,
          cc.revenue_at_risk_usd, cc.criticality,
          cc.created_at AS cc_created_at
        FROM customer_commitments cc
        JOIN commitments c ON c.id = cc.commitment_id
        WHERE cc.customer_resource_id = $1
          AND cc.tenant_id = $2
        ORDER BY c.created_at DESC
    """
    if conn is not None:
        rows = await conn.fetch(q, customer_resource_id, tenant_id)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, customer_resource_id, tenant_id)
    out: list[tuple[CommitmentRow, CustomerCommitmentRow]] = []
    for r in rows:
        cmt = CommitmentRow.model_validate(
            {
                "id": r["c_id"],
                "tenant_id": r["c_tenant_id"],
                "title": r["title"],
                "description": r["description"],
                "state": r["state"],
                "owner_id": r["owner_id"],
                "due_date": r["due_date"],
                "ambition_level": r["ambition_level"],
                "priority": r["priority"],
                "success_criteria": r["success_criteria"],
                "resolved_by_event_ids": r["resolved_by_event_ids"],
                "external_counterparty_ref": r["external_counterparty_ref"],
                "estimated_capacity": r["estimated_capacity"],
                "created_at": r["c_created_at"],
                "last_state_change_at": r["last_state_change_at"],
                "terminal_at": r["terminal_at"],
                "created_by_event_id": r["created_by_event_id"],
                "last_confidence_basis": r["last_confidence_basis"],
            }
        )
        link = CustomerCommitmentRow.model_validate(
            {
                "id": r["cc_id"],
                "tenant_id": r["cc_tenant_id"],
                "customer_resource_id": r["customer_resource_id"],
                "commitment_id": r["commitment_id"],
                "served_description": r["served_description"],
                "relationship_kind": r["relationship_kind"],
                "revenue_at_risk_usd": r["revenue_at_risk_usd"],
                "criticality": r["criticality"],
                "created_at": r["cc_created_at"],
            }
        )
        out.append((cmt, link))
    return out


async def customers_for_commitment(
    commitment_id: UUID,
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> list[UUID]:
    """Reverse lookup: which Customer Resources does this Commitment serve?"""
    q = (
        "SELECT customer_resource_id FROM customer_commitments "
        "WHERE commitment_id = $1 AND tenant_id = $2"
    )
    if conn is not None:
        rows = await conn.fetch(q, commitment_id, tenant_id)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, commitment_id, tenant_id)
    return [r["customer_resource_id"] for r in rows]


__all__ = [
    "link_commitment",
    "unlink",
    "commitments_for_customer",
    "customers_for_commitment",
]
