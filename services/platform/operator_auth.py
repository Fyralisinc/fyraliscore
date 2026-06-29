"""Shared authorization checks for operator tools and admin workflows."""
from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar
from uuid import UUID

import asyncpg


OperatorErrorT = TypeVar("OperatorErrorT", bound=ValueError)
DEFAULT_OPERATOR_ROLES = ("admin", "leadership")


async def require_tenant_operator(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    error_type: type[OperatorErrorT] = ValueError,
    allowed_roles: Sequence[str] = DEFAULT_OPERATOR_ROLES,
) -> None:
    """Require an actor in this tenant with a tenant-wide operator role."""
    actor_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM actors WHERE id = $1 AND tenant_id = $2)",
        actor_id,
        tenant_id,
    )
    if not actor_exists:
        raise error_type("operator_actor must be an actor in the target tenant")

    authorized = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM actor_roles
          WHERE tenant_id = $1
            AND actor_id = $2
            AND entity_type = 'tenant'
            AND entity_id IS NULL
            AND role = ANY($3::text[])
            AND revoked_at IS NULL
        )
        """,
        tenant_id,
        actor_id,
        list(allowed_roles),
    )
    if not authorized:
        roles = ", ".join(allowed_roles)
        raise error_type(f"operator_actor requires tenant role: {roles}")
