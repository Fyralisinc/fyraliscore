"""
services/access_control/roles.py — role grants and lookups.

Spec ref: ARCHITECTURE-FINAL.md §26 + BUILD-PLAN §6 Prompt 5.A.

Table schema: `actor_roles` per migration 0014. A role grant is the
tuple (tenant_id, actor_id, entity_type, entity_id, role). `entity_id`
is NULL when `entity_type='tenant'` (admin / leadership / finance /
legal are tenant-scoped; owner / contributor / viewer are entity-
scoped).

Functions:
  - grant_role(actor_id, entity_type, entity_id, role, granted_by,
               *, conn, tenant_id) — idempotent on the dedup constraint.
  - revoke_role(actor_id, entity_type, entity_id, role,
               *, conn, tenant_id) — marks revoked_at=now() on the
               active row. Re-grant afterwards creates a fresh row.
  - roles_for_actor(actor_id, *, conn, tenant_id) — all active grants.
  - has_role(actor_id, role, *, conn, tenant_id, entity_id=None)
      - entity_id=None → tenant-scoped check.
      - entity_id=<uuid> → entity-scoped check for that specific entity.
      - tenant-wide admin/leadership ALSO implies every per-entity
        role check when requested at tenant scope.

Every function starts with `WHERE tenant_id = $1` — tenant isolation is
absolute. The `tenant_id` kwarg is required except where clearly
inferable from context (we keep it explicit).
"""
from __future__ import annotations

from typing import Any, Iterable, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError


EntityType = Literal["goal", "commitment", "decision", "resource", "tenant"]
RoleName = Literal[
    "owner", "contributor", "viewer", "admin",
    "finance", "legal", "leadership",
]

_LEGAL_ENTITY_TYPES: frozenset[str] = frozenset(
    ("goal", "commitment", "decision", "resource", "tenant")
)
_LEGAL_ROLES: frozenset[str] = frozenset(
    (
        "owner", "contributor", "viewer", "admin",
        "finance", "legal", "leadership",
    )
)


class RoleGrantError(CompanyOSError):
    default_code = "role_grant_error"


def _validate(entity_type: str, role: str, entity_id: UUID | None) -> None:
    if entity_type not in _LEGAL_ENTITY_TYPES:
        raise ValidationError(
            f"invalid entity_type {entity_type!r}",
            entity_type=entity_type,
            legal=sorted(_LEGAL_ENTITY_TYPES),
        )
    if role not in _LEGAL_ROLES:
        raise ValidationError(
            f"invalid role {role!r}",
            role=role,
            legal=sorted(_LEGAL_ROLES),
        )
    if entity_type == "tenant" and entity_id is not None:
        raise ValidationError(
            "tenant-scoped role must have entity_id=None",
            entity_type=entity_type, entity_id=str(entity_id),
        )
    if entity_type != "tenant" and entity_id is None:
        raise ValidationError(
            "entity-scoped role must have a non-null entity_id",
            entity_type=entity_type,
        )


async def grant_role(
    actor_id: UUID,
    entity_type: EntityType,
    entity_id: UUID | None,
    role: RoleName,
    granted_by: UUID | None,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> None:
    """
    Idempotent grant. If the (tenant, actor, entity_type, entity_id,
    role, NULL) tuple already exists, this is a no-op. Concurrent
    callers both succeed.

    The function does NOT re-activate a previously-revoked grant
    (that would make audit trail ambiguous). A re-grant after revoke
    inserts a brand-new row; the old row remains with revoked_at set.
    """
    _validate(entity_type, role, entity_id)
    # ON CONFLICT on the dedup constraint makes the insert idempotent.
    await conn.execute(
        """
        INSERT INTO actor_roles (
            tenant_id, actor_id, entity_type, entity_id, role,
            granted_by, granted_at, revoked_at
        ) VALUES ($1, $2, $3, $4, $5, $6, now(), NULL)
        ON CONFLICT ON CONSTRAINT actor_roles_dedup
        DO NOTHING
        """,
        tenant_id, actor_id, entity_type, entity_id, role, granted_by,
    )


async def revoke_role(
    actor_id: UUID,
    entity_type: EntityType,
    entity_id: UUID | None,
    role: RoleName,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> bool:
    """
    Mark the active grant's `revoked_at = now()`. Returns True if a
    row was revoked, False otherwise (idempotent from the caller's
    perspective: double-revoke returns False).
    """
    _validate(entity_type, role, entity_id)
    if entity_id is None:
        tag = await conn.execute(
            """
            UPDATE actor_roles
            SET revoked_at = now()
            WHERE tenant_id = $1
              AND actor_id = $2
              AND entity_type = $3
              AND entity_id IS NULL
              AND role = $4
              AND revoked_at IS NULL
            """,
            tenant_id, actor_id, entity_type, role,
        )
    else:
        tag = await conn.execute(
            """
            UPDATE actor_roles
            SET revoked_at = now()
            WHERE tenant_id = $1
              AND actor_id = $2
              AND entity_type = $3
              AND entity_id = $4
              AND role = $5
              AND revoked_at IS NULL
            """,
            tenant_id, actor_id, entity_type, entity_id, role,
        )
    try:
        return int(tag.split()[-1]) > 0
    except (IndexError, ValueError):
        return False


async def roles_for_actor(
    actor_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    """
    Return every active role grant for this actor within the tenant.
    Each item: {entity_type, entity_id, role, granted_at, granted_by}.
    """
    rows = await conn.fetch(
        """
        SELECT entity_type, entity_id, role, granted_at, granted_by
        FROM actor_roles
        WHERE tenant_id = $1
          AND actor_id = $2
          AND revoked_at IS NULL
        ORDER BY granted_at ASC
        """,
        tenant_id, actor_id,
    )
    return [dict(r) for r in rows]


async def has_role(
    actor_id: UUID,
    role: RoleName,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    entity_id: UUID | None = None,
    entity_type: EntityType | None = None,
) -> bool:
    """
    Role-presence check.

    - Tenant-scoped call (entity_id=None): looks for
      entity_type='tenant' row with this role. This is how admin /
      leadership / finance / legal are checked.

    - Entity-scoped call (entity_id provided): looks for a role on that
      specific entity, OR a tenant-wide admin/leadership grant (spec
      §26 admin override). The caller must pass entity_type so we can
      search the right slice (the materialized views assume the right
      kind).

    The check always filters by tenant — cross-tenant lookups return
    False unconditionally.
    """
    if entity_id is None:
        # Tenant-scoped check.
        val = await conn.fetchval(
            """
            SELECT 1 FROM actor_roles
            WHERE tenant_id = $1
              AND actor_id = $2
              AND entity_type = 'tenant'
              AND role = $3
              AND revoked_at IS NULL
            LIMIT 1
            """,
            tenant_id, actor_id, role,
        )
        return val is not None

    # Entity-scoped: entity_type is required for an accurate search.
    # We don't silently accept mixed types — callers must tell us.
    if entity_type is None:
        raise ValidationError(
            "has_role with entity_id requires entity_type",
            actor_id=str(actor_id), entity_id=str(entity_id),
        )
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_roles ar
        WHERE ar.tenant_id = $1
          AND ar.actor_id = $2
          AND ar.revoked_at IS NULL
          AND (
            (ar.entity_type = $3 AND ar.entity_id = $4 AND ar.role = $5)
            OR (ar.entity_type = 'tenant' AND ar.role IN ('admin', 'leadership'))
          )
        LIMIT 1
        """,
        tenant_id, actor_id, entity_type, entity_id, role,
    )
    return val is not None


__all__ = [
    "EntityType",
    "RoleGrantError",
    "RoleName",
    "grant_role",
    "has_role",
    "revoke_role",
    "roles_for_actor",
]
