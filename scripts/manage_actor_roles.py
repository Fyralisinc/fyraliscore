#!/usr/bin/env python3
"""Manage Fyralis actor role grants from an operator shell.

Examples:

  python scripts/manage_actor_roles.py list \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --actor 00000000-0000-0000-0000-000000000002

  python scripts/manage_actor_roles.py grant \
    --tenant 00000000-0000-0000-0000-000000000001 \
    --actor 00000000-0000-0000-0000-000000000002 \
    --entity-type tenant \
    --role admin \
    --granted-by 00000000-0000-0000-0000-000000000003
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.access_control.roles import (  # noqa: E402
    EntityType,
    RoleName,
    grant_role,
    revoke_role,
    roles_for_actor,
)
from lib.shared.ids import uuid7  # noqa: E402
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


TENANT_ROLE_NAMES = frozenset(("admin", "finance", "legal", "leadership"))
ENTITY_ROLE_NAMES = frozenset(("owner", "contributor", "viewer"))
ENTITY_TYPES = frozenset(("goal", "commitment", "decision", "resource"))


class RoleCliError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Fyralis actor_roles grants.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List active actor roles.")
    _add_common_identity_args(list_parser)

    grant_parser = subparsers.add_parser("grant", help="Grant a role.")
    _add_common_identity_args(grant_parser)
    _add_role_target_args(grant_parser)
    grant_parser.add_argument(
        "--granted-by",
        help=(
            "Actor UUID that approved the grant. Required for normal "
            "production grants."
        ),
    )
    grant_parser.add_argument(
        "--allow-bootstrap",
        action="store_true",
        help=(
            "Allow the first tenant-wide admin/leadership grant when no "
            "operator role exists yet."
        ),
    )

    revoke_parser = subparsers.add_parser("revoke", help="Revoke a role.")
    _add_common_identity_args(revoke_parser)
    _add_role_target_args(revoke_parser)

    return parser


def _add_common_identity_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tenant", required=True, help="Tenant UUID.")
    parser.add_argument("--actor", required=True, help="Actor UUID receiving role.")
    parser.add_argument(
        "--operator",
        help=(
            "Actor UUID performing this operator action. Required except "
            "for explicit first-admin bootstrap grants."
        ),
    )


def _add_role_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--entity-type",
        required=True,
        choices=sorted((*ENTITY_TYPES, "tenant")),
        help="Role scope type.",
    )
    parser.add_argument(
        "--entity-id",
        help="Entity UUID for goal/commitment/decision/resource grants.",
    )
    parser.add_argument(
        "--role",
        required=True,
        choices=sorted((*TENANT_ROLE_NAMES, *ENTITY_ROLE_NAMES)),
        help="Role name.",
    )


def _parse_uuid(value: str | None, *, field: str, required: bool = True) -> UUID | None:
    if value is None:
        if required:
            raise RoleCliError(f"{field} is required")
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise RoleCliError(f"{field} must be a UUID") from exc


def _role_target(args: argparse.Namespace) -> tuple[EntityType, UUID | None, RoleName]:
    entity_type = str(args.entity_type)
    entity_id = _parse_uuid(args.entity_id, field="entity_id", required=False)
    role = str(args.role)

    if entity_type == "tenant":
        if entity_id is not None:
            raise RoleCliError("tenant-scoped roles must not include --entity-id")
        if role not in TENANT_ROLE_NAMES:
            raise RoleCliError(
                "tenant-scoped roles are admin, finance, legal, leadership"
            )
    else:
        if entity_id is None:
            raise RoleCliError(
                "entity-scoped roles require --entity-id for the target row"
            )
        if role not in ENTITY_ROLE_NAMES:
            raise RoleCliError(
                "entity-scoped roles are owner, contributor, viewer"
            )

    return entity_type, entity_id, role  # type: ignore[return-value]


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    actor_id = _parse_uuid(args.actor, field="actor")
    assert tenant_id is not None
    assert actor_id is not None

    if args.command == "list":
        operator_id = await _authorized_operator_id(
            conn,
            tenant_id=tenant_id,
            args=args,
        )
        async with conn.transaction():
            roles = await roles_for_actor(actor_id, conn=conn, tenant_id=tenant_id)
            await _record_operator_action(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_id,
                action="role.list",
                resource_type="actor_role",
                resource_id=actor_id,
                metadata={
                    "target_actor_id": str(actor_id),
                    "result_count": len(roles),
                },
            )
        return {
            "ok": True,
            "action": "list",
            "tenant_id": str(tenant_id),
            "actor_id": str(actor_id),
            "roles": [_jsonable_role(row) for row in roles],
        }

    entity_type, entity_id, role = _role_target(args)

    if args.command == "grant":
        granted_by = _parse_uuid(
            args.granted_by,
            field="granted_by",
            required=False,
        )
        operator_id, operator_bootstrap = await _authorized_role_mutation_operator_id(
            conn,
            tenant_id=tenant_id,
            target_actor_id=actor_id,
            args=args,
            entity_type=entity_type,
            role=role,
            granted_by=granted_by,
        )
        async with conn.transaction():
            await grant_role(
                actor_id,
                entity_type,
                entity_id,
                role,
                granted_by,
                conn=conn,
                tenant_id=tenant_id,
            )
            await _record_operator_action(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_id,
                action="role.grant",
                resource_type="actor_role",
                resource_id=actor_id,
                metadata={
                    "target_actor_id": str(actor_id),
                    "entity_type": entity_type,
                    "entity_id": str(entity_id) if entity_id else None,
                    "role": role,
                    "granted_by": str(granted_by) if granted_by else None,
                    "operator_bootstrap": operator_bootstrap,
                },
            )
        return _result(
            "grant",
            tenant_id=tenant_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            role=role,
            granted_by=granted_by,
        )

    if args.command == "revoke":
        operator_id = await _authorized_operator_id(
            conn,
            tenant_id=tenant_id,
            args=args,
        )
        async with conn.transaction():
            revoked = await revoke_role(
                actor_id,
                entity_type,
                entity_id,
                role,
                conn=conn,
                tenant_id=tenant_id,
            )
            await _record_operator_action(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_id,
                action="role.revoke",
                resource_type="actor_role",
                resource_id=actor_id,
                metadata={
                    "target_actor_id": str(actor_id),
                    "entity_type": entity_type,
                    "entity_id": str(entity_id) if entity_id else None,
                    "role": role,
                    "revoked": revoked,
                    "operator_bootstrap": False,
                },
            )
        return _result(
            "revoke",
            tenant_id=tenant_id,
            actor_id=actor_id,
            entity_type=entity_type,
            entity_id=entity_id,
            role=role,
            revoked=revoked,
        )

    raise RoleCliError(f"unknown command {args.command!r}")


async def _authorized_operator_id(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    args: argparse.Namespace,
) -> UUID:
    operator_id = _parse_uuid(
        getattr(args, "operator", None),
        field="operator",
        required=True,
    )
    assert operator_id is not None
    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_id,
        error_type=RoleCliError,
    )
    return operator_id


async def _authorized_role_mutation_operator_id(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_actor_id: UUID,
    args: argparse.Namespace,
    entity_type: EntityType,
    role: RoleName,
    granted_by: UUID | None,
) -> tuple[UUID, bool]:
    operator_id = _parse_uuid(
        getattr(args, "operator", None),
        field="operator",
        required=False,
    )
    if operator_id is not None:
        await require_tenant_operator(
            conn,
            tenant_id=tenant_id,
            actor_id=operator_id,
            error_type=RoleCliError,
        )
        return operator_id, False

    if not getattr(args, "allow_bootstrap", False):
        raise RoleCliError(
            "--operator is required for role grant/revoke outside bootstrap"
        )
    await _ensure_bootstrap_grant_allowed(
        conn,
        tenant_id=tenant_id,
        target_actor_id=target_actor_id,
        entity_type=entity_type,
        role=role,
        operator_id=granted_by or target_actor_id,
    )
    return granted_by or target_actor_id, True


async def _ensure_bootstrap_grant_allowed(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_actor_id: UUID,
    entity_type: EntityType,
    role: RoleName,
    operator_id: UUID,
) -> None:
    if entity_type != "tenant" or role not in {"admin", "leadership"}:
        raise RoleCliError(
            "bootstrap is limited to first tenant admin/leadership grant"
        )
    operator_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM actors WHERE id = $1 AND tenant_id = $2)",
        operator_id,
        tenant_id,
    )
    target_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM actors WHERE id = $1 AND tenant_id = $2)",
        target_actor_id,
        tenant_id,
    )
    if not operator_exists or not target_exists:
        raise RoleCliError("bootstrap actor must exist in the target tenant")
    existing_operator = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM actor_roles
          WHERE tenant_id = $1
            AND entity_type = 'tenant'
            AND entity_id IS NULL
            AND role IN ('admin', 'leadership')
            AND revoked_at IS NULL
        )
        """,
        tenant_id,
    )
    if existing_operator:
        raise RoleCliError("bootstrap is allowed only before an operator exists")


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_type: str,
    resource_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
        """,
        uuid7(),
        tenant_id,
        actor_id,
        action,
        resource_type,
        resource_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


def _result(action: str, **fields: Any) -> dict[str, Any]:
    out = {"ok": True, "action": action}
    for key, value in fields.items():
        if isinstance(value, UUID):
            out[key] = str(value)
        else:
            out[key] = value
    if out.get("entity_id") is None:
        out.pop("entity_id", None)
    if out.get("granted_by") is None:
        out.pop("granted_by", None)
    return out


def _jsonable_role(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, UUID):
            out[key] = str(value)
        elif isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


async def _main_async(args: argparse.Namespace) -> int:
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            file=sys.stderr,
        )
        return 2
    conn = await asyncpg.connect(dsn=args.dsn)
    try:
        await _register_codecs(conn)
        result = await run_command(args, conn=conn)
    finally:
        await conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except RoleCliError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
