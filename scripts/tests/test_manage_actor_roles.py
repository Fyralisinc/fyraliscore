from __future__ import annotations

import argparse
import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from scripts.manage_actor_roles import RoleCliError, build_parser, run_command
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _metadata(row: asyncpg.Record) -> dict[str, object]:
    value = row["metadata"]
    return json.loads(value) if isinstance(value, str) else value


async def _grant_operator_role(
    conn: asyncpg.Connection,
    *,
    tenant,
    actor_id,
) -> None:
    await grant_role(
        actor_id,
        "tenant",
        None,
        "admin",
        actor_id,
        conn=conn,
        tenant_id=tenant,
    )


def test_manage_actor_roles_rejects_invalid_role_scope() -> None:
    args = _parse(
        [
            "grant",
            "--tenant",
            str(uuid7()),
            "--actor",
            str(uuid7()),
            "--entity-type",
            "tenant",
            "--role",
            "viewer",
        ]
    )

    with pytest.raises(RoleCliError, match="tenant-scoped roles"):
        # Validation happens in run_command after UUID normalization; this
        # direct helper path keeps the test DB-free.
        from scripts.manage_actor_roles import _role_target

        _role_target(args)


@pytest.mark.asyncio
async def test_manage_actor_roles_grant_list_revoke_cycle(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "Operator target")
        operator_id = await insert_actor(conn, tenant, "Operator approver")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_id)

        grant_result = await run_command(
            _parse(
                [
                    "grant",
                    "--tenant",
                    str(tenant),
                    "--actor",
                    str(actor_id),
                    "--entity-type",
                    "tenant",
                    "--role",
                    "admin",
                    "--granted-by",
                    str(operator_id),
                    "--operator",
                    str(operator_id),
                ]
            ),
            conn=conn,
        )
        assert grant_result == {
            "ok": True,
            "action": "grant",
            "tenant_id": str(tenant),
            "actor_id": str(actor_id),
            "entity_type": "tenant",
            "role": "admin",
            "granted_by": str(operator_id),
        }
        grant_audit = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'role.grant'
              AND resource_id = $2
            """,
            tenant,
            actor_id,
        )
        assert grant_audit is not None
        assert grant_audit["actor_id"] == operator_id
        assert grant_audit["resource_type"] == "actor_role"
        grant_metadata = _metadata(grant_audit)
        assert grant_metadata["role"] == "admin"
        assert grant_metadata["target_actor_id"] == str(actor_id)
        assert grant_metadata["operator_bootstrap"] is False

        list_result = await run_command(
            _parse(
                [
                    "list",
                    "--tenant",
                    str(tenant),
                    "--actor",
                    str(actor_id),
                    "--operator",
                    str(operator_id),
                ]
            ),
            conn=conn,
        )
        assert list_result["ok"] is True
        assert [
            (row["entity_type"], row["entity_id"], row["role"])
            for row in list_result["roles"]
        ] == [("tenant", None, "admin")]
        list_audit = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'role.list'
              AND resource_id = $2
            """,
            tenant,
            actor_id,
        )
        assert list_audit is not None
        assert list_audit["actor_id"] == operator_id
        assert list_audit["resource_type"] == "actor_role"
        assert _metadata(list_audit)["result_count"] == 1

        revoke_result = await run_command(
            _parse(
                [
                    "revoke",
                    "--tenant",
                    str(tenant),
                    "--actor",
                    str(actor_id),
                    "--operator",
                    str(operator_id),
                    "--entity-type",
                    "tenant",
                    "--role",
                    "admin",
                ]
            ),
            conn=conn,
        )
        assert revoke_result == {
            "ok": True,
            "action": "revoke",
            "tenant_id": str(tenant),
            "actor_id": str(actor_id),
            "entity_type": "tenant",
            "role": "admin",
            "revoked": True,
        }
        revoke_audit = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'role.revoke'
              AND resource_id = $2
            """,
            tenant,
            actor_id,
        )
        assert revoke_audit is not None
        assert revoke_audit["actor_id"] == operator_id
        assert revoke_audit["resource_type"] == "actor_role"
        revoke_metadata = _metadata(revoke_audit)
        assert revoke_metadata["role"] == "admin"
        assert revoke_metadata["revoked"] is True
        assert revoke_metadata["operator_bootstrap"] is False

        list_after_revoke = await run_command(
            _parse(
                [
                    "list",
                    "--tenant",
                    str(tenant),
                    "--actor",
                    str(actor_id),
                    "--operator",
                    str(operator_id),
                ]
            ),
            conn=conn,
        )
        assert list_after_revoke["roles"] == []


@pytest.mark.asyncio
async def test_manage_actor_roles_requires_operator_for_normal_grant(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "Operator target")

        with pytest.raises(RoleCliError, match="--operator is required"):
            await run_command(
                _parse(
                    [
                        "grant",
                        "--tenant",
                        str(tenant),
                        "--actor",
                        str(actor_id),
                        "--entity-type",
                        "tenant",
                        "--role",
                        "admin",
                    ]
                ),
                conn=conn,
            )


@pytest.mark.asyncio
async def test_manage_actor_roles_allows_explicit_first_admin_bootstrap(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "First admin")

        result = await run_command(
            _parse(
                [
                    "grant",
                    "--tenant",
                    str(tenant),
                    "--actor",
                    str(actor_id),
                    "--entity-type",
                    "tenant",
                    "--role",
                    "admin",
                    "--granted-by",
                    str(actor_id),
                    "--allow-bootstrap",
                ]
            ),
            conn=conn,
        )

        assert result["ok"] is True
        assert result["action"] == "grant"
        metadata = _metadata(
            await conn.fetchrow(
                """
                SELECT metadata
                FROM operator_action_log
                WHERE tenant_id = $1
                  AND action = 'role.grant'
                  AND resource_id = $2
                """,
                tenant,
                actor_id,
            )
        )
        assert metadata["operator_bootstrap"] is True

        second_actor_id = await insert_actor(conn, tenant, "Second admin")
        with pytest.raises(RoleCliError, match="only before an operator exists"):
            await run_command(
                _parse(
                    [
                        "grant",
                        "--tenant",
                        str(tenant),
                        "--actor",
                        str(second_actor_id),
                        "--entity-type",
                        "tenant",
                        "--role",
                        "admin",
                        "--allow-bootstrap",
                    ]
                ),
                conn=conn,
            )
