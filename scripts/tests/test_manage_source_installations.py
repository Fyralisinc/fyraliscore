from __future__ import annotations

from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from scripts.manage_source_installations import build_parser, run_command
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


def _parse(argv: list[str]):
    return build_parser().parse_args(argv)


async def _grant_operator(conn, *, tenant, actor_id: UUID) -> None:
    await grant_role(
        actor_id,
        "tenant",
        None,
        "admin",
        actor_id,
        conn=conn,
        tenant_id=tenant,
    )


def test_parser_exposes_only_common_lifecycle_commands() -> None:
    action = next(item for item in build_parser()._actions if item.choices)
    assert set(action.choices) == {
        "status",
        "pause",
        "resume",
        "maintenance",
        "uninstall",
    }


@pytest.mark.asyncio
async def test_common_installation_status_pause_resume_cycle(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator(conn, tenant=tenant, actor_id=operator)
        installation_id = uuid7()
        await conn.execute(
            """
            INSERT INTO source_connector_installations (
                id, tenant_id, connector_id, external_installation_id,
                desired_state, observed_phase, observed_generation,
                bound_connector_version, enabled_capabilities
            ) VALUES (
                $1, $2, 'fyralis/slack', $3, 'Ready', 'Ready', 1,
                '1.0.0', ARRAY['webhook_ingress.v1']::text[]
            )
            """,
            installation_id,
            tenant,
            f"workspace-{tenant.hex[:12]}",
        )
        await conn.execute(
            """
            INSERT INTO source_connector_authority_grants (
                installation_id, tenant_id, connector_id, credential_owner,
                granted_slot_names, granted_outbound_hosts,
                maximum_trust_tier
            ) VALUES (
                $1, $2, 'fyralis/slack', 'test',
                ARRAY[]::text[], ARRAY[]::text[], 'attested_agent'
            )
            """,
            installation_id,
            tenant,
        )
        status = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator),
                    "--source",
                    "slack",
                ]
            ),
            conn=conn,
        )
        assert status["action"] == "status"
        assert status["installations"][0]["id"] == str(installation_id)
        assert status["installations"][0]["connector_id"] == "fyralis/slack"

        paused = await run_command(
            _parse(
                [
                    "pause",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator),
                    "--installation-id",
                    str(installation_id),
                    "--reason",
                    "provider maintenance",
                ]
            ),
            conn=conn,
        )
        assert paused["installations"][0]["desired_state"] == "Paused"
        assert paused["installations"][0]["generation"] == 2

        resumed = await run_command(
            _parse(
                [
                    "resume",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator),
                    "--installation-id",
                    str(installation_id),
                    "--reason",
                    "provider recovered",
                ]
            ),
            conn=conn,
        )
        assert resumed["installations"][0]["desired_state"] == "Ready"
        assert resumed["installations"][0]["generation"] == 3
        authority_generation = await conn.fetchval(
            """
            SELECT authority_generation
              FROM source_connector_authority_grants
             WHERE installation_id = $1
            """,
            installation_id,
        )
        assert authority_generation == 3
        actions = await conn.fetch(
            """
            SELECT action, resource_type
              FROM operator_action_log
             WHERE tenant_id = $1
               AND action LIKE 'source_installation.%'
             ORDER BY occurred_at
            """,
            tenant,
        )
        assert [row["action"] for row in actions] == [
            "source_installation.status",
            "source_installation.pause",
            "source_installation.resume",
        ]
        assert {row["resource_type"] for row in actions} == {
            "source_connector_installation"
        }
