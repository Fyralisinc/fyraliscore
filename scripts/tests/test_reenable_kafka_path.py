from __future__ import annotations

import json

import asyncpg
import pytest

from scripts.reenable_kafka_path import KafkaPathCliError, _reenable
from scripts.tests.conftest import insert_actor
from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


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


@pytest.mark.asyncio
async def test_reenable_kafka_path_requires_operator_role(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "Unprivileged operator")
        await conn.execute(
            """
            INSERT INTO tenant_flags (tenant_id, flag_name, flag_value, set_by)
            VALUES ($1, $2, FALSE, 'auto:circuit_breaker')
            ON CONFLICT (tenant_id, flag_name)
            DO UPDATE SET flag_value = FALSE, set_by = 'auto:circuit_breaker'
            """,
            tenant,
            KAFKA_PATH_ENABLED,
        )

    with pytest.raises(KafkaPathCliError, match="requires tenant role"):
        await _reenable(
            fresh_db,
            tenant,
            operator_actor_id=actor_id,
            note="broker recovered",
        )


@pytest.mark.asyncio
async def test_reenable_kafka_path_sets_flag_and_audits(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        actor_id = await insert_actor(conn, tenant, "Kafka operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=actor_id)
        await conn.execute(
            """
            INSERT INTO tenant_flags (tenant_id, flag_name, flag_value, set_by)
            VALUES ($1, $2, FALSE, 'auto:circuit_breaker')
            ON CONFLICT (tenant_id, flag_name)
            DO UPDATE SET flag_value = FALSE, set_by = 'auto:circuit_breaker'
            """,
            tenant,
            KAFKA_PATH_ENABLED,
        )

    result = await _reenable(
        fresh_db,
        tenant,
        operator_actor_id=actor_id,
        note="broker recovered",
    )
    assert result == 0

    async with fresh_db.acquire() as conn:
        flag_row = await conn.fetchrow(
            """
            SELECT flag_value, set_by, note
            FROM tenant_flags
            WHERE tenant_id = $1 AND flag_name = $2
            """,
            tenant,
            KAFKA_PATH_ENABLED,
        )
        assert flag_row is not None
        assert flag_row["flag_value"] is True
        assert flag_row["set_by"] == f"operator:{actor_id}"
        assert flag_row["note"] == "broker recovered"

        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'kafka_path.reenable'
            """,
            tenant,
        )
        assert audit_row is not None
        assert audit_row["actor_id"] == actor_id
        assert audit_row["resource_type"] == "tenant_flag"
        assert audit_row["resource_id"] == tenant
        metadata = _metadata(audit_row)
        assert metadata["changed"] is True
        assert metadata["flag_name"] == KAFKA_PATH_ENABLED
        assert metadata["previous_value"] is False
        assert metadata["set_by_before"] == "auto:circuit_breaker"
        assert metadata["set_by_after"] == f"operator:{actor_id}"
        assert metadata["note"] == "broker recovered"
