from __future__ import annotations

from uuid import uuid4

import asyncpg
import pytest

from services.ingest.connector_platform.rollout_evidence import (
    PostgresRolloutEvidenceSink,
)
from services.ingest.connector_platform.rollout_store import (
    PostgresRolloutRepository,
)
from services.ingest.connector_runtime.rollout import RolloutRevision, RolloutStage
from services.ingest.connector_runtime.shadow import ShadowReport


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TENANT_TABLES = (
    "source_connector_installations",
    "source_connector_authority_grants",
    "source_connector_credentials",
    "source_connector_installation_data",
    "source_connector_callbacks",
)


async def test_connector_control_plane_schema_is_fail_closed(db_pool) -> None:
    async with db_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity,
                   EXISTS (
                     SELECT 1
                       FROM pg_policy p
                      WHERE p.polrelid = c.oid
                        AND p.polname = 'tenant_isolation'
                   ) AS has_tenant_policy
              FROM pg_class c
             WHERE c.relname = ANY($1::text[])
            """,
            list(TENANT_TABLES),
        )
        by_name = {row["relname"]: row for row in rows}

        assert set(by_name) == set(TENANT_TABLES)
        assert all(row["relrowsecurity"] for row in rows)
        assert all(row["relforcerowsecurity"] for row in rows)
        assert all(row["has_tenant_policy"] for row in rows)

        credential_columns = {
            row["column_name"]
            for row in await connection.fetch(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = 'source_connector_credentials'
                """
            )
        }
        assert "secret_ref" in credential_columns
        assert "secret" not in credential_columns
        assert "secret_value" not in credential_columns


async def test_connector_installation_rls_isolates_tenants(
    db_pool,
    rls_app_pool,
) -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    installation_id = uuid4()
    async with db_pool.acquire() as connection:
        await connection.executemany(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            ((tenant_a, "connector-tenant-a"), (tenant_b, "connector-tenant-b")),
        )
        await connection.execute(
            """
            INSERT INTO source_connector_installations (
              id, tenant_id, connector_id, external_installation_id
            ) VALUES ($1, $2, 'fyralis/slack', 'T-RLS')
            """,
            installation_id,
            tenant_a,
        )

    async with rls_app_pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM source_connector_installations WHERE id = $1",
                installation_id,
            )
            == 0
        )
        async with connection.transaction():
            await connection.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_b),
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM source_connector_installations WHERE id = $1",
                    installation_id,
                )
                == 0
            )
        async with connection.transaction():
            await connection.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_a),
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM source_connector_installations WHERE id = $1",
                    installation_id,
                )
                == 1
            )


async def test_rollout_event_constraints_reject_invalid_evidence(db_pool) -> None:
    async with db_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO source_connector_routing_revisions (
              revision, policy, status, created_by
            ) VALUES (900001, '{"revision": 900001, "global": "legacy"}',
                      'active', 'integration-test')
            """
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO source_connector_rollout_events (
                  id, revision, event_type, connector_id, capability,
                  implementation, outcome
                ) VALUES ($1, 900001, 'execution', 'fyralis/slack',
                          'semantic.identity', 'unbounded-process', 'completed')
                """,
                uuid4(),
            )


async def test_rollout_evidence_writer_feeds_threshold_reader(db_pool) -> None:
    revision_number = 900002
    next_revision = 900003
    async with db_pool.acquire() as connection:
        await connection.executemany(
            """
            INSERT INTO source_connector_routing_revisions (
              revision, policy, status, created_by
            ) VALUES ($1, $2::jsonb, 'staged', 'integration-test')
            """,
            (
                (
                    revision_number,
                    '{"revision": 900002, "global": "legacy"}',
                ),
                (next_revision, '{"revision": 900003, "global": "legacy"}'),
            ),
        )

    active_revision = revision_number
    sink = PostgresRolloutEvidenceSink(db_pool, lambda: active_revision)
    attributes = (
        ("connector_id", "fyralis/slack"),
        ("capability", "semantic.normalization"),
        ("implementation", "connector"),
        ("outcome", "completed"),
    )
    sink.increment("source_connector.rollout.execution", 1, attributes)
    sink.observe("source_connector.rollout.duration_ms", 20.0, attributes)
    sink.increment(
        "source_connector.rollout.execution",
        1,
        tuple(
            (key, "failed" if key == "outcome" else value) for key, value in attributes
        ),
    )
    sink.observe(
        "source_connector.rollout.duration_ms",
        10.0,
        (
            ("connector_id", "fyralis/slack"),
            ("capability", "semantic.normalization"),
            ("implementation", "legacy"),
            ("outcome", "completed"),
        ),
    )
    sink.record(
        ShadowReport(
            connector_id="fyralis/slack",
            installation_id="not-persisted",
            capability="semantic.normalization",
            differences=(),
            connector_error_code="behavior_mismatch",
        )
    )
    sink.record_lifecycle(connector_id="fyralis/slack", outcome="failed")
    sink.record_dlq(
        connector_id="fyralis/slack",
        capability="semantic.normalization",
        implementation="connector",
    )
    active_revision = next_revision
    await sink.flush()

    metrics = await PostgresRolloutRepository(db_pool).read_metrics(
        RolloutRevision(
            revision=revision_number,
            policy={"revision": revision_number, "global": "legacy"},
            stage=RolloutStage.FULL,
        )
    )
    assert metrics.executions == 2
    assert metrics.failures == 1
    assert metrics.parity_samples == 1
    assert metrics.parity_mismatches == 1
    assert metrics.connector_p95_ms == 20.0
    assert metrics.legacy_p95_ms == 10.0
    assert metrics.lifecycle_failures == 1
    assert metrics.connector_dlq_rate == 0.5
    assert metrics.baseline_dlq_rate == 0.0
    async with db_pool.acquire() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM source_connector_rollout_events WHERE revision = $1",
                next_revision,
            )
            == 0
        )
