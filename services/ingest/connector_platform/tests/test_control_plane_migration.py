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
                     SELECT 1 FROM pg_policy p
                      WHERE p.polrelid = c.oid AND p.polname = 'tenant_isolation'
                   ) AS has_tenant_policy
              FROM pg_class c
             WHERE c.relname = ANY($1::text[])
            """,
            list(TENANT_TABLES),
        )
        assert {row["relname"] for row in rows} == set(TENANT_TABLES)
        assert all(row["relrowsecurity"] for row in rows)
        assert all(row["relforcerowsecurity"] for row in rows)
        assert all(row["has_tenant_policy"] for row in rows)
        credential_columns = {
            row["column_name"]
            for row in await connection.fetch(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = 'source_connector_credentials'
                """
            )
        }
        assert "secret_ref" in credential_columns
        assert not {"secret", "secret_value"}.intersection(credential_columns)


async def test_contract_only_migration_removes_legacy_control_columns(db_pool) -> None:
    async with db_pool.acquire() as connection:
        assert await connection.fetchval(
            "SELECT to_regclass('source_connector_retirement_evidence')"
        ) is None
        metric_columns = {
            row["column_name"]
            for row in await connection.fetch(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = 'source_connector_rollout_metric_windows'
                """
            )
        }
        assert not {
            "parity_samples",
            "parity_mismatches",
            "legacy_p95_ms",
            "baseline_dlq_rate",
        }.intersection(metric_columns)
        trigger_columns = {
            row["column_name"]
            for row in await connection.fetch(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = 'onboarding_triggers'
                """
            )
        }
        assert "connector_installation_id" in trigger_columns
        assert "installation_row_id" not in trigger_columns
        assert "gmail_installation_id" not in trigger_columns


async def test_routing_and_rollout_evidence_reject_legacy_shapes(db_pool) -> None:
    revision = 910001
    async with db_pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO source_connector_routing_revisions (
                  revision, policy, status, created_by
                ) VALUES ($1, $2::jsonb, 'staged', 'integration-test')
                """,
                revision,
                '{"revision":910001,"global":"legacy"}',
            )
        await connection.execute(
            """
            INSERT INTO source_connector_routing_revisions (
              revision, policy, status, created_by
            ) VALUES ($1, $2::jsonb, 'staged', 'integration-test')
            """,
            revision,
            '{"revision":910001,"global":"connector"}',
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO source_connector_rollout_events (
                  id, revision, event_type, connector_id, capability,
                  implementation, outcome
                ) VALUES ($1, $2, 'execution', 'fyralis/slack',
                          'semantic.identity', 'legacy', 'completed')
                """,
                uuid4(),
                revision,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                """
                INSERT INTO source_connector_rollout_events (
                  id, revision, event_type, connector_id, capability,
                  implementation, outcome
                ) VALUES ($1, $2, 'parity', 'fyralis/slack',
                          'semantic.identity', 'connector', 'completed')
                """,
                uuid4(),
                revision,
            )


async def test_connector_evidence_writer_feeds_threshold_reader(db_pool) -> None:
    revision = 910002
    async with db_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO source_connector_routing_revisions (
              revision, policy, status, created_by
            ) VALUES ($1, $2::jsonb, 'staged', 'integration-test')
            """,
            revision,
            '{"revision":910002,"global":"connector"}',
        )
    sink = PostgresRolloutEvidenceSink(db_pool, lambda: revision)
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
            (key, "failed" if key == "outcome" else value)
            for key, value in attributes
        ),
    )
    sink.record_lifecycle(connector_id="fyralis/slack", outcome="failed")
    sink.record_dlq(
        connector_id="fyralis/slack",
        capability="semantic.normalization",
        implementation="connector",
    )
    await sink.flush()
    metrics = await PostgresRolloutRepository(db_pool).read_metrics(
        RolloutRevision(revision, {}, RolloutStage.FULL)
    )
    assert metrics.executions == 2
    assert metrics.failures == 1
    assert metrics.connector_p95_ms == 20.0
    assert metrics.lifecycle_failures == 1
    assert metrics.connector_dlq_rate == 0.5
