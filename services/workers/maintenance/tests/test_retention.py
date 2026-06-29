from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.observability.metrics import render_default
from services.workers.housekeeper.retention import (
    SAGE_TRACE_RETENTION_TABLES,
    RetentionTableSpec,
    delete_expired_table_rows,
    delete_expired_think_run_artifacts,
    sage_trace_retention_cutoff,
    think_run_artifact_retention_cutoff,
)


pytestmark = pytest.mark.integration


async def _seed_artifact(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    captured_at: datetime,
    stage: str = "response",
) -> UUID:
    artifact_id = uuid7()
    await conn.execute(
        """
        INSERT INTO think_run_artifacts (
            id, run_id, tenant_id, stage, payload, captured_at
        ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        artifact_id,
        uuid7(),
        tenant_id,
        stage,
        json.dumps({"marker": str(artifact_id)}),
        captured_at,
    )
    return artifact_id


ZERO_VECTOR_768 = "[" + ",".join("0" for _ in range(768)) + "]"


async def _seed_inquiry_fixture(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    created_at: datetime,
) -> tuple[UUID, UUID]:
    inquiry_session_id = uuid7()
    model_id = uuid7()
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
            id, tenant_id, signal_ref_type, route, status, stop_status, created_at
        ) VALUES (
            $1, $2, 'internal', 'FAST_PATH', 'completed',
            'sufficient_for_reasoning', $3
        )
        """,
        inquiry_session_id,
        tenant_id,
        created_at,
    )
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id, proposition, "natural", embedding,
            scope_temporal, confidence, confidence_at_assertion
        ) VALUES (
            $1, $2, $3, '{"kind":"belief"}'::jsonb, 'retention test model',
            $4::vector, '{}'::jsonb, 0.5, 0.5
        )
        """,
        model_id,
        tenant_id,
        uuid7(),
        ZERO_VECTOR_768,
    )
    return inquiry_session_id, model_id


async def _seed_sage_trace_row(
    conn: asyncpg.Connection,
    *,
    spec: RetentionTableSpec,
    tenant_id: UUID,
    inquiry_session_id: UUID,
    model_id: UUID,
    captured_at: datetime,
    suffix: str,
) -> UUID:
    row_id = uuid7()
    if spec.table == "sage_reader_activations":
        await conn.execute(
            """
            INSERT INTO sage_reader_activations (
                id, tenant_id, inquiry_session_id, question_id, model_id,
                activation_score, created_at
            ) VALUES ($1, $2, $3, $4, $5, 0.7, $6)
            """,
            row_id,
            tenant_id,
            inquiry_session_id,
            f"q-{suffix}",
            model_id,
            captured_at,
        )
    elif spec.table == "retrieval_plans":
        await conn.execute(
            """
            INSERT INTO retrieval_plans (
                id, tenant_id, inquiry_session_id, question_id, plan_revision,
                created_at
            ) VALUES ($1, $2, $3, $4, 0, $5)
            """,
            row_id,
            tenant_id,
            inquiry_session_id,
            f"q-{suffix}",
            captured_at,
        )
    elif spec.table == "omitted_evidence":
        await conn.execute(
            """
            INSERT INTO omitted_evidence (
                id, tenant_id, inquiry_session_id, question_id, source_type,
                source_ref, omission_reason, created_at
            ) VALUES ($1, $2, $3, $4, 'model', $5, 'budget_exhausted', $6)
            """,
            row_id,
            tenant_id,
            inquiry_session_id,
            f"q-{suffix}",
            f"source-{suffix}",
            captured_at,
        )
    elif spec.table == "inquiry_outcome_events":
        await conn.execute(
            """
            INSERT INTO inquiry_outcome_events (
                id, tenant_id, inquiry_session_id, event_type, payload, created_at
            ) VALUES (
                $1, $2, $3, 'retrieved_evidence_omitted',
                $4::jsonb, $5
            )
            """,
            row_id,
            tenant_id,
            inquiry_session_id,
            json.dumps({"marker": suffix}),
            captured_at,
        )
    else:  # pragma: no cover - catalog changes should add a seeder branch.
        raise AssertionError(f"no seeder for {spec.table}")
    return row_id


async def _trace_ids_for_tenant(
    conn: asyncpg.Connection,
    *,
    spec: RetentionTableSpec,
    tenant_id: UUID,
) -> set[UUID]:
    return {
        row["id"]
        for row in await conn.fetch(
            f"SELECT id FROM {spec.table} WHERE tenant_id = $1",
            tenant_id,
        )
    }


@pytest.mark.asyncio
async def test_think_run_artifact_retention_deletes_only_expired_batch(
    m_pool: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    async with m_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'retention-test')",
            tenant_id,
        )
        old_a = await _seed_artifact(
            conn,
            tenant_id=tenant_id,
            captured_at=cutoff - timedelta(days=2),
        )
        old_b = await _seed_artifact(
            conn,
            tenant_id=tenant_id,
            captured_at=cutoff - timedelta(days=1),
        )
        recent = await _seed_artifact(
            conn,
            tenant_id=tenant_id,
            captured_at=cutoff + timedelta(seconds=1),
        )

        first = await delete_expired_think_run_artifacts(
            conn,
            cutoff=cutoff,
            batch_size=1,
        )
        assert first.deleted == 1
        assert first.matched == 1
        assert first.dry_run is False
        assert first.table == "think_run_artifacts"

        remaining_after_first = {
            row["id"]
            for row in await conn.fetch(
                "SELECT id FROM think_run_artifacts WHERE tenant_id = $1",
                tenant_id,
            )
        }
        assert old_a not in remaining_after_first
        assert {old_b, recent} <= remaining_after_first

        second = await delete_expired_think_run_artifacts(
            conn,
            cutoff=cutoff,
            batch_size=10,
        )
        assert second.deleted == 1
        assert second.matched == 1

        remaining_after_second = {
            row["id"]
            for row in await conn.fetch(
                "SELECT id FROM think_run_artifacts WHERE tenant_id = $1",
                tenant_id,
            )
        }
        assert remaining_after_second == {recent}


@pytest.mark.asyncio
async def test_think_run_artifact_retention_dry_run_keeps_rows_and_emits_metric(
    m_pool: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    async with m_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'retention-dry-run-test')",
            tenant_id,
        )
        artifact_id = await _seed_artifact(
            conn,
            tenant_id=tenant_id,
            captured_at=cutoff - timedelta(days=1),
        )

        result = await delete_expired_think_run_artifacts(
            conn,
            cutoff=cutoff,
            batch_size=10,
            dry_run=True,
        )
        assert result.matched == 1
        assert result.deleted == 0
        assert result.dry_run is True

        still_present = await conn.fetchval(
            "SELECT 1 FROM think_run_artifacts WHERE id = $1",
            artifact_id,
        )
        assert still_present == 1

    text = render_default()
    assert (
        'housekeeper_retention_rows_total{table="think_run_artifacts",'
        'mode="dry_run"} 1'
    ) in text
    assert 'housekeeper_retention_eligible_rows{table="think_run_artifacts"} 1' in text


def test_think_run_artifact_retention_cutoff_uses_days() -> None:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    assert think_run_artifact_retention_cutoff(
        now=now,
        retention_days=30,
    ) == datetime(2026, 5, 25, 12, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", SAGE_TRACE_RETENTION_TABLES)
async def test_sage_trace_retention_deletes_only_expired_batch(
    m_pool: asyncpg.Pool,
    tenant_id: UUID,
    spec: RetentionTableSpec,
) -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=90)
    async with m_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'sage-retention-test')",
            tenant_id,
        )
        inquiry_session_id, model_id = await _seed_inquiry_fixture(
            conn,
            tenant_id=tenant_id,
            created_at=cutoff - timedelta(days=3),
        )
        old_a = await _seed_sage_trace_row(
            conn,
            spec=spec,
            tenant_id=tenant_id,
            inquiry_session_id=inquiry_session_id,
            model_id=model_id,
            captured_at=cutoff - timedelta(days=2),
            suffix=f"{spec.table}-old-a",
        )
        old_b = await _seed_sage_trace_row(
            conn,
            spec=spec,
            tenant_id=tenant_id,
            inquiry_session_id=inquiry_session_id,
            model_id=model_id,
            captured_at=cutoff - timedelta(days=1),
            suffix=f"{spec.table}-old-b",
        )
        recent = await _seed_sage_trace_row(
            conn,
            spec=spec,
            tenant_id=tenant_id,
            inquiry_session_id=inquiry_session_id,
            model_id=model_id,
            captured_at=cutoff + timedelta(seconds=1),
            suffix=f"{spec.table}-recent",
        )

        first = await delete_expired_table_rows(
            conn,
            spec,
            cutoff=cutoff,
            batch_size=1,
        )
        assert first.table == spec.table
        assert first.deleted == 1
        assert first.matched == 1

        remaining_after_first = await _trace_ids_for_tenant(
            conn,
            spec=spec,
            tenant_id=tenant_id,
        )
        assert old_a not in remaining_after_first
        assert {old_b, recent} <= remaining_after_first

        second = await delete_expired_table_rows(
            conn,
            spec,
            cutoff=cutoff,
            batch_size=10,
        )
        assert second.deleted == 1
        assert second.matched == 1

        remaining_after_second = await _trace_ids_for_tenant(
            conn,
            spec=spec,
            tenant_id=tenant_id,
        )
        assert remaining_after_second == {recent}


@pytest.mark.asyncio
async def test_sage_trace_retention_dry_run_keeps_rows_and_emits_metric(
    m_pool: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=90)
    spec = SAGE_TRACE_RETENTION_TABLES[0]
    async with m_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'sage-retention-dry-run')",
            tenant_id,
        )
        inquiry_session_id, model_id = await _seed_inquiry_fixture(
            conn,
            tenant_id=tenant_id,
            created_at=cutoff - timedelta(days=2),
        )
        trace_id = await _seed_sage_trace_row(
            conn,
            spec=spec,
            tenant_id=tenant_id,
            inquiry_session_id=inquiry_session_id,
            model_id=model_id,
            captured_at=cutoff - timedelta(days=1),
            suffix="dry-run",
        )

        result = await delete_expired_table_rows(
            conn,
            spec,
            cutoff=cutoff,
            batch_size=10,
            dry_run=True,
        )
        assert result.matched == 1
        assert result.deleted == 0
        assert result.dry_run is True

        assert trace_id in await _trace_ids_for_tenant(
            conn,
            spec=spec,
            tenant_id=tenant_id,
        )

    text = render_default()
    assert (
        'housekeeper_retention_eligible_rows{table="sage_reader_activations"} 1'
        in text
    )


def test_sage_trace_retention_cutoff_uses_days() -> None:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    assert sage_trace_retention_cutoff(
        now=now,
        retention_days=90,
    ) == datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)
