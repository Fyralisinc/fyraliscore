from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.observability.metrics import render_default
from services.workers.housekeeper.retention import (
    delete_expired_think_run_artifacts,
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
