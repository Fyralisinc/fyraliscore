"""Database gates for migration 0199 event-to-replica attribution."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migration
from services.ingest.ingestion.event_replica_attribution import (
    EventReplicaAttribution,
    EventReplicaIdentityConflict,
    delete_trial_event_replica_attributions,
    purge_expired_event_replica_attributions,
    read_active_event_replica_attributions,
    record_event_replica_attribution,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
_MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "db/migrations/0199_ingestion_event_replica_attributions.sql"
)
_NOW = dt.datetime(2026, 7, 27, 12, 0, tzinfo=dt.timezone.utc)


async def _seed_tenant(pool: asyncpg.Pool, label: str) -> UUID:
    tenant_id = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"event-attribution-{label}-{tenant_id}",
    )
    return tenant_id


def _claim(
    *,
    tenant_id: UUID,
    installation_id: UUID,
    replica_id: str,
    operation_id: str = "issues.list",
) -> EventReplicaAttribution:
    return EventReplicaAttribution(
        trial_namespace="pipeline:github:trial-7",
        source="github",
        tenant_id=tenant_id,
        installation_id=installation_id,
        event_id="github:7:41:0:1:1",
        operation_id=operation_id,
        replica_id=replica_id,
    )


async def test_event_attribution_migration_is_idempotent_and_strict_rls(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = _MIGRATION.read_text(encoding="utf-8")
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=_MIGRATION.name)
        await apply_migration(conn, sql, name=_MIGRATION.name)

        relation = await conn.fetchrow(
            """
            SELECT relrowsecurity, relforcerowsecurity
              FROM pg_class
             WHERE oid =
                 'ingestion_event_replica_attributions'::regclass
            """
        )
        assert relation["relrowsecurity"] is True
        assert relation["relforcerowsecurity"] is True

        policy = await conn.fetchrow(
            """
            SELECT pg_get_expr(polqual, polrelid) AS using_expression,
                   pg_get_expr(polwithcheck, polrelid) AS check_expression
              FROM pg_policy
             WHERE polrelid =
                       'ingestion_event_replica_attributions'::regclass
               AND polname = 'tenant_isolation'
            """
        )
        assert policy is not None
        assert "app.current_tenant" in policy["using_expression"]
        assert "IS NULL" not in policy["using_expression"]
        assert "app.current_tenant" in policy["check_expression"]


async def test_first_commit_owns_event_and_identity_drift_fails_closed(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, "ownership")
    installation_id = uuid7()

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            first = await record_event_replica_attribution(
                conn,
                _claim(
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    replica_id="writer-a",
                ),
                recorded_at=_NOW,
            )
            replay = await record_event_replica_attribution(
                conn,
                _claim(
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    replica_id="writer-b",
                ),
                recorded_at=_NOW + dt.timedelta(seconds=1),
            )

            assert first.attribution.replica_id == "writer-a"
            assert replay.attribution.replica_id == "writer-a"
            assert replay.delivery_count == 2

            with pytest.raises(EventReplicaIdentityConflict):
                await record_event_replica_attribution(
                    conn,
                    _claim(
                        tenant_id=tenant_id,
                        installation_id=uuid7(),
                        replica_id="writer-b",
                    ),
                    recorded_at=_NOW + dt.timedelta(seconds=2),
                )

            with pytest.raises(EventReplicaIdentityConflict):
                await record_event_replica_attribution(
                    conn,
                    _claim(
                        tenant_id=tenant_id,
                        installation_id=installation_id,
                        operation_id="issues.get",
                        replica_id="writer-b",
                    ),
                    recorded_at=_NOW + dt.timedelta(seconds=2),
                )

            rows = await read_active_event_replica_attributions(
                conn,
                trial_namespace="pipeline:github:trial-7",
                source="github",
                tenant_id=tenant_id,
                active_at=_NOW + dt.timedelta(minutes=1),
            )
            assert len(rows) == 1
            assert rows[0].attribution.installation_id == installation_id
            assert rows[0].attribution.operation_id == "issues.list"
            assert rows[0].attribution.replica_id == "writer-a"


async def test_namespace_cleanup_and_expiry_sweep_are_tenant_scoped(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, "cleanup")

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            await record_event_replica_attribution(
                conn,
                _claim(
                    tenant_id=tenant_id,
                    installation_id=uuid7(),
                    replica_id="writer-a",
                ),
                recorded_at=_NOW,
                retention=dt.timedelta(hours=1),
            )
            assert await purge_expired_event_replica_attributions(
                conn,
                tenant_id=tenant_id,
                expired_at=_NOW + dt.timedelta(minutes=59),
            ) == 0
            assert await purge_expired_event_replica_attributions(
                conn,
                tenant_id=tenant_id,
                expired_at=_NOW + dt.timedelta(hours=1),
            ) == 1

            await record_event_replica_attribution(
                conn,
                _claim(
                    tenant_id=tenant_id,
                    installation_id=uuid7(),
                    replica_id="writer-b",
                ),
                recorded_at=_NOW + dt.timedelta(hours=2),
            )
            assert await delete_trial_event_replica_attributions(
                conn,
                trial_namespace="pipeline:github:trial-7",
                source="github",
                tenant_id=tenant_id,
            ) == 1
            assert await read_active_event_replica_attributions(
                conn,
                trial_namespace="pipeline:github:trial-7",
                source="github",
                tenant_id=tenant_id,
                active_at=_NOW + dt.timedelta(hours=2),
            ) == ()
