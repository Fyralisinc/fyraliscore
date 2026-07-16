from __future__ import annotations

import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.correction_propagation.projections import (
    ProjectionCorrectionAdapter,
)


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _insert_model(
    conn: asyncpg.Connection,
    *,
    model_id,
    tenant_id,
    resource_id,
    status: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural", embedding,
          scope_entities, scope_temporal, confidence, confidence_at_assertion,
          status
        ) VALUES (
          $1, $2, $3,
          jsonb_build_object(
            'kind', 'state',
            'subject', $4::text,
            'assertion', 'projection replacement integration'
          ),
          'projection replacement integration',
          array_fill(0.0::real, ARRAY[768])::vector,
          jsonb_build_array(
            jsonb_build_object('type', 'resource', 'id', $4::text)
          ),
          '{}'::jsonb, 0.5, 0.5, $5
        )
        """,
        model_id,
        tenant_id,
        uuid7(),
        str(resource_id),
        status,
    )
    await conn.execute(
        """
        INSERT INTO model_scope_entities (
          model_id, tenant_id, entity_type, entity_id, source
        ) VALUES ($1, $2, 'resource', $3, 'integration_test')
        """,
        model_id,
        tenant_id,
        resource_id,
    )


async def _insert_projection(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    subject_key: str,
    model_id,
) -> None:
    await conn.execute(
        """
        INSERT INTO projection_snapshots (
          tenant_id, projection_name, projection_version, subject_key,
          payload, confidence, source_model_ids
        ) VALUES (
          $1, 'resources', 'v1', $2, '{}'::jsonb, 1.0, ARRAY[$3]::uuid[]
        )
        """,
        tenant_id,
        subject_key,
        model_id,
    )
    await conn.execute(
        """
        INSERT INTO projection_dependencies (
          tenant_id, projection_name, projection_version, subject_key,
          ref_kind, ref_value, reason
        ) VALUES (
          $1, 'resources', 'v1', $2, 'model', $3, 'source_model'
        )
        """,
        tenant_id,
        subject_key,
        str(model_id),
    )


async def test_referent_replacement_invalidates_only_active_tenant_models(
    db_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    other_tenant_id = uuid7()
    predecessor_id = uuid7()
    other_resource_id = uuid7()
    active_model_id = uuid7()
    archived_model_id = uuid7()
    other_resource_model_id = uuid7()
    foreign_model_id = uuid7()
    affected_subject = f"resource:{predecessor_id}"
    archived_subject = f"resource:{predecessor_id}:archived"
    other_resource_subject = f"resource:{other_resource_id}"
    foreign_subject = f"resource:{predecessor_id}:foreign"
    first_cause_event_id = uuid7()
    second_cause_event_id = uuid7()

    async with db_pool.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        try:
            await conn.executemany(
                """
                INSERT INTO tenants (id, name, is_demo)
                VALUES ($1, $2, FALSE)
                ON CONFLICT (id) DO NOTHING
                """,
                [
                    (tenant_id, f"projection-{tenant_id}"),
                    (other_tenant_id, f"projection-{other_tenant_id}"),
                ],
            )
            await _insert_model(
                conn,
                model_id=active_model_id,
                tenant_id=tenant_id,
                resource_id=predecessor_id,
                status="active",
            )
            await _insert_model(
                conn,
                model_id=archived_model_id,
                tenant_id=tenant_id,
                resource_id=predecessor_id,
                status="archived",
            )
            await _insert_model(
                conn,
                model_id=other_resource_model_id,
                tenant_id=tenant_id,
                resource_id=other_resource_id,
                status="active",
            )
            await _insert_model(
                conn,
                model_id=foreign_model_id,
                tenant_id=other_tenant_id,
                resource_id=predecessor_id,
                status="active",
            )
            await _insert_projection(
                conn,
                tenant_id=tenant_id,
                subject_key=affected_subject,
                model_id=active_model_id,
            )
            await _insert_projection(
                conn,
                tenant_id=tenant_id,
                subject_key=archived_subject,
                model_id=archived_model_id,
            )
            await _insert_projection(
                conn,
                tenant_id=tenant_id,
                subject_key=other_resource_subject,
                model_id=other_resource_model_id,
            )
            await _insert_projection(
                conn,
                tenant_id=other_tenant_id,
                subject_key=foreign_subject,
                model_id=foreign_model_id,
            )

            adapter = ProjectionCorrectionAdapter()
            first_report = await adapter.invalidate_for_canonical_referent(
                conn,
                tenant_id=tenant_id,
                canonical_referent_type="resource",
                canonical_referent_id=predecessor_id,
                cause_event_id=first_cause_event_id,
            )

            assert [subject.subject_key for subject in first_report.invalidated_subjects] == [
                affected_subject
            ]
            assert len(first_report.refresh_job_ids) == 1
            assert await conn.fetchval(
                """
                SELECT count(*)
                FROM models
                WHERE tenant_id=$1
                  AND id=ANY($2::uuid[])
                """,
                tenant_id,
                [active_model_id, archived_model_id, other_resource_model_id],
            ) == 3
            assert await conn.fetchval(
                """
                SELECT status
                FROM models
                WHERE tenant_id=$1 AND id=$2
                """,
                tenant_id,
                active_model_id,
            ) == "active"
            assert await conn.fetchval(
                """
                SELECT count(*)
                FROM projection_snapshots
                WHERE tenant_id=$1 AND subject_key=$2
                """,
                tenant_id,
                affected_subject,
            ) == 0
            assert await conn.fetchval(
                """
                SELECT count(*)
                FROM projection_dependencies
                WHERE tenant_id=$1 AND subject_key=$2
                """,
                tenant_id,
                affected_subject,
            ) == 0
            assert await conn.fetchval(
                """
                SELECT count(*)
                FROM projection_snapshots
                WHERE (tenant_id=$1 AND subject_key=ANY($2::text[]))
                   OR (tenant_id=$3 AND subject_key=$4)
                """,
                tenant_id,
                [archived_subject, other_resource_subject],
                other_tenant_id,
                foreign_subject,
            ) == 3

            await _insert_projection(
                conn,
                tenant_id=tenant_id,
                subject_key=affected_subject,
                model_id=active_model_id,
            )
            second_report = await adapter.invalidate_for_canonical_referent(
                conn,
                tenant_id=tenant_id,
                canonical_referent_type="resource",
                canonical_referent_id=str(predecessor_id),
                cause_event_id=second_cause_event_id,
            )

            assert second_report.refresh_job_ids == first_report.refresh_job_ids
            job = await conn.fetchrow(
                """
                SELECT id, reason, event_ids, payload, status
                FROM projection_refresh_jobs
                WHERE tenant_id=$1
                  AND projection_name='resources'
                  AND projection_version='v1'
                  AND subject_key=$2
                """,
                tenant_id,
                affected_subject,
            )
            assert job is not None
            assert job["id"] == first_report.refresh_job_ids[0]
            assert job["reason"] == "dependency_delta"
            assert job["status"] == "pending"
            assert set(job["event_ids"]) == {
                first_cause_event_id,
                second_cause_event_id,
            }
            payload = (
                dict(job["payload"])
                if isinstance(job["payload"], dict)
                else json.loads(job["payload"])
            )
            assert payload == {
                "correction_kind": "canonical_referent_replaced",
                "canonical_referent": {
                    "type": "resource",
                    "id": str(predecessor_id),
                },
                "scoped_model_ids": [str(active_model_id)],
            }
        finally:
            await transaction.rollback()
