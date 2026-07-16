"""Fail-closed invalidation for disposable projection snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.store import enqueue_projection_refresh_job
from services.domain.projections.types import (
    ProjectionSubjectRef,
)


@dataclass(frozen=True, slots=True)
class ProjectionCorrectionFenceReport:
    invalidated_subjects: tuple[ProjectionSubjectRef, ...] = ()
    refresh_job_ids: tuple[UUID, ...] = ()


class ProjectionCorrectionAdapter:
    """Remove contaminated views and enqueue their existing rebuild path."""

    async def invalidate_for_canonical_referent(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        canonical_referent_type: str,
        canonical_referent_id: UUID | str,
        cause_event_id: UUID,
    ) -> ProjectionCorrectionFenceReport:
        """Invalidate views derived from active Models scoped to one predecessor.

        Canonical Models remain untouched. The normalized Model-scope sidecar is
        the only discovery surface for this first replacement vertical, and it
        deliberately supports only an exact ``resource`` UUID reference.
        """

        resource_id = _exact_resource_uuid(
            canonical_referent_type,
            canonical_referent_id,
        )
        if resource_id is None:
            return ProjectionCorrectionFenceReport()

        rows = await conn.fetch(
            """
            SELECT DISTINCT scope.model_id
            FROM model_scope_entities AS scope
            JOIN models AS model
              ON model.tenant_id=scope.tenant_id
             AND model.id=scope.model_id
            WHERE scope.tenant_id=$1
              AND model.tenant_id=$1
              AND scope.entity_type=$2
              AND scope.entity_id=$3
              AND model.status='active'
            ORDER BY scope.model_id
            """,
            tenant_id,
            "resource",
            resource_id,
        )
        model_ids = tuple(
            sorted({row["model_id"] for row in rows})
        )
        if not model_ids:
            return ProjectionCorrectionFenceReport()

        canonical_referent = {
            "type": "resource",
            "id": str(resource_id),
        }
        return await self._invalidate_for_models(
            conn,
            tenant_id=tenant_id,
            model_ids=model_ids,
            cause_event_id=cause_event_id,
            refresh_payload={
                "correction_kind": "canonical_referent_replaced",
                "canonical_referent": canonical_referent,
                "scoped_model_ids": [str(model_id) for model_id in model_ids],
            },
        )

    async def invalidate_for_models(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        contaminated_model_ids: tuple[UUID, ...],
        cause_event_id: UUID,
    ) -> ProjectionCorrectionFenceReport:
        if not contaminated_model_ids:
            return ProjectionCorrectionFenceReport()
        model_ids = tuple(sorted(set(contaminated_model_ids)))
        return await self._invalidate_for_models(
            conn,
            tenant_id=tenant_id,
            model_ids=model_ids,
            cause_event_id=cause_event_id,
            refresh_payload={
                "correction_kind": "grounding_corrected",
                "contaminated_model_ids": [
                    str(model_id) for model_id in model_ids
                ],
            },
        )

    async def _invalidate_for_models(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        model_ids: tuple[UUID, ...],
        cause_event_id: UUID,
        refresh_payload: dict[str, Any],
    ) -> ProjectionCorrectionFenceReport:
        model_ref_values = [str(model_id) for model_id in model_ids]
        rows = await conn.fetch(
            """
            SELECT projection_name, projection_version, subject_key
            FROM projection_snapshots
            WHERE tenant_id=$1
              AND source_model_ids && $2::uuid[]

            UNION

            SELECT projection_name, projection_version, subject_key
            FROM projection_dependencies
            WHERE tenant_id=$1
              AND ref_kind='model'
              AND ref_value=ANY($3::text[])

            ORDER BY projection_name, projection_version, subject_key
            """,
            tenant_id,
            list(model_ids),
            model_ref_values,
        )
        subjects = tuple(
            ProjectionSubjectRef(
                projection_name=row["projection_name"],
                projection_version=row["projection_version"],
                subject_key=row["subject_key"],
            )
            for row in rows
        )
        refresh_job_ids: list[UUID] = []
        for subject in subjects:
            refresh_job_ids.append(
                await enqueue_projection_refresh_job(
                    conn,
                    tenant_id=tenant_id,
                    projection_name=subject.projection_name,
                    projection_version=subject.projection_version,
                    subject_key=subject.subject_key,
                    reason="dependency_delta",
                    event_ids=(cause_event_id,),
                    payload=refresh_payload,
                )
            )
            await conn.execute(
                """
                DELETE FROM projection_dependencies
                WHERE tenant_id=$1
                  AND projection_name=$2
                  AND projection_version=$3
                  AND subject_key=$4
                """,
                tenant_id,
                subject.projection_name,
                subject.projection_version,
                subject.subject_key,
            )
            await conn.execute(
                """
                DELETE FROM projection_snapshots
                WHERE tenant_id=$1
                  AND projection_name=$2
                  AND projection_version=$3
                  AND subject_key=$4
                """,
                tenant_id,
                subject.projection_name,
                subject.projection_version,
                subject.subject_key,
            )

        return ProjectionCorrectionFenceReport(
            invalidated_subjects=subjects,
            refresh_job_ids=tuple(refresh_job_ids),
        )


def _exact_resource_uuid(
    canonical_referent_type: str,
    canonical_referent_id: UUID | str,
) -> UUID | None:
    if canonical_referent_type != "resource":
        return None
    if isinstance(canonical_referent_id, UUID):
        return canonical_referent_id
    if not isinstance(canonical_referent_id, str):
        return None
    try:
        parsed = UUID(canonical_referent_id)
    except ValueError:
        return None
    if str(parsed) != canonical_referent_id.lower():
        return None
    return parsed


__all__ = ["ProjectionCorrectionAdapter", "ProjectionCorrectionFenceReport"]
