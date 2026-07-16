"""Fail-closed invalidation for disposable projection snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from services.domain.projections.store import enqueue_projection_refresh_job
from services.domain.projections.types import (
    ProjectionDependencyRef,
    ProjectionSubjectRef,
)


@dataclass(frozen=True, slots=True)
class ProjectionCorrectionFenceReport:
    invalidated_subjects: tuple[ProjectionSubjectRef, ...] = ()
    refresh_job_ids: tuple[UUID, ...] = ()


class ProjectionCorrectionAdapter:
    """Remove contaminated views and enqueue their existing rebuild path."""

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
        model_ref_values = [str(model_id) for model_id in contaminated_model_ids]
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
            list(contaminated_model_ids),
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
        dependency_refs = tuple(
            ProjectionDependencyRef(
                ref_kind="model",
                ref_value=str(model_id),
                reason="grounding_corrected",
            )
            for model_id in contaminated_model_ids
        )
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
                    dependency_refs=dependency_refs,
                    payload={
                        "correction_kind": "grounding_corrected",
                        "contaminated_model_ids": model_ref_values,
                    },
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


__all__ = ["ProjectionCorrectionAdapter", "ProjectionCorrectionFenceReport"]
