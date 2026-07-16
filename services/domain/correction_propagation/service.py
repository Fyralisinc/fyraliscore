"""Direct fail-closed repair after an authoritative grounding correction."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EDGE_REGISTRY
from services.domain.correction_propagation.projections import (
    ProjectionCorrectionAdapter,
    ProjectionCorrectionFenceReport,
)
from services.domain.correction_propagation.relations import (
    RelationCorrectionAdapter,
    RelationCorrectionFenceReport,
)
from services.domain.models.repo import ModelsRepo
from services.domain.triggers import enqueue_model_reeval


_SOURCE_ARCHIVE_DEPENDENCY_KINDS = tuple(
    kind
    for kind, spec in EDGE_REGISTRY.items()
    if spec.on_source_archive is not None
)
_TARGET_ARCHIVE_DEPENDENCY_KINDS = tuple(
    kind
    for kind, spec in EDGE_REGISTRY.items()
    if spec.on_target_archive is not None
)


@dataclass(frozen=True, slots=True)
class DirectCorrectionFenceReport:
    """What the synchronous direct correction fence changed."""

    predecessor_grounding_trace_id: UUID | None
    old_model_ids: tuple[UUID, ...] = ()
    archived_model_ids: tuple[UUID, ...] = ()
    dependent_model_ids: tuple[UUID, ...] = ()
    newly_fenced_model_ids: tuple[UUID, ...] = ()
    reeval_pairs: tuple[tuple[UUID, UUID], ...] = ()
    relation_fence: RelationCorrectionFenceReport = field(
        default_factory=RelationCorrectionFenceReport
    )
    projection_fence: ProjectionCorrectionFenceReport = field(
        default_factory=ProjectionCorrectionFenceReport
    )

    @property
    def correction_found(self) -> bool:
        return self.predecessor_grounding_trace_id is not None


class CorrectionPropagationService:
    """Fence the directly contaminated Model layer in the caller transaction."""

    def __init__(
        self,
        *,
        models_repo: ModelsRepo | None = None,
        relation_adapter: RelationCorrectionAdapter | None = None,
        projection_adapter: ProjectionCorrectionAdapter | None = None,
    ) -> None:
        self._models = models_repo or ModelsRepo(
            pool=None,  # type: ignore[arg-type]
            embedder=None,
            run_topology_on_insert=False,
        )
        self._relations = relation_adapter or RelationCorrectionAdapter()
        self._projections = projection_adapter or ProjectionCorrectionAdapter()

    async def propagate_direct_correction(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        predecessor_grounding_trace_id: UUID | None,
        successor_grounding_trace_id: UUID,
        cause_event_id: UUID,
        corrected_model_id: UUID | None,
    ) -> DirectCorrectionFenceReport:
        """Archive directly wrong Models and fence their direct dependents.

        The source signal and source-semantic history remain immutable. This
        operation only changes canonical Models that were admitted from the
        superseded grounding trace, then hides active Models whose registered
        dependency edges or legacy support arrays point at those Models.
        """

        if predecessor_grounding_trace_id is None:
            return DirectCorrectionFenceReport(
                predecessor_grounding_trace_id=None,
            )
        if predecessor_grounding_trace_id == successor_grounding_trace_id:
            raise ValueError("a corrected grounding trace cannot supersede itself")

        old_rows = await conn.fetch(
            """
            SELECT DISTINCT model.id, model.status
            FROM source_semantic_interpretations interpretation
            JOIN source_semantic_admission_decisions admission
              ON admission.tenant_id=interpretation.tenant_id
             AND admission.interpretation_id=interpretation.id
            JOIN models model
              ON model.tenant_id=admission.tenant_id
             AND model.id=admission.admitted_model_id
            WHERE interpretation.tenant_id=$1
              AND interpretation.grounding_trace_id=$2
              AND admission.disposition='belief_applied'
              AND admission.admitted_model_id IS NOT NULL
              AND ($3::uuid IS NULL OR admission.admitted_model_id <> $3)
            ORDER BY model.id
            """,
            tenant_id,
            predecessor_grounding_trace_id,
            corrected_model_id,
        )
        old_model_ids = tuple(row["id"] for row in old_rows)
        if not old_model_ids:
            return DirectCorrectionFenceReport(
                predecessor_grounding_trace_id=predecessor_grounding_trace_id,
            )
        active_old_model_ids = {
            row["id"] for row in old_rows if str(row["status"]) == "active"
        }

        dependency_rows = await conn.fetch(
            """
            WITH dependency_pairs AS (
              SELECT edge.target_model_id AS dependent_model_id,
                     edge.source_model_id AS cause_model_id
              FROM model_edges edge
              WHERE edge.tenant_id=$1
                AND edge.source_model_id=ANY($2::uuid[])
                AND edge.edge_kind=ANY($3::text[])
                AND edge.status IN ('active', 'inert')

              UNION

              SELECT edge.source_model_id AS dependent_model_id,
                     edge.target_model_id AS cause_model_id
              FROM model_edges edge
              WHERE edge.tenant_id=$1
                AND edge.target_model_id=ANY($2::uuid[])
                AND edge.edge_kind=ANY($4::text[])
                AND edge.status IN ('active', 'inert')

              UNION

              SELECT dependent.id AS dependent_model_id,
                     support.support_model_id AS cause_model_id
              FROM models dependent
              CROSS JOIN LATERAL unnest(
                COALESCE(dependent.supporting_model_ids, '{}'::uuid[])
              ) AS support(support_model_id)
              WHERE dependent.tenant_id=$1
                AND support.support_model_id=ANY($2::uuid[])
            )
            SELECT DISTINCT pair.dependent_model_id, pair.cause_model_id
            FROM dependency_pairs pair
            JOIN models dependent
              ON dependent.tenant_id=$1
             AND dependent.id=pair.dependent_model_id
            WHERE dependent.status='active'
              AND pair.dependent_model_id <> ALL($2::uuid[])
            ORDER BY pair.dependent_model_id, pair.cause_model_id
            """,
            tenant_id,
            list(old_model_ids),
            list(_SOURCE_ARCHIVE_DEPENDENCY_KINDS),
            list(_TARGET_ARCHIVE_DEPENDENCY_KINDS),
        )
        dependency_pairs = tuple(
            (row["dependent_model_id"], row["cause_model_id"])
            for row in dependency_rows
        )

        newly_fenced: set[UUID] = set()
        first_cause_by_dependent: dict[UUID, UUID] = {}
        for dependent_model_id, cause_model_id in dependency_pairs:
            first_cause_by_dependent.setdefault(dependent_model_id, cause_model_id)
        for dependent_model_id, cause_model_id in first_cause_by_dependent.items():
            changed = await self._models.fence_for_correction(
                dependent_model_id,
                tenant_id=tenant_id,
                cause_event_id=cause_event_id,
                cause_model_id=cause_model_id,
                conn=conn,
            )
            if changed is not None:
                newly_fenced.add(dependent_model_id)

        reeval_pairs: list[tuple[UUID, UUID]] = []
        for dependent_model_id, cause_model_id in dependency_pairs:
            if (
                cause_model_id not in active_old_model_ids
                and dependent_model_id not in newly_fenced
            ):
                continue
            await enqueue_model_reeval(
                conn,
                tenant_id=tenant_id,
                model_id=dependent_model_id,
                cause_model_id=cause_model_id,
                cause_kind="grounding_corrected",
            )
            reeval_pairs.append((dependent_model_id, cause_model_id))

        relation_fence = await self._relations.fence_for_models(
            conn,
            tenant_id=tenant_id,
            contaminated_model_ids=old_model_ids,
            cause_event_id=cause_event_id,
        )
        projection_fence = await self._projections.invalidate_for_models(
            conn,
            tenant_id=tenant_id,
            contaminated_model_ids=old_model_ids,
            cause_event_id=cause_event_id,
        )

        archived_model_ids: list[UUID] = []
        for old_model_id in old_model_ids:
            if old_model_id not in active_old_model_ids:
                continue
            await self._models.archive(
                old_model_id,
                reason="superseded",
                cause_event_id=cause_event_id,
                conn=conn,
            )
            archived_model_ids.append(old_model_id)

        return DirectCorrectionFenceReport(
            predecessor_grounding_trace_id=predecessor_grounding_trace_id,
            old_model_ids=old_model_ids,
            archived_model_ids=tuple(archived_model_ids),
            dependent_model_ids=tuple(sorted(first_cause_by_dependent)),
            newly_fenced_model_ids=tuple(sorted(newly_fenced)),
            reeval_pairs=tuple(reeval_pairs),
            relation_fence=relation_fence,
            projection_fence=projection_fence,
        )


__all__ = ["CorrectionPropagationService", "DirectCorrectionFenceReport"]
