"""Tenant-scoped fail-closed handling for relation frames."""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

import asyncpg


@dataclass(frozen=True, slots=True)
class RelationCorrectionFenceReport:
    affected_relation_ids: tuple[UUID, ...] = ()
    retired_relation_ids: tuple[UUID, ...] = ()
    needs_review_relation_ids: tuple[UUID, ...] = ()
    retired_projection_ids: tuple[UUID, ...] = ()


class RelationCorrectionAdapter:
    """Fence relation truth without importing reasoning-owned repositories."""

    async def fence_for_models(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        contaminated_model_ids: tuple[UUID, ...],
        cause_event_id: UUID,
    ) -> RelationCorrectionFenceReport:
        if not contaminated_model_ids:
            return RelationCorrectionFenceReport()

        rows = await conn.fetch(
            """
            SELECT relation.id, relation.status, relation.evidence_model_ids,
                   COALESCE(
                     array_agg(DISTINCT participant.model_id)
                       FILTER (WHERE participant.model_id IS NOT NULL),
                     '{}'::uuid[]
                   ) AS participant_model_ids
            FROM relation_instances relation
            LEFT JOIN relation_participants participant
              ON participant.tenant_id=relation.tenant_id
             AND participant.relation_id=relation.id
            WHERE relation.tenant_id=$1
              AND relation.status NOT IN ('rejected', 'retired')
              AND (
                relation.evidence_model_ids && $2::uuid[]
                OR EXISTS (
                  SELECT 1
                  FROM relation_participants affected
                  WHERE affected.tenant_id=relation.tenant_id
                    AND affected.relation_id=relation.id
                    AND affected.model_id=ANY($2::uuid[])
                )
              )
            GROUP BY relation.id, relation.status, relation.evidence_model_ids
            ORDER BY relation.id
            """,
            tenant_id,
            list(contaminated_model_ids),
        )
        contaminated = set(contaminated_model_ids)
        retired: list[UUID] = []
        needs_review: list[UUID] = []
        affected_ids = tuple(row["id"] for row in rows)
        for row in rows:
            evidence_ids = set(row["evidence_model_ids"] or ())
            participant_ids = set(row["participant_model_ids"] or ())
            has_contaminated_participant = bool(participant_ids & contaminated)
            exclusively_contaminated_evidence = bool(evidence_ids) and evidence_ids <= (
                contaminated
            )
            target_status = (
                "retired"
                if has_contaminated_participant or exclusively_contaminated_evidence
                else "needs_review"
            )
            if str(row["status"]) == target_status:
                continue
            await conn.execute(
                """
                UPDATE relation_instances
                SET status=$3,
                    metadata=metadata || $4::jsonb,
                    decided_at=CASE
                      WHEN $3='retired' THEN COALESCE(decided_at, now())
                      ELSE decided_at
                    END,
                    updated_at=now()
                WHERE tenant_id=$1 AND id=$2
                """,
                tenant_id,
                row["id"],
                target_status,
                json.dumps(
                    {
                        "correction_fence": {
                            "cause_event_id": str(cause_event_id),
                            "contaminated_model_ids": [
                                str(model_id)
                                for model_id in contaminated_model_ids
                            ],
                        }
                    },
                    sort_keys=True,
                ),
            )
            if target_status == "retired":
                retired.append(row["id"])
            else:
                needs_review.append(row["id"])

        projection_rows = []
        if affected_ids:
            projection_rows = await conn.fetch(
                """
                UPDATE relation_edge_projections
                SET status='retired',
                    metadata=metadata || $3::jsonb,
                    updated_at=now()
                WHERE tenant_id=$1
                  AND relation_id=ANY($2::uuid[])
                  AND status='active'
                RETURNING id
                """,
                tenant_id,
                list(affected_ids),
                json.dumps(
                    {
                        "correction_fence": {
                            "cause_event_id": str(cause_event_id),
                        }
                    },
                    sort_keys=True,
                ),
            )

        return RelationCorrectionFenceReport(
            affected_relation_ids=affected_ids,
            retired_relation_ids=tuple(retired),
            needs_review_relation_ids=tuple(needs_review),
            retired_projection_ids=tuple(row["id"] for row in projection_rows),
        )


__all__ = ["RelationCorrectionAdapter", "RelationCorrectionFenceReport"]
