"""Executable HG-10 probes over derived writers and projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from lib.evaluation.epistemic_repair.p2_oracles import stable_digest
from services.reasoning.edge_intelligence.repo import EdgeIntelligenceRepo
from services.reasoning.edge_intelligence.types import RelationEdgeProjection


@dataclass(frozen=True, slots=True)
class DerivedWriterProbe:
    component: str
    rejected: bool
    unchanged: bool
    error_type: str | None

    @property
    def conforms(self) -> bool:
        return self.rejected and self.unchanged


@dataclass(frozen=True, slots=True)
class ProjectionIdempotenceProbe:
    repeat_count: int
    row_count: int
    semantic_digest_count: int
    stable_projection_id: bool

    @property
    def conforms(self) -> bool:
        return (
            self.repeat_count > 1
            and self.row_count == 1
            and self.semantic_digest_count == 1
            and self.stable_projection_id
        )


async def probe_derived_writer_rejection(
    conn: Any,
    *,
    tenant_id: UUID,
    model_id: UUID,
    component: str,
) -> DerivedWriterProbe:
    """Attempt a forbidden direct semantic write in an isolated savepoint."""

    before = await conn.fetchrow(
        """
        SELECT id, tenant_id, proposition, "natural", scope_actors,
               scope_entities, status
        FROM models WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        model_id,
    )
    error_type: str | None = None
    rejected = False
    try:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE models
                SET "natural" = "natural" || $3
                WHERE tenant_id=$1 AND id=$2
                """,
                tenant_id,
                model_id,
                f" [forbidden-derived-writer:{component}]",
            )
    except Exception as error:  # the database rejection is the evidence
        rejected = True
        error_type = type(error).__name__
    after = await conn.fetchrow(
        """
        SELECT id, tenant_id, proposition, "natural", scope_actors,
               scope_entities, status
        FROM models WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        model_id,
    )
    return DerivedWriterProbe(
        component=component,
        rejected=rejected,
        unchanged=stable_digest(dict(before)) == stable_digest(dict(after)),
        error_type=error_type,
    )


async def probe_projection_idempotence(
    conn: Any,
    *,
    tenant_id: UUID,
    relation_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    repeat_count: int,
) -> ProjectionIdempotenceProbe:
    """Replay one projection and compare its semantic state after every write."""

    if repeat_count < 2:
        raise ValueError("projection idempotence requires at least two applications")
    repo = EdgeIntelligenceRepo()
    edge_id = uuid4()
    projection = RelationEdgeProjection(
        id=uuid4(),
        relation_id=relation_id,
        tenant_id=tenant_id,
        edge_id=edge_id,
        projection_rule="p2_hg10_idempotence",
        source_role="source",
        target_role="target",
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        edge_kind="supports",
        metadata={"evaluator": "p2", "sealed": True},
    )
    returned_ids: list[UUID] = []
    digests: list[str] = []
    for _ in range(repeat_count):
        row = await repo.insert_relation_edge_projection(conn, projection)
        returned_ids.append(row["id"])
        digests.append(
            stable_digest(
                {
                    key: row[key]
                    for key in (
                        "relation_id",
                        "tenant_id",
                        "edge_id",
                        "projection_rule",
                        "source_role",
                        "target_role",
                        "source_model_id",
                        "target_model_id",
                        "edge_kind",
                        "status",
                        "metadata",
                    )
                }
            )
        )
    row_count = await conn.fetchval(
        """
        SELECT count(*) FROM relation_edge_projections
        WHERE tenant_id=$1 AND relation_id=$2
          AND projection_rule='p2_hg10_idempotence'
          AND source_model_id=$3 AND target_model_id=$4
          AND edge_kind='supports'
        """,
        tenant_id,
        relation_id,
        source_model_id,
        target_model_id,
    )
    return ProjectionIdempotenceProbe(
        repeat_count=repeat_count,
        row_count=int(row_count),
        semantic_digest_count=len(set(digests)),
        stable_projection_id=len(set(returned_ids)) == 1,
    )


__all__ = [
    "DerivedWriterProbe",
    "ProjectionIdempotenceProbe",
    "probe_derived_writer_rejection",
    "probe_projection_idempotence",
]
