"""N-ary relation frame projection.

Relation frames are the semantic source of truth. This module exposes a small
deterministic compiler that projects accepted frames into the binary
``model_edges`` graph only where the existing graph ontology has a useful
operational edge kind.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EdgeRegistryError
from lib.shared.errors import ValidationError

from services.domain.models.edges_repo import EdgesRepo

from .repo import EdgeIntelligenceRepo
from .types import RelationEdgeProjection


@dataclass(frozen=True)
class RelationProjectionRule:
    relation_kind: str
    rule_name: str
    source_role: str
    target_role: str
    edge_kind: str


@dataclass(frozen=True)
class RelationFrameProjectionReport:
    relation_id: UUID
    relation_kind: str
    edge_ids: list[UUID] = field(default_factory=list)
    projections: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


_PROJECTION_RULES: dict[str, tuple[RelationProjectionRule, ...]] = {
    "blocked_workstream": (
        RelationProjectionRule(
            relation_kind="blocked_workstream",
            rule_name="blocker_blocks_work",
            source_role="blocker",
            target_role="blocked_work",
            edge_kind="blocks",
        ),
        RelationProjectionRule(
            relation_kind="blocked_workstream",
            rule_name="blocked_work_warns_downstream_risk",
            source_role="blocked_work",
            target_role="downstream_risk",
            edge_kind="early_warning_for",
        ),
        RelationProjectionRule(
            relation_kind="blocked_workstream",
            rule_name="resolution_contributes_to_blocker_resolution",
            source_role="possible_resolution",
            target_role="blocker",
            edge_kind="contributes_to_resolution",
        ),
    ),
}


async def project_relation_frame(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    relation_id: UUID,
    created_by_event_id: UUID | None = None,
    max_edges: int = 12,
    repo: EdgeIntelligenceRepo | None = None,
    edges_repo: EdgesRepo | None = None,
) -> RelationFrameProjectionReport:
    repo = repo or EdgeIntelligenceRepo()
    edges_repo = edges_repo or EdgesRepo()
    frame = await repo.get_relation_frame(
        conn,
        tenant_id=tenant_id,
        relation_id=relation_id,
    )
    relation_kind = str(frame["relation_kind"])
    report = RelationFrameProjectionReport(
        relation_id=relation_id,
        relation_kind=relation_kind,
    )
    if frame["status"] != "accepted" or frame["write_policy"] != "project_edges":
        report.skipped.append({
            "reason": "frame_not_projectable",
            "status": frame["status"],
            "write_policy": frame["write_policy"],
        })
        return report

    rules = _PROJECTION_RULES.get(relation_kind, ())
    if not rules:
        report.skipped.append({
            "reason": "no_projection_rules",
            "relation_kind": relation_kind,
        })
        return report

    participants_by_role = _participants_by_role(frame["participants"])
    for rule in rules:
        sources = participants_by_role.get(rule.source_role, ())
        targets = participants_by_role.get(rule.target_role, ())
        if not sources or not targets:
            report.skipped.append({
                "reason": "missing_role",
                "projection_rule": rule.rule_name,
                "source_role": rule.source_role,
                "target_role": rule.target_role,
            })
            continue
        for source, target in product(sources, targets):
            if len(report.edge_ids) >= max(1, int(max_edges)):
                report.skipped.append({
                    "reason": "projection_cap_reached",
                    "max_edges": max_edges,
                })
                return report
            if source["model_id"] == target["model_id"]:
                report.skipped.append({
                    "reason": "self_projection",
                    "projection_rule": rule.rule_name,
                    "model_id": str(source["model_id"]),
                })
                continue
            confidence = min(
                float(frame["confidence"]),
                float(source["binding_confidence"]),
                float(target["binding_confidence"]),
            )
            try:
                edge_ids = await edges_repo.link(
                    conn,
                    source=source["model_id"],
                    target=target["model_id"],
                    kind=rule.edge_kind,
                    tenant_id=tenant_id,
                    detected_by="think_edge_op",
                    metadata={
                        "relation_instance_id": str(relation_id),
                        "relation_kind": relation_kind,
                        "projection_rule": rule.rule_name,
                        "source_role": rule.source_role,
                        "target_role": rule.target_role,
                        "source": "relation_frame_projection",
                    },
                    created_by_event_id=created_by_event_id,
                    confidence=confidence,
                    evidence_event_ids=frame["evidence_event_ids"],
                    evidence_model_ids=frame["evidence_model_ids"],
                    explanation=frame.get("explanation") or frame.get("evidence_text"),
                    review_status="accepted",
                )
            except (EdgeRegistryError, ValidationError) as exc:
                report.skipped.append({
                    "reason": "edge_projection_failed",
                    "projection_rule": rule.rule_name,
                    "edge_kind": rule.edge_kind,
                    "message": str(getattr(exc, "message", exc))[:500],
                })
                continue
            for edge_id in edge_ids:
                projection = await repo.insert_relation_edge_projection(
                    conn,
                    RelationEdgeProjection(
                        relation_id=relation_id,
                        tenant_id=tenant_id,
                        edge_id=edge_id,
                        projection_rule=rule.rule_name,
                        source_role=rule.source_role,
                        target_role=rule.target_role,
                        source_model_id=source["model_id"],
                        target_model_id=target["model_id"],
                        edge_kind=rule.edge_kind,
                        metadata={
                            "relation_kind": relation_kind,
                            "source_participant_id": str(source["id"]),
                            "target_participant_id": str(target["id"]),
                        },
                    ),
                )
                report.edge_ids.append(edge_id)
                report.projections.append(projection)
    return report


def projection_rules_for_relation_kind(
    relation_kind: str,
) -> tuple[RelationProjectionRule, ...]:
    return _PROJECTION_RULES.get(relation_kind, ())


def _participants_by_role(
    participants: list[dict[str, Any]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for participant in participants:
        grouped.setdefault(str(participant["role"]), []).append(participant)
    return {
        role: tuple(sorted(rows, key=lambda item: (str(item["model_id"]), str(item["id"]))))
        for role, rows in grouped.items()
    }


__all__ = [
    "RelationFrameProjectionReport",
    "RelationProjectionRule",
    "project_relation_frame",
    "projection_rules_for_relation_kind",
]
