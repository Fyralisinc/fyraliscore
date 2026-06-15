"""Housekeeper promotion for high-confidence relationship candidates."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EdgeKindSpec
from services.domain.models.edges_repo import EdgesRepo
from services.reasoning.relationships.ontology_runtime import resolve_edge_kind_spec
from services.reasoning.relationships.repo import RelationshipCandidatesRepo


@dataclass(frozen=True)
class PromotionReport:
    promoted_candidates: int = 0
    promoted_edge_ids: list[UUID] = field(default_factory=list)
    retired_stale_candidates: int = 0
    failed_candidates: int = 0
    errors: list[str] = field(default_factory=list)


async def promote_high_confidence_edges(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int = 50,
    min_confidence: float = 0.80,
    min_leverage: float = 0.75,
    max_uncertainty: float = 0.50,
    repo: RelationshipCandidatesRepo | None = None,
    edges: EdgesRepo | None = None,
) -> PromotionReport:
    """Promote narrow safe candidates into `model_edges`.

    This is deliberately not a general adjudicator. It closes the dormant
    "candidate pile" only for high-confidence observed/causal-confirmed edge
    candidates. Broader inferred/correlated candidates stay pre-truth.
    """
    repo = repo or RelationshipCandidatesRepo()
    edges = edges or EdgesRepo()
    retired = await _retire_expired_candidates(conn, tenant_id=tenant_id)
    rows = await conn.fetch(
        """
        SELECT *
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND candidate_kind = 'edge'
          AND review_status IN ('candidate', 'needs_review')
          AND basis IN ('observed', 'causal_confirmed')
          AND source_model_id IS NOT NULL
          AND target_model_id IS NOT NULL
          AND edge_kind IS NOT NULL
          AND confidence_score >= $2
          AND judgment_leverage_score >= $3
          AND uncertainty_score <= $4
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY judgment_leverage_score DESC, confidence_score DESC, created_at ASC
        LIMIT $5
        """,
        tenant_id,
        float(min_confidence),
        float(min_leverage),
        float(max_uncertainty),
        max(1, int(limit)),
    )

    promoted = 0
    promoted_edge_ids: list[UUID] = []
    failed = 0
    errors: list[str] = []
    for row in rows:
        try:
            spec = await resolve_edge_kind_spec(
                conn,
                tenant_id=tenant_id,
                kind=row["edge_kind"],
            )
            edge_ids = await edges.link(
                conn,
                source=row["source_model_id"],
                target=row["target_model_id"],
                kind=row["edge_kind"],
                tenant_id=tenant_id,
                detected_by="link_miner",
                weight=_candidate_weight(row, spec),
                metadata={
                    "relationship_candidate_id": str(row["id"]),
                    "basis": row["basis"],
                    "promotion": "high_confidence_housekeeper",
                    **_as_dict(row["metadata"]),
                },
                confidence=float(row["confidence_score"] or 0.0),
                evidence_event_ids=list(row["evidence_event_ids"] or []),
                evidence_model_ids=list(row["evidence_model_ids"] or []),
                explanation=row["explanation"],
                review_status="accepted",
            )
            await repo.mark_decided(
                conn,
                candidate_id=row["id"],
                tenant_id=tenant_id,
                review_status="accepted",
                accepted_edge_ids=edge_ids,
                decision_metadata={
                    "reason": "high_confidence_housekeeper_promotion",
                    "promoted_edge_ids": [str(edge_id) for edge_id in edge_ids],
                },
            )
            promoted += 1
            promoted_edge_ids.extend(edge_ids)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            errors.append(f"{row['id']}:{type(exc).__name__}:{exc}")
            await repo.mark_decided(
                conn,
                candidate_id=row["id"],
                tenant_id=tenant_id,
                review_status="needs_review",
                decision_metadata={
                    "reason": "housekeeper_promotion_failed",
                    "error": f"{type(exc).__name__}:{exc}",
                },
            )

    return PromotionReport(
        promoted_candidates=promoted,
        promoted_edge_ids=promoted_edge_ids,
        retired_stale_candidates=retired,
        failed_candidates=failed,
        errors=errors,
    )


async def _retire_expired_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> int:
    return int(
        await conn.fetchval(
            """
            WITH retired AS (
              UPDATE relationship_candidates
              SET review_status = 'retired',
                  decided_at = now(),
                  metadata = metadata || '{"retired_reason":"expired"}'::jsonb
              WHERE tenant_id = $1
                AND review_status IN ('candidate', 'needs_review', 'contested')
                AND expires_at IS NOT NULL
                AND expires_at <= now()
              RETURNING 1
            )
            SELECT count(*) FROM retired
            """,
            tenant_id,
        )
        or 0
    )


def _candidate_weight(row: asyncpg.Record, spec: EdgeKindSpec) -> float | None:
    if not spec.weight_allowed:
        return None
    confidence = float(row["confidence_score"] or 0.0)
    leverage = float(row["judgment_leverage_score"] or 0.0)
    return max(0.1, min(1.0, (confidence + leverage) / 2.0))


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


__all__ = ["PromotionReport", "promote_high_confidence_edges"]
