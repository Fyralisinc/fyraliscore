"""Promotion from pair evidence aggregates into relationship candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable
from uuid import UUID

import asyncpg

from services.reasoning.edge_intelligence.compiler import (
    EdgeCompilerConfig,
    compile_pair_evidence_candidate,
)
from services.reasoning.edge_intelligence.endpoint_quality import endpoint_quality_gate
from services.reasoning.edge_intelligence.repo import EdgeIntelligenceRepo
from services.reasoning.relationships.repo import RelationshipCandidatesRepo


@dataclass(frozen=True)
class PairEvidencePromotionReport:
    scanned_pair_evidence: int = 0
    candidates_inserted: int = 0
    candidates_skipped: int = 0
    failed_pair_evidence: int = 0
    errors: list[str] = field(default_factory=list)


async def promote_pair_evidence_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int = 50,
    config: EdgeCompilerConfig | None = None,
    edge_repo: EdgeIntelligenceRepo | None = None,
    candidates_repo: RelationshipCandidatesRepo | None = None,
    model_ids: Iterable[UUID] | None = None,
) -> PairEvidencePromotionReport:
    """Compile high-confidence pair evidence into pre-truth candidates."""
    config = config or EdgeCompilerConfig()
    edge_repo = edge_repo or EdgeIntelligenceRepo()
    candidates_repo = candidates_repo or RelationshipCandidatesRepo()
    pair_rows = await edge_repo.list_promotable_pair_evidence(
        conn,
        tenant_id=tenant_id,
        min_confidence=config.min_confidence,
        limit=limit,
        model_ids=model_ids,
    )
    inserted = 0
    skipped = 0
    failed = 0
    errors: list[str] = []
    for evidence in pair_rows:
        try:
            candidate = compile_pair_evidence_candidate(evidence, config=config)
            if candidate is None:
                skipped += 1
                continue
            gate = await endpoint_quality_gate(
                conn,
                tenant_id=tenant_id,
                source_model_id=candidate.source_model_id,
                target_model_id=candidate.target_model_id,
                edge_kind=candidate.edge_kind,
            )
            if not gate.allowed:
                skipped += 1
                continue
            if await _active_edge_already_exists(
                conn,
                tenant_id=tenant_id,
                source_model_id=candidate.source_model_id,
                target_model_id=candidate.target_model_id,
                edge_kind=candidate.edge_kind,
            ):
                skipped += 1
                continue
            if await _candidate_already_exists(
                conn,
                tenant_id=tenant_id,
                source_model_id=candidate.source_model_id,
                target_model_id=candidate.target_model_id,
                edge_kind=candidate.edge_kind,
                pair_evidence_id=evidence.id,
            ):
                skipped += 1
                continue
            await candidates_repo.insert(conn, candidate)
            inserted += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            errors.append(f"{evidence.id}: {type(exc).__name__}: {exc}")
    return PairEvidencePromotionReport(
        scanned_pair_evidence=len(pair_rows),
        candidates_inserted=inserted,
        candidates_skipped=skipped,
        failed_pair_evidence=failed,
        errors=errors,
    )


async def _candidate_already_exists(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_model_id: UUID | None,
    target_model_id: UUID | None,
    edge_kind: str | None,
    pair_evidence_id: UUID,
) -> bool:
    if source_model_id is None or target_model_id is None or edge_kind is None:
        return True
    found = await conn.fetchval(
        """
        SELECT 1
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND candidate_kind = 'edge'
          AND source_model_id = $2
          AND target_model_id = $3
          AND edge_kind = $4
          AND review_status IN ('candidate', 'needs_review', 'accepted')
          AND (
            metadata->'edge_intelligence'->>'model_pair_evidence_id' = $5
            OR source = 'edge_intelligence_kernel'
          )
        LIMIT 1
        """,
        tenant_id,
        source_model_id,
        target_model_id,
        edge_kind,
        str(pair_evidence_id),
    )
    return found is not None


async def _active_edge_already_exists(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_model_id: UUID | None,
    target_model_id: UUID | None,
    edge_kind: str | None,
) -> bool:
    if source_model_id is None or target_model_id is None or edge_kind is None:
        return True
    found = await conn.fetchval(
        """
        SELECT 1
        FROM model_edges
        WHERE tenant_id = $1
          AND source_model_id = $2
          AND target_model_id = $3
          AND edge_kind = $4
          AND status = 'active'
        LIMIT 1
        """,
        tenant_id,
        source_model_id,
        target_model_id,
        edge_kind,
    )
    return found is not None


__all__ = ["PairEvidencePromotionReport", "promote_pair_evidence_candidates"]
