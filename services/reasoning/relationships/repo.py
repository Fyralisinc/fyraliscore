"""Persistence for relationship-intelligence candidates.

Candidates are deliberately pre-truth: they are ranked hypotheses that
may later become accepted `model_edges` or composite `situation` Models.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError

from .candidates import RelationshipCandidate


_SELECT_COLUMNS = """
  id, tenant_id, candidate_kind, basis, source_model_id, target_model_id,
  edge_kind, member_model_ids, evidence_event_ids, evidence_model_ids,
  counterevidence_model_ids, proposed_proposition, explanation,
  novelty_score, impact_score, actionability_score, urgency_score,
  uncertainty_score, authority_required_score, reversibility_score,
  confidence_score, judgment_leverage_score, source, review_status,
  accepted_model_id, accepted_edge_ids, created_at, decided_at,
  expires_at, metadata
"""


class RelationshipCandidatesRepo:
    async def insert(
        self,
        conn: asyncpg.Connection,
        candidate: RelationshipCandidate,
    ) -> dict[str, Any]:
        record = candidate.to_record()
        row = await conn.fetchrow(
            f"""
            INSERT INTO relationship_candidates (
              id, tenant_id, candidate_kind, basis,
              source_model_id, target_model_id, edge_kind,
              member_model_ids, evidence_event_ids, evidence_model_ids,
              counterevidence_model_ids, proposed_proposition, explanation,
              novelty_score, impact_score, actionability_score, urgency_score,
              uncertainty_score, authority_required_score, reversibility_score,
              confidence_score, judgment_leverage_score, source, review_status,
              metadata
            )
            VALUES (
              $1, $2, $3, $4,
              $5, $6, $7,
              $8::uuid[], $9::uuid[], $10::uuid[],
              $11::uuid[], $12::jsonb, $13,
              $14, $15, $16, $17,
              $18, $19, $20,
              $21, $22, $23, $24,
              $25::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
              review_status = EXCLUDED.review_status,
              metadata = EXCLUDED.metadata
            RETURNING {_SELECT_COLUMNS}
            """,
            record["id"],
            record["tenant_id"],
            record["candidate_kind"],
            record["basis"],
            record["source_model_id"],
            record["target_model_id"],
            record["edge_kind"],
            record["member_model_ids"],
            record["evidence_event_ids"],
            record["evidence_model_ids"],
            record["counterevidence_model_ids"],
            _jsonb(record["proposed_proposition"]),
            record["explanation"],
            record["novelty_score"],
            record["impact_score"],
            record["actionability_score"],
            record["urgency_score"],
            record["uncertainty_score"],
            record["authority_required_score"],
            record["reversibility_score"],
            record["confidence_score"],
            record["judgment_leverage_score"],
            record["source"],
            record["review_status"],
            _jsonb(record["metadata"]),
        )
        if row is None:
            raise ValidationError("relationship candidate insert returned no row")
        return _row_to_dict(row)

    async def insert_many(
        self,
        conn: asyncpg.Connection,
        candidates: Sequence[RelationshipCandidate],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for candidate in candidates:
            out.append(await self.insert(conn, candidate))
        return out

    async def list_for_review(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        statuses: Sequence[str] = ("candidate", "needs_review"),
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT_COLUMNS}
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND review_status = ANY($2::text[])
              AND (expires_at IS NULL OR expires_at > now())
            ORDER BY judgment_leverage_score DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            list(statuses),
            max(1, int(limit)),
        )
        return [_row_to_dict(row) for row in rows]

    async def get(
        self,
        conn: asyncpg.Connection,
        *,
        candidate_id: UUID,
        tenant_id: UUID,
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            f"""
            SELECT {_SELECT_COLUMNS}
            FROM relationship_candidates
            WHERE id = $1
              AND tenant_id = $2
            """,
            candidate_id,
            tenant_id,
        )
        return _row_to_dict(row) if row is not None else None

    async def metrics(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID | None = None,
        since: datetime | None = None,
    ) -> "RelationshipCandidateMetrics":
        """Summarize candidate lifecycle health for observability."""
        clauses: list[str] = []
        args: list[Any] = []
        if tenant_id is not None:
            args.append(tenant_id)
            clauses.append(f"tenant_id = ${len(args)}")
        if since is not None:
            args.append(since)
            clauses.append(f"created_at >= ${len(args)}")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""

        summary = await conn.fetchrow(
            f"""
            SELECT
              COUNT(*)::int AS total,
              COALESCE(AVG(judgment_leverage_score), 0.0)::float AS avg_score,
              COUNT(*) FILTER (
                WHERE expires_at IS NOT NULL AND expires_at <= now()
              )::int AS expired_count,
              COALESCE(MAX(EXTRACT(EPOCH FROM now() - created_at)) FILTER (
                WHERE review_status IN ('candidate', 'needs_review')
              ), 0.0)::float AS oldest_open_age_seconds
            FROM relationship_candidates
            {where}
            """,
            *args,
        )
        status_rows = await conn.fetch(
            f"""
            SELECT review_status, COUNT(*)::int AS n
            FROM relationship_candidates
            {where}
            GROUP BY review_status
            """,
            *args,
        )
        kind_rows = await conn.fetch(
            f"""
            SELECT candidate_kind, COUNT(*)::int AS n
            FROM relationship_candidates
            {where}
            GROUP BY candidate_kind
            """,
            *args,
        )
        source_rows = await conn.fetch(
            f"""
            SELECT source, COUNT(*)::int AS n
            FROM relationship_candidates
            {where}
            GROUP BY source
            """,
            *args,
        )
        return RelationshipCandidateMetrics(
            total=int(summary["total"] if summary else 0),
            avg_score=float(summary["avg_score"] if summary else 0.0),
            expired_count=int(summary["expired_count"] if summary else 0),
            oldest_open_age_seconds=float(
                summary["oldest_open_age_seconds"] if summary else 0.0
            ),
            by_status={r["review_status"]: int(r["n"]) for r in status_rows},
            by_kind={r["candidate_kind"]: int(r["n"]) for r in kind_rows},
            by_source={r["source"]: int(r["n"]) for r in source_rows},
        )

    async def mark_decided(
        self,
        conn: asyncpg.Connection,
        *,
        candidate_id: UUID,
        tenant_id: UUID,
        review_status: str,
        accepted_model_id: UUID | None = None,
        accepted_edge_ids: Sequence[UUID] = (),
        decision_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if review_status not in {
            "accepted",
            "rejected",
            "contested",
            "retired",
            "needs_review",
        }:
            raise ValidationError(
                "invalid relationship candidate review_status",
                review_status=review_status,
            )
        metadata_patch = (
            {"latest_adjudication": decision_metadata}
            if decision_metadata is not None
            else {}
        )
        row = await conn.fetchrow(
            f"""
            UPDATE relationship_candidates
            SET review_status = $3,
                accepted_model_id = $4,
                accepted_edge_ids = $5::uuid[],
                metadata = metadata || $6::jsonb,
                decided_at = CASE
                  WHEN $3 IN ('accepted', 'rejected', 'contested', 'retired')
                  THEN now()
                  ELSE decided_at
                END
            WHERE id = $1
              AND tenant_id = $2
            RETURNING {_SELECT_COLUMNS}
            """,
            candidate_id,
            tenant_id,
            review_status,
            accepted_model_id,
            list(accepted_edge_ids),
            _jsonb(metadata_patch),
        )
        if row is not None:
            await _record_candidate_edge_feedback(
                conn,
                row=row,
                review_status=review_status,
                decision_metadata=decision_metadata,
            )
        return _row_to_dict(row) if row is not None else None


def _jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def _decode_jsonb(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    out = {k: row[k] for k in row.keys()}
    out["proposed_proposition"] = _decode_jsonb(out.get("proposed_proposition"))
    out["metadata"] = _decode_jsonb(out.get("metadata"))
    return out


async def _record_candidate_edge_feedback(
    conn: asyncpg.Connection,
    *,
    row: asyncpg.Record,
    review_status: str,
    decision_metadata: dict[str, Any] | None,
) -> None:
    """Teach the edge kernel from candidate decisions best-effort."""
    if row["candidate_kind"] != "edge":
        return
    source_model_id = row["source_model_id"]
    target_model_id = row["target_model_id"]
    edge_kind = row["edge_kind"]
    if source_model_id is None or target_model_id is None or edge_kind is None:
        return
    try:
        from lib.shared.edge_registry import EDGE_REGISTRY
        from services.reasoning.edge_intelligence.repo import EdgeIntelligenceRepo
        from services.reasoning.edge_intelligence.types import (
            PairEvidenceObservation,
            RelationEvidence,
        )
    except Exception:  # noqa: BLE001
        return

    metadata = _decode_jsonb(row["metadata"])
    primitive = _primitive_from_candidate(edge_kind, metadata)
    decision = decision_metadata if isinstance(decision_metadata, dict) else {}
    is_t4_decision = bool(decision.get("decision_reason"))
    accepted = review_status == "accepted"
    rejected = review_status == "rejected"
    needs_review = review_status == "needs_review"
    if not (accepted or rejected or needs_review):
        return

    spec = EDGE_REGISTRY.get(str(edge_kind))
    directed_source = source_model_id if spec is None or spec.is_directed else None
    directed_target = target_model_id if spec is None or spec.is_directed else None
    repo = EdgeIntelligenceRepo()
    try:
        async with conn.transaction():
            await repo.record_pair_observation(
                conn,
                PairEvidenceObservation(
                    tenant_id=row["tenant_id"],
                    left_model_id=source_model_id,
                    right_model_id=target_model_id,
                    primitive=primitive,
                    t4_accept_delta=1 if accepted and is_t4_decision else 0,
                    t4_reject_delta=1 if rejected and is_t4_decision else 0,
                    no_edge_delta=1 if rejected else 0,
                    positive_outcome_delta=1 if accepted else 0,
                    negative_outcome_delta=1 if rejected else 0,
                    directed_source_model_id=directed_source,
                    directed_target_model_id=directed_target,
                    edge_kind_hint=str(edge_kind),
                    metadata={
                        "relationship_candidate_id": str(row["id"]),
                        "candidate_review_status": review_status,
                        "decision": decision,
                    },
                ),
            )
            if accepted:
                await repo.insert_relation_evidence(
                    conn,
                    RelationEvidence(
                        tenant_id=row["tenant_id"],
                        source_model_id=source_model_id,
                        target_model_id=target_model_id,
                        predicate=str(edge_kind),
                        edge_kind_hint=str(edge_kind),
                        direction=(
                            "source_to_target"
                            if spec is None or spec.is_directed
                            else "symmetric"
                        ),
                        evidence_text=row["explanation"],
                        confidence=float(row["confidence_score"] or 0.0),
                        extraction_method="relationship_candidate_decision",
                        metadata={
                            "relationship_candidate_id": str(row["id"]),
                            "review_status": review_status,
                            "decision": decision,
                        },
                    ),
                )
    except Exception:  # noqa: BLE001
        return


def _primitive_from_candidate(edge_kind: str, metadata: Any) -> str:
    if isinstance(metadata, dict):
        edge_intel = metadata.get("edge_intelligence")
        if isinstance(edge_intel, dict) and edge_intel.get("primitive"):
            return str(edge_intel["primitive"]).upper()
        lifecycle = metadata.get("candidate_lifecycle")
        if isinstance(lifecycle, dict) and lifecycle.get("primitive"):
            return str(lifecycle["primitive"]).upper()
    return {
        "blocks": "DEPENDENCY",
        "enables": "ENABLEMENT",
        "weakens": "COUNTEREVIDENCE",
        "contradicts": "COUNTEREVIDENCE",
        "same_issue_as": "RECURRENCE",
        "supports": "GOAL_IMPACT",
        "early_warning_for": "PREDICTION",
        "predicts": "PREDICTION",
        "causes": "CAUSAL",
        "explains": "EXPLANATION",
        "contributes_to_resolution": "RESOLUTION",
    }.get(edge_kind, "UNKNOWN")


@dataclass(frozen=True)
class RelationshipCandidateMetrics:
    total: int = 0
    avg_score: float = 0.0
    expired_count: int = 0
    oldest_open_age_seconds: float = 0.0
    by_status: dict[str, int] = field(default_factory=dict)
    by_kind: dict[str, int] = field(default_factory=dict)
    by_source: dict[str, int] = field(default_factory=dict)

    @property
    def accepted(self) -> int:
        return self.by_status.get("accepted", 0)

    @property
    def rejected(self) -> int:
        return self.by_status.get("rejected", 0)

    @property
    def needs_review(self) -> int:
        return self.by_status.get("needs_review", 0)

    @property
    def open_count(self) -> int:
        return self.by_status.get("candidate", 0) + self.needs_review

    @property
    def decided_count(self) -> int:
        return self.accepted + self.rejected + self.by_status.get("retired", 0)

    @property
    def acceptance_rate(self) -> float:
        decided = self.accepted + self.rejected
        if decided <= 0:
            return 0.0
        return self.accepted / decided

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "avg_score": self.avg_score,
            "expired_count": self.expired_count,
            "oldest_open_age_seconds": self.oldest_open_age_seconds,
            "by_status": dict(self.by_status),
            "by_kind": dict(self.by_kind),
            "by_source": dict(self.by_source),
            "open_count": self.open_count,
            "decided_count": self.decided_count,
            "acceptance_rate": self.acceptance_rate,
        }


__all__ = ["RelationshipCandidateMetrics", "RelationshipCandidatesRepo"]
