"""Persistence for relationship-intelligence candidates.

Candidates are deliberately pre-truth: they are ranked hypotheses that
may later become accepted `model_edges` or composite `situation` Models.
"""
from __future__ import annotations

import json
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

    async def mark_decided(
        self,
        conn: asyncpg.Connection,
        *,
        candidate_id: UUID,
        tenant_id: UUID,
        review_status: str,
        accepted_model_id: UUID | None = None,
        accepted_edge_ids: Sequence[UUID] = (),
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
        row = await conn.fetchrow(
            f"""
            UPDATE relationship_candidates
            SET review_status = $3,
                accepted_model_id = $4,
                accepted_edge_ids = $5::uuid[],
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


__all__ = ["RelationshipCandidatesRepo"]
