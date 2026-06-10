"""Aggregate ontology-gap edge-type candidates into reviewable proposals."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


_EDGE_KIND_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


@dataclass(slots=True)
class RelationshipOntologyProposal:
    tenant_id: UUID
    proposed_edge_kind: str
    status: str
    description: str = ""
    relationship_summary: str = ""
    parent_kind: str | None = None
    nearest_existing_kind: str | None = None
    retrieval_fallback_kind: str | None = None
    directionality: str = "unknown"
    dropped_dimensions: tuple[str, ...] = ()
    example_candidate_ids: tuple[UUID, ...] = ()
    example_count: int = 0
    evidence_model_ids: tuple[UUID, ...] = ()
    evidence_event_ids: tuple[UUID, ...] = ()
    promotion_criteria: dict[str, Any] = field(default_factory=dict)
    facets: dict[str, Any] = field(default_factory=dict)
    avg_judgment_leverage_score: float = 0.0
    max_judgment_leverage_score: float = 0.0
    confidence_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value


def _row_dict(row: asyncpg.Record | dict[str, Any]) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return {k: row[k] for k in row.keys()}


def normalize_proposed_edge_kind(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not _EDGE_KIND_RE.match(text):
        return None
    return text


def _strings(values: Iterable[Any]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


def _uuids(values: Iterable[Any]) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        try:
            parsed = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError):
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return tuple(out)


def _proposal_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    payload = _decode_json(candidate.get("proposed_proposition"))
    return payload if isinstance(payload, dict) else {}


def _metadata(candidate: dict[str, Any]) -> dict[str, Any]:
    metadata = _decode_json(candidate.get("metadata"))
    return metadata if isinstance(metadata, dict) else {}


def _pick_text(values: Iterable[Any]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def aggregate_edge_type_candidates(
    candidates: Sequence[asyncpg.Record | dict[str, Any]],
    *,
    minimum_distinct_examples: int = 3,
) -> list[RelationshipOntologyProposal]:
    """Group edge-type candidates by proposed kind into proposal rows."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in candidates:
        candidate = _row_dict(raw)
        if candidate.get("candidate_kind") != "edge_type":
            continue
        payload = _proposal_payload(candidate)
        proposed = normalize_proposed_edge_kind(payload.get("proposed_edge_kind"))
        if proposed is None:
            continue
        grouped.setdefault(proposed, []).append(candidate)

    proposals: list[RelationshipOntologyProposal] = []
    for proposed, rows in sorted(grouped.items()):
        payloads = [_proposal_payload(row) for row in rows]
        metadatas = [_metadata(row) for row in rows]
        ontology_gaps = [
            md.get("ontology_gap")
            for md in metadatas
            if isinstance(md.get("ontology_gap"), dict)
        ]
        example_candidate_ids = _uuids(row.get("id") for row in rows)
        evidence_model_ids = _uuids(
            value
            for row in rows
            for value in (row.get("evidence_model_ids") or [])
        )
        evidence_event_ids = _uuids(
            value
            for row in rows
            for value in (row.get("evidence_event_ids") or [])
        )
        dropped_dimensions = _strings(
            value
            for payload in payloads
            for value in (payload.get("dropped_dimensions") or [])
        )
        scores = [
            float(row.get("judgment_leverage_score") or 0.0)
            for row in rows
        ]
        confidences = [
            float(row.get("confidence_score") or 0.0)
            for row in rows
        ]
        example_count = len(example_candidate_ids)
        status = (
            "review_ready"
            if example_count >= int(minimum_distinct_examples)
            else "draft"
        )
        directionality = _pick_text(
            payload.get("directionality") for payload in payloads
        )
        if directionality not in {"directed", "symmetric", "unknown"}:
            directionality = "unknown"
        fallback = _pick_text(
            [
                *(
                    gap.get("retrieval_fallback_kind")
                    for gap in ontology_gaps
                    if isinstance(gap, dict)
                ),
                *(payload.get("parent_kind") for payload in payloads),
                *(payload.get("nearest_existing_kind") for payload in payloads),
            ]
        ) or None
        proposal = RelationshipOntologyProposal(
            tenant_id=rows[0]["tenant_id"],
            proposed_edge_kind=proposed,
            status=status,
            description=_pick_text(payload.get("description") for payload in payloads),
            relationship_summary=_pick_text(
                payload.get("relationship_summary") for payload in payloads
            ),
            parent_kind=_pick_text(payload.get("parent_kind") for payload in payloads)
            or fallback,
            nearest_existing_kind=_pick_text(
                payload.get("nearest_existing_kind") for payload in payloads
            )
            or fallback,
            retrieval_fallback_kind=fallback,
            directionality=directionality,
            dropped_dimensions=dropped_dimensions,
            example_candidate_ids=example_candidate_ids,
            example_count=example_count,
            evidence_model_ids=evidence_model_ids,
            evidence_event_ids=evidence_event_ids,
            promotion_criteria=dict(
                next(
                    (
                        payload.get("promotion_criteria")
                        for payload in payloads
                        if isinstance(payload.get("promotion_criteria"), dict)
                    ),
                    {},
                )
            )
            or {
                "minimum_distinct_examples": minimum_distinct_examples,
                "requires_human_or_llm_adjudication": True,
                "requires_registry_spec": True,
            },
            facets={
                "dropped_dimensions": list(dropped_dimensions),
                "directionality": directionality,
                "retrieval_fallback_kind": fallback,
            },
            avg_judgment_leverage_score=(
                sum(scores) / len(scores) if scores else 0.0
            ),
            max_judgment_leverage_score=max(scores) if scores else 0.0,
            confidence_score=max(confidences) if confidences else 0.0,
            metadata={
                "source": "edge_type_candidate_aggregation",
                "candidate_count": len(rows),
            },
            first_seen_at=min(
                (row.get("created_at") for row in rows if row.get("created_at")),
                default=None,
            ),
            last_seen_at=max(
                (row.get("created_at") for row in rows if row.get("created_at")),
                default=None,
            ),
        )
        proposals.append(proposal)
    return proposals


class RelationshipOntologyProposalsRepo:
    async def aggregate_from_edge_type_candidates(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        minimum_distinct_examples: int = 3,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT
              id, tenant_id, candidate_kind, review_status,
              proposed_proposition, evidence_model_ids, evidence_event_ids,
              novelty_score, impact_score, actionability_score, urgency_score,
              uncertainty_score, authority_required_score, confidence_score,
              judgment_leverage_score, created_at, metadata
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND candidate_kind = 'edge_type'
              AND review_status IN ('candidate', 'needs_review')
            ORDER BY judgment_leverage_score DESC, created_at DESC
            LIMIT $2
            """,
            tenant_id,
            max(1, int(limit)),
        )
        proposals = aggregate_edge_type_candidates(
            rows,
            minimum_distinct_examples=minimum_distinct_examples,
        )
        out: list[dict[str, Any]] = []
        for proposal in proposals:
            row = await self.upsert(conn, proposal)
            await self.attach_to_candidates(
                conn,
                tenant_id=tenant_id,
                proposal_id=row["id"],
                candidate_ids=proposal.example_candidate_ids,
                retire_examples=row["status"] == "review_ready",
            )
            out.append(row)
        return out

    async def upsert(
        self,
        conn: asyncpg.Connection,
        proposal: RelationshipOntologyProposal,
    ) -> dict[str, Any]:
        row = await conn.fetchrow(
            """
            INSERT INTO relationship_ontology_proposals (
              id, tenant_id, proposed_edge_kind, status,
              description, relationship_summary, parent_kind,
              nearest_existing_kind, retrieval_fallback_kind,
              directionality, dropped_dimensions, example_candidate_ids,
              example_count, evidence_model_ids, evidence_event_ids,
              promotion_criteria, facets, avg_judgment_leverage_score,
              max_judgment_leverage_score, confidence_score, metadata,
              first_seen_at, last_seen_at
            )
            VALUES (
              $1, $2, $3, $4,
              $5, $6, $7,
              $8, $9,
              $10, $11::text[], $12::uuid[],
              $13, $14::uuid[], $15::uuid[],
              $16::jsonb, $17::jsonb, $18,
              $19, $20, $21::jsonb,
              COALESCE($22::timestamptz, now()),
              COALESCE($23::timestamptz, now())
            )
            ON CONFLICT (tenant_id, proposed_edge_kind) DO UPDATE SET
              status = CASE
                WHEN relationship_ontology_proposals.status IN (
                  'accepted', 'rejected', 'superseded'
                )
                  THEN relationship_ontology_proposals.status
                WHEN EXCLUDED.status = 'review_ready'
                  THEN 'review_ready'
                ELSE relationship_ontology_proposals.status
              END,
              description = COALESCE(NULLIF(EXCLUDED.description, ''), relationship_ontology_proposals.description),
              relationship_summary = COALESCE(NULLIF(EXCLUDED.relationship_summary, ''), relationship_ontology_proposals.relationship_summary),
              parent_kind = COALESCE(EXCLUDED.parent_kind, relationship_ontology_proposals.parent_kind),
              nearest_existing_kind = COALESCE(EXCLUDED.nearest_existing_kind, relationship_ontology_proposals.nearest_existing_kind),
              retrieval_fallback_kind = COALESCE(EXCLUDED.retrieval_fallback_kind, relationship_ontology_proposals.retrieval_fallback_kind),
              directionality = CASE
                WHEN EXCLUDED.directionality != 'unknown'
                  THEN EXCLUDED.directionality
                ELSE relationship_ontology_proposals.directionality
              END,
              dropped_dimensions = ARRAY(
                SELECT DISTINCT value
                FROM unnest(
                  relationship_ontology_proposals.dropped_dimensions
                  || EXCLUDED.dropped_dimensions
                ) AS t(value)
                WHERE value IS NOT NULL AND value != ''
              ),
              example_candidate_ids = ARRAY(
                SELECT DISTINCT value
                FROM unnest(
                  relationship_ontology_proposals.example_candidate_ids
                  || EXCLUDED.example_candidate_ids
                ) AS t(value)
              ),
              example_count = GREATEST(
                relationship_ontology_proposals.example_count,
                EXCLUDED.example_count
              ),
              evidence_model_ids = ARRAY(
                SELECT DISTINCT value
                FROM unnest(
                  relationship_ontology_proposals.evidence_model_ids
                  || EXCLUDED.evidence_model_ids
                ) AS t(value)
              ),
              evidence_event_ids = ARRAY(
                SELECT DISTINCT value
                FROM unnest(
                  relationship_ontology_proposals.evidence_event_ids
                  || EXCLUDED.evidence_event_ids
                ) AS t(value)
              ),
              promotion_criteria = relationship_ontology_proposals.promotion_criteria || EXCLUDED.promotion_criteria,
              facets = relationship_ontology_proposals.facets || EXCLUDED.facets,
              avg_judgment_leverage_score = GREATEST(
                relationship_ontology_proposals.avg_judgment_leverage_score,
                EXCLUDED.avg_judgment_leverage_score
              ),
              max_judgment_leverage_score = GREATEST(
                relationship_ontology_proposals.max_judgment_leverage_score,
                EXCLUDED.max_judgment_leverage_score
              ),
              confidence_score = GREATEST(
                relationship_ontology_proposals.confidence_score,
                EXCLUDED.confidence_score
              ),
              metadata = relationship_ontology_proposals.metadata || EXCLUDED.metadata,
              first_seen_at = LEAST(
                relationship_ontology_proposals.first_seen_at,
                EXCLUDED.first_seen_at
              ),
              last_seen_at = GREATEST(
                relationship_ontology_proposals.last_seen_at,
                EXCLUDED.last_seen_at
              ),
              updated_at = now()
            RETURNING *
            """,
            uuid7(),
            proposal.tenant_id,
            proposal.proposed_edge_kind,
            proposal.status,
            proposal.description,
            proposal.relationship_summary,
            proposal.parent_kind,
            proposal.nearest_existing_kind,
            proposal.retrieval_fallback_kind,
            proposal.directionality,
            list(proposal.dropped_dimensions),
            list(proposal.example_candidate_ids),
            proposal.example_count,
            list(proposal.evidence_model_ids),
            list(proposal.evidence_event_ids),
            _jsonb(proposal.promotion_criteria),
            _jsonb(proposal.facets),
            proposal.avg_judgment_leverage_score,
            proposal.max_judgment_leverage_score,
            proposal.confidence_score,
            _jsonb(proposal.metadata),
            proposal.first_seen_at,
            proposal.last_seen_at,
        )
        if row is None:
            raise RuntimeError("relationship ontology proposal upsert returned no row")
        return _proposal_row(row)

    async def attach_to_candidates(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        proposal_id: UUID,
        candidate_ids: Sequence[UUID],
        retire_examples: bool = False,
    ) -> int:
        if not candidate_ids:
            return 0
        review_status_sql = (
            """
              review_status = CASE
                WHEN review_status IN ('candidate', 'needs_review')
                THEN 'retired'
                ELSE review_status
              END,
              decided_at = CASE
                WHEN review_status IN ('candidate', 'needs_review')
                THEN now()
                ELSE decided_at
              END,
            """
            if retire_examples
            else ""
        )
        status = await conn.execute(
            f"""
            UPDATE relationship_candidates
            SET {review_status_sql}
                metadata = metadata || jsonb_build_object(
              'ontology_proposal_id', $3::text
            )
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            list(candidate_ids),
            str(proposal_id),
        )
        return int(status.split()[-1]) if status.startswith("UPDATE ") else 0

    async def get_accepted(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        proposed_edge_kind: str,
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM relationship_ontology_proposals
            WHERE tenant_id = $1
              AND proposed_edge_kind = $2
              AND status = 'accepted'
            """,
            tenant_id,
            proposed_edge_kind,
        )
        return _proposal_row(row) if row is not None else None

    async def review(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        proposal_id: UUID,
        status: str,
        reviewed_by: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any] | None:
        if status not in {"accepted", "rejected", "superseded"}:
            raise ValueError(f"invalid ontology proposal review status {status!r}")
        row = await conn.fetchrow(
            """
            UPDATE relationship_ontology_proposals
            SET status = $3,
                reviewed_at = now(),
                promoted_at = CASE
                  WHEN $3 = 'accepted' THEN COALESCE(promoted_at, now())
                  ELSE promoted_at
                END,
                metadata = metadata || jsonb_build_object(
                  'last_review',
                  jsonb_build_object(
                    'status', $3::text,
                    'reviewed_by', $4::text,
                    'note', $5::text,
                    'reviewed_at', now()
                  )
                ),
                updated_at = now()
            WHERE tenant_id = $1
              AND id = $2
              AND (
                status IN ('draft', 'review_ready')
                OR status = $3
                OR (status = 'accepted' AND $3 = 'superseded')
              )
            RETURNING *
            """,
            tenant_id,
            proposal_id,
            status,
            reviewed_by,
            note,
        )
        return _proposal_row(row) if row is not None else None


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _proposal_row(row: asyncpg.Record) -> dict[str, Any]:
    out = {k: row[k] for k in row.keys()}
    for key in ("promotion_criteria", "facets", "metadata"):
        out[key] = _decode_json(out.get(key))
    return out


__all__ = [
    "RelationshipOntologyProposal",
    "RelationshipOntologyProposalsRepo",
    "aggregate_edge_type_candidates",
    "normalize_proposed_edge_kind",
]
