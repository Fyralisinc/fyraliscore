"""
services/recommendations/repo.py — read-side query for the action list,
plus the act / dismiss state-change handlers.

The recommendation surface is intentionally thin: a single ranker query
joined to the relevant Acts/Resources tables to denormalize the
target entity, plus two write handlers that archive the recommendation
Model alongside (in the act case) applying its `proposed_change` via
the existing Acts modification services.

Ranker: `(coalesce(expected_impact, 0) * confidence) DESC, created_at DESC`.
The choice of `0` for missing impact pushes qualitative-only
recommendations to the bottom on ties — they aren't down-weighted by
fiat, just unable to compete on the numeric axis. v1 ships with this;
a learned ranker can replace it in v1.1.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError


# ---------------------------------------------------------------------
# Output shape — denormalized for the action-list UI.
# ---------------------------------------------------------------------


@dataclass
class TargetEntitySummary:
    """Resolved snapshot of the entity a recommendation acts on."""
    type: str            # goal | commitment | decision | resource
    id: UUID
    title: str           # for resources, the `identity` field
    state: str | None    # commitments/goals/decisions; None for resources
    archived: bool


@dataclass
class RecommendationView:
    """One row in `GET /v1/recommendations`."""
    id: UUID
    proposition_text: str       # the Model.natural sentence
    confidence: float
    target_act_ref: dict[str, Any] | None
    proposed_change: dict[str, Any]
    expected_impact: float | None
    qualitative_impact: str | None
    target_actor_id: UUID
    supporting_event_ids: list[UUID]
    supporting_model_ids: list[UUID]
    created_at: datetime
    scope_entities: list[dict[str, Any]]
    target_entity: TargetEntitySummary | None
    rank_score: float           # impact * confidence; ties broken on created_at DESC
    proposition_kind: str | None = None


class RecommendationsRepoError(CompanyOSError):
    default_code = "recommendations_repo_error"


# ---------------------------------------------------------------------
# List ranker
# ---------------------------------------------------------------------


_LIST_SQL = """
SELECT
    m.id,
    m.proposition,
    m."natural"               AS natural,
    m.confidence,
    m.proposition_kind,
    m.target_actor_id,
    m.supporting_event_ids,
    m.supporting_model_ids,
    m.created_at,
    m.scope_entities
FROM models m
WHERE m.tenant_id           = $1
  AND m.proposition_kind    = 'recommendation'
  AND (m.target_actor_id = $2 OR m.target_actor_id IS NULL)
  AND m.status              = 'active'
  AND m.archived_at IS NULL
  -- Drop recommendations whose target commitment is already paused
  -- or blocked. These cards represented a "CEO should unblock X"
  -- ask; once the commitment is in a non-active state the system
  -- has already absorbed the block, so the card is stale.
  AND NOT EXISTS (
      SELECT 1 FROM commitments c
      WHERE c.tenant_id = m.tenant_id
        AND m.proposition -> 'target_act_ref' ->> 'type' = 'commitment'
        AND m.proposition -> 'target_act_ref' ->> 'id'
            ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        AND c.id::text = m.proposition -> 'target_act_ref' ->> 'id'
        AND c.state IN ('paused', 'blocked')
  )
ORDER BY (
    COALESCE((m.proposition->>'expected_impact')::float, 0)
    * m.confidence
) DESC,
m.created_at DESC
LIMIT $3
"""


_REF_TYPE_TO_TABLE: dict[str, str] = {
    "goal": "goals",
    "commitment": "commitments",
    "decision": "decisions",
    "resource": "resources",
}


async def list_for_actor(
    *,
    tenant_id: UUID,
    target_actor_id: UUID,
    limit: int = 15,
    conn: asyncpg.Connection,
) -> list[RecommendationView]:
    """
    Return ranked active recommendations for one actor.

    Side effect: the target entity for each recommendation is fetched
    and denormalized. Rows whose target entity has been archived
    (Commitment closed, Decision archived, Resource archived) are
    filtered out — the recommendation is moot once the underlying
    Act is gone, even if the recommendation Model itself hasn't been
    archived yet (background workers will catch up).
    """
    rows = await conn.fetch(_LIST_SQL, tenant_id, target_actor_id, max(int(limit), 1))
    if not rows:
        return []

    # Group target_act_refs by entity type so we can do at most 4 batch
    # fetches (one per Acts/Resources table) instead of N queries.
    # Rows with null target_act_ref (organic T2 recommendations) are kept
    # with ref=None and shown without a target entity.
    by_type: dict[str, list[UUID]] = {}
    parsed: list[tuple[asyncpg.Record, dict[str, Any], dict[str, Any] | None]] = []
    for r in rows:
        proposition = _coerce_jsonb(r["proposition"])
        raw_ref = (proposition or {}).get("target_act_ref")
        if not raw_ref:
            # Organic recommendation — no specific act target.
            parsed.append((r, proposition, None))
            continue
        ref_type = raw_ref.get("type")
        ref_id_raw = raw_ref.get("id")
        if ref_type not in _REF_TYPE_TO_TABLE or not ref_id_raw:
            # Malformed recommendation; skip rather than crash the list.
            continue
        try:
            ref_id = UUID(str(ref_id_raw))
        except (ValueError, TypeError):
            continue
        by_type.setdefault(ref_type, []).append(ref_id)
        parsed.append((r, proposition, {**raw_ref, "id": ref_id}))

    target_index: dict[tuple[str, UUID], TargetEntitySummary] = {}
    for ref_type, ids in by_type.items():
        # Per-kind SELECT — goals/decisions/resources carry archived_at
        # while commitments use a state machine (terminal_at) instead.
        # Resources have `identity` not `title`.
        if ref_type == "resource":
            sql = (
                "SELECT id, identity AS title, NULL::text AS state, "
                "archived_at "
                "FROM resources WHERE id = ANY($1::uuid[]) AND tenant_id = $2"
            )
        elif ref_type == "commitment":
            sql = (
                "SELECT id, title, state, NULL::timestamptz AS archived_at "
                "FROM commitments WHERE id = ANY($1::uuid[]) AND tenant_id = $2"
            )
        elif ref_type == "goal":
            sql = (
                "SELECT id, title, state, archived_at "
                "FROM goals WHERE id = ANY($1::uuid[]) AND tenant_id = $2"
            )
        else:  # decision
            sql = (
                "SELECT id, title, state, archived_at "
                "FROM decisions WHERE id = ANY($1::uuid[]) AND tenant_id = $2"
            )
        entity_rows = await conn.fetch(sql, ids, tenant_id)
        for er in entity_rows:
            target_index[(ref_type, er["id"])] = TargetEntitySummary(
                type=ref_type,
                id=er["id"],
                title=er["title"],
                state=er["state"],
                archived=er["archived_at"] is not None,
            )

    out: list[RecommendationView] = []
    for r, proposition, ref in parsed:
        if ref is not None:
            # Filter out recommendations whose target was archived OR whose
            # target Commitment reached a terminal state (closed/doneverified).
            target: TargetEntitySummary | None = target_index.get((ref["type"], ref["id"]))
            if target is None:
                continue
            if target.archived:
                continue
            if ref["type"] == "commitment" and target.state in ("closed", "doneverified"):
                continue
            target_act_ref_out: dict[str, Any] | None = {"type": ref["type"], "id": str(ref["id"])}
        else:
            target = None
            target_act_ref_out = None

        ei_raw = proposition.get("expected_impact")
        try:
            ei: float | None = float(ei_raw) if ei_raw is not None else None
        except (TypeError, ValueError):
            ei = None
        rank_score = (ei if ei is not None else 0.0) * float(r["confidence"])

        out.append(
            RecommendationView(
                id=r["id"],
                proposition_text=r["natural"],
                confidence=float(r["confidence"]),
                target_act_ref=target_act_ref_out,
                proposed_change=proposition.get("proposed_change") or {},
                expected_impact=ei,
                qualitative_impact=proposition.get("qualitative_impact"),
                target_actor_id=r["target_actor_id"],
                supporting_event_ids=list(r["supporting_event_ids"] or []),
                supporting_model_ids=list(r["supporting_model_ids"] or []),
                created_at=r["created_at"],
                scope_entities=_coerce_jsonb_list(r["scope_entities"]),
                target_entity=target,
                rank_score=rank_score,
                proposition_kind=r["proposition_kind"],
            )
        )

    return out


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _coerce_jsonb(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        import json
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _coerce_jsonb_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


__all__ = [
    "RecommendationView",
    "TargetEntitySummary",
    "RecommendationsRepoError",
    "list_for_actor",
]
