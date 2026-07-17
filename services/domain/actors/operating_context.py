"""Derived actor operating context.

This module does not create a new Model kind. It summarizes existing
actor-scoped Models, owned commitments, and recent observations so Think
can reason about people as constrained organizational actors without
turning every person into a separate schema product.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from services.domain.projections.repo import ProjectionRecord, ProjectionRepo


_PROJECTIONS = ProjectionRepo()


@dataclass(frozen=True)
class ActorOperatingContext:
    actor_id: UUID
    display_name: str | None = None
    active_model_count: int = 0
    recent_observation_count: int = 0
    active_commitment_count: int = 0
    blocked_commitment_count: int = 0
    constraints: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    support_needs: tuple[str, ...] = ()
    risk_factors: tuple[str, ...] = ()
    relationship_context: tuple[str, ...] = ()
    model_ids: tuple[UUID, ...] = ()
    commitment_ids: tuple[UUID, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": str(self.actor_id),
            "display_name": self.display_name,
            "active_model_count": self.active_model_count,
            "recent_observation_count": self.recent_observation_count,
            "active_commitment_count": self.active_commitment_count,
            "blocked_commitment_count": self.blocked_commitment_count,
            "constraints": list(self.constraints),
            "capabilities": list(self.capabilities),
            "support_needs": list(self.support_needs),
            "risk_factors": list(self.risk_factors),
            "relationship_context": list(self.relationship_context),
            "model_ids": [str(v) for v in self.model_ids],
            "commitment_ids": [str(v) for v in self.commitment_ids],
        }


async def load_actor_operating_context(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_ids: Iterable[UUID],
    max_models_per_actor: int = 8,
    max_commitments_per_actor: int = 8,
    observation_window: timedelta = timedelta(days=14),
    reference_time: datetime | None = None,
) -> list[ActorOperatingContext]:
    ids = tuple(dict.fromkeys(actor_ids))
    if not ids:
        return []
    ref = reference_time or datetime.now(timezone.utc)
    window_start = ref - observation_window

    actor_rows = await conn.fetch(
        """
        SELECT id, display_name
        FROM actors
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(ids),
    )
    names = {r["id"]: r["display_name"] for r in actor_rows}

    model_rows = await conn.fetch(
        """
        SELECT id, "natural", proposition_kind, claim_role,
               abstraction_level, confidence, activation, scope_actors,
               created_at
        FROM accepted_current_models
        WHERE tenant_id = $1
          AND status = 'active'
          AND scope_actors && $2::uuid[]
        ORDER BY activation * confidence DESC, created_at DESC
        LIMIT $3
        """,
        tenant_id,
        list(ids),
        max(1, int(max_models_per_actor) * len(ids)),
    )

    commitment_rows = await conn.fetch(
        """
        SELECT id, title, state, owner_id, due_date, priority, created_at
        FROM commitments
        WHERE tenant_id = $1
          AND owner_id = ANY($2::uuid[])
          AND terminal_at IS NULL
          AND state != 'closed'
        ORDER BY
          CASE WHEN state = 'blocked' THEN 0 ELSE 1 END,
          due_date ASC NULLS LAST,
          priority ASC NULLS LAST,
          created_at DESC
        LIMIT $3
        """,
        tenant_id,
        list(ids),
        max(1, int(max_commitments_per_actor) * len(ids)),
    )

    observation_rows = await conn.fetch(
        """
        SELECT actor_id, count(*) AS n
        FROM observations
        WHERE tenant_id = $1
          AND actor_id = ANY($2::uuid[])
          AND occurred_at >= $3
          AND occurred_at <= $4
          AND kind != 'state_change'
        GROUP BY actor_id
        """,
        tenant_id,
        list(ids),
        window_start,
        ref,
    )
    obs_counts = {r["actor_id"]: int(r["n"]) for r in observation_rows}
    profile_snapshots = await _load_employee_profile_snapshots(
        conn,
        tenant_id=tenant_id,
        actor_ids=ids,
    )

    out: list[ActorOperatingContext] = []
    for actor_id in ids:
        actor_models = [
            r for r in model_rows
            if actor_id in set(r["scope_actors"] or [])
        ][:max_models_per_actor]
        actor_commitments = [
            r for r in commitment_rows
            if r["owner_id"] == actor_id
        ][:max_commitments_per_actor]

        capabilities: list[str] = []
        constraints: list[str] = []
        support_needs: list[str] = []
        risk_factors: list[str] = []
        relationship_context: list[str] = []

        for row in actor_models:
            natural = str(row["natural"] or "").strip()
            if not natural:
                continue
            item = _model_line(row)
            role = str(row["claim_role"] or "")
            text = natural.lower()
            if role == "capability":
                capabilities.append(item)
            elif role == "relation":
                relationship_context.append(item)
            elif role == "concern":
                if _looks_like_support_need(text):
                    support_needs.append(item)
                else:
                    risk_factors.append(item)
            elif role == "pattern":
                constraints.append(item)
            elif _looks_like_constraint(text):
                constraints.append(item)
            elif _looks_like_support_need(text):
                support_needs.append(item)

        blocked = [r for r in actor_commitments if r["state"] == "blocked"]
        if blocked:
            for row in blocked[:3]:
                support_needs.append(
                    f"blocked commitment {row['id']}: {row['title']}"
                )
        if len(actor_commitments) >= 5:
            constraints.append(
                f"owns {len(actor_commitments)} active commitments in current context"
            )

        profile = profile_snapshots.get(actor_id)
        if profile is not None and profile.source_model_ids:
            projected = _projected_context_lists(profile)
            capabilities = projected["capabilities"]
            constraints = projected["constraints"]
            support_needs = [
                *projected["support_needs"],
                *support_needs,
            ]
            risk_factors = projected["risk_factors"]
            relationship_context = projected["relationship_context"]
            profile_model_ids = tuple(profile.source_model_ids)
            active_model_count = len(profile_model_ids)
            model_ids = profile_model_ids[:max_models_per_actor]
        else:
            active_model_count = len(actor_models)
            model_ids = tuple(r["id"] for r in actor_models)

        out.append(
            ActorOperatingContext(
                actor_id=actor_id,
                display_name=names.get(actor_id),
                active_model_count=active_model_count,
                recent_observation_count=obs_counts.get(actor_id, 0),
                active_commitment_count=len(actor_commitments),
                blocked_commitment_count=len(blocked),
                constraints=tuple(dict.fromkeys(constraints[:5])),
                capabilities=tuple(dict.fromkeys(capabilities[:5])),
                support_needs=tuple(dict.fromkeys(support_needs[:5])),
                risk_factors=tuple(dict.fromkeys(risk_factors[:5])),
                relationship_context=tuple(
                    dict.fromkeys(relationship_context[:5])
                ),
                model_ids=model_ids,
                commitment_ids=tuple(r["id"] for r in actor_commitments),
            )
        )
    return out


def summarize_actor_operating_context(
    contexts: list[ActorOperatingContext],
    *,
    max_chars: int = 1800,
) -> str | None:
    if not contexts:
        return None
    lines: list[str] = []
    for ctx in contexts:
        label = ctx.display_name or "actor"
        lines.append(f"- actor {label} ({ctx.actor_id})")
        lines.append(
            "  load: "
            f"{ctx.active_commitment_count} active commitments, "
            f"{ctx.blocked_commitment_count} blocked, "
            f"{ctx.recent_observation_count} recent observations"
        )
        _append_group(lines, "capabilities", ctx.capabilities)
        _append_group(lines, "constraints", ctx.constraints)
        _append_group(lines, "support_needs", ctx.support_needs)
        _append_group(lines, "risk_factors", ctx.risk_factors)
        _append_group(lines, "relationships", ctx.relationship_context)
    summary = "\n".join(lines)
    if len(summary) <= max_chars:
        return summary
    return summary[: max_chars - 3] + "..."


def _append_group(lines: list[str], label: str, values: tuple[str, ...]) -> None:
    if not values:
        return
    lines.append(f"  {label}:")
    for value in values[:3]:
        lines.append(f"    - {value}")


def _model_line(row: asyncpg.Record) -> str:
    return (
        f"model {row['id']} ({row['claim_role'] or row['proposition_kind']}, "
        f"conf={float(row['confidence']):.2f}): {row['natural']}"
    )


async def _load_employee_profile_snapshots(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_ids: tuple[UUID, ...],
) -> dict[UUID, ProjectionRecord]:
    out: dict[UUID, ProjectionRecord] = {}
    for actor_id in actor_ids:
        snapshot = await _PROJECTIONS.get_snapshot(
            conn,
            tenant_id=tenant_id,
            projection_name="employee_profiles",
            subject_key=f"employee:{actor_id}:profile",
        )
        if snapshot is not None:
            out[actor_id] = snapshot
    return out


def _projected_context_lists(profile: ProjectionRecord) -> dict[str, list[str]]:
    facets = profile.payload.get("facets")
    if not isinstance(facets, dict):
        facets = {}
    return {
        "capabilities": _profile_facet_lines(facets, "capabilities"),
        "constraints": [
            *_profile_facet_lines(facets, "patterns"),
            *_profile_facet_lines(facets, "constraints"),
        ],
        "support_needs": _profile_facet_lines(facets, "support_needs"),
        "risk_factors": _profile_facet_lines(facets, "risk_factors"),
        "relationship_context": [
            *_profile_facet_lines(facets, "work_style"),
            *_profile_facet_lines(facets, "relationships"),
        ],
    }


def _profile_facet_lines(
    facets: dict[str, Any],
    name: str,
    *,
    limit: int = 5,
) -> list[str]:
    values = facets.get(name)
    if not isinstance(values, list):
        return []
    return [
        line
        for line in (_profile_card_line(card) for card in values[:limit])
        if line
    ]


def _profile_card_line(card: Any) -> str | None:
    if not isinstance(card, dict):
        return None
    natural = str(card.get("natural") or "").strip()
    model_id = str(card.get("model_id") or "").strip()
    role = str(card.get("claim_role") or "model").strip()
    try:
        confidence = float(card.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    if not natural:
        return None
    if model_id:
        return f"model {model_id} ({role}, conf={confidence:.2f}): {natural}"
    return f"{role} (conf={confidence:.2f}): {natural}"


def _looks_like_constraint(text: str) -> bool:
    return any(
        token in text
        for token in (
            "blocked",
            "overload",
            "overloaded",
            "capacity",
            "waiting on",
            "depends on",
            "ambiguous",
            "unclear owner",
            "ownership",
            "stuck",
            "delay",
        )
    )


def _looks_like_support_need(text: str) -> bool:
    return any(
        token in text
        for token in (
            "needs support",
            "needs help",
            "needs decision",
            "needs clarity",
            "needs owner",
            "needs ownership",
            "ownership clarity",
            "waiting on",
            "blocked",
            "unblock",
            "approval",
            "escalat",
        )
    )


__all__ = [
    "ActorOperatingContext",
    "load_actor_operating_context",
    "summarize_actor_operating_context",
]
