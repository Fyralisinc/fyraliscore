"""Deterministic consequence preview for recommendation actions."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg


async def build_consequence_preview(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_act_ref: dict[str, Any] | None,
    proposed_change: dict[str, Any] | None,
) -> dict[str, Any]:
    if not target_act_ref:
        return _empty_preview("no_target")
    target_type = str(target_act_ref.get("type") or "")
    try:
        target_id = UUID(str(target_act_ref.get("id")))
    except (TypeError, ValueError):
        return _empty_preview("invalid_target")

    proposed_change = proposed_change or {}
    operation = proposed_change.get("operation")
    payload = proposed_change.get("payload") or {}
    preview: dict[str, Any] = {
        "target": {"type": target_type, "id": str(target_id)},
        "operation": operation,
        "affected_commitments": [],
        "affected_goals": [],
        "affected_decisions": [],
        "linked_predictions": [],
        "cascade_warnings": [],
    }

    if target_type == "commitment":
        await _commitment_preview(conn, tenant_id, target_id, payload, preview)
    elif target_type == "goal":
        await _goal_preview(conn, tenant_id, target_id, preview)
    elif target_type == "decision":
        await _decision_preview(conn, tenant_id, target_id, payload, preview)

    preview["linked_predictions"] = await _linked_predictions(
        conn,
        tenant_id=tenant_id,
        target_type=target_type,
        target_id=target_id,
    )
    preview["impact_count"] = (
        len(preview["affected_commitments"])
        + len(preview["affected_goals"])
        + len(preview["affected_decisions"])
        + len(preview["linked_predictions"])
    )
    return preview


async def _commitment_preview(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    commitment_id: UUID,
    payload: dict[str, Any],
    preview: dict[str, Any],
) -> None:
    dependents = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, 'depends_on' AS relation
        FROM depends_on d
        JOIN commitments c
          ON c.id = d.dependent_commitment_id
         AND c.tenant_id = $1
        WHERE d.dependency_commitment_id = $2
        ORDER BY c.priority ASC, c.created_at DESC
        LIMIT 12
        """,
        tenant_id,
        commitment_id,
    )
    preview["affected_commitments"].extend(_act_rows(dependents))

    goals = await conn.fetch(
        """
        SELECT g.id, g.title, g.state, 'contributes_to' AS relation
        FROM contributes_to ct
        JOIN goals g
          ON g.id = ct.goal_id
         AND g.tenant_id = $1
        WHERE ct.commitment_id = $2
        ORDER BY ct.is_critical_path DESC, g.created_at DESC
        LIMIT 8
        """,
        tenant_id,
        commitment_id,
    )
    preview["affected_goals"].extend(_act_rows(goals))

    decisions = await conn.fetch(
        """
        SELECT d.id, d.title, d.state, 'constrained_by' AS relation
        FROM constrained_by cb
        JOIN decisions d
          ON d.id = cb.decision_id
         AND d.tenant_id = $1
        WHERE cb.commitment_id = $2
        ORDER BY d.created_at DESC
        LIMIT 8
        """,
        tenant_id,
        commitment_id,
    )
    preview["affected_decisions"].extend(_act_rows(decisions))

    new_state = payload.get("new_state") or payload.get("state")
    if new_state in {"paused", "blocked", "closed"} and dependents:
        preview["cascade_warnings"].append({
            "kind": "dependent_commitments_may_block",
            "count": len(dependents),
        })


async def _goal_preview(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    goal_id: UUID,
    preview: dict[str, Any],
) -> None:
    commitments = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, 'contributes_to' AS relation
        FROM contributes_to ct
        JOIN commitments c
          ON c.id = ct.commitment_id
         AND c.tenant_id = $1
        WHERE ct.goal_id = $2
        ORDER BY ct.is_critical_path DESC, c.priority ASC, c.created_at DESC
        LIMIT 12
        """,
        tenant_id,
        goal_id,
    )
    preview["affected_commitments"].extend(_act_rows(commitments))
    if commitments:
        preview["cascade_warnings"].append({
            "kind": "goal_change_affects_commitments",
            "count": len(commitments),
        })


async def _decision_preview(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    decision_id: UUID,
    payload: dict[str, Any],
    preview: dict[str, Any],
) -> None:
    constrained = await conn.fetch(
        """
        SELECT c.id, c.title, c.state, 'constrained_by' AS relation
        FROM constrained_by cb
        JOIN commitments c
          ON c.id = cb.commitment_id
         AND c.tenant_id = $1
        WHERE cb.decision_id = $2
        ORDER BY c.priority ASC, c.created_at DESC
        LIMIT 12
        """,
        tenant_id,
        decision_id,
    )
    preview["affected_commitments"].extend(_act_rows(constrained))
    new_state = payload.get("new_state") or payload.get("state")
    if new_state == "revisited" and constrained:
        preview["cascade_warnings"].append({
            "kind": "revisited_decision_may_block_constraints",
            "count": len(constrained),
        })


async def _linked_predictions(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_type: str,
    target_id: UUID,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT id, "natural" AS natural, confidence, evaluate_at
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND claim_role = 'prediction'
          AND scope_entities @> $2::jsonb
        ORDER BY evaluate_at NULLS LAST, confidence DESC
        LIMIT 8
        """,
        tenant_id,
        json.dumps([{"type": target_type, "id": str(target_id)}]),
    )
    return [
        {
            "id": str(row["id"]),
            "natural": row["natural"],
            "confidence": float(row["confidence"] or 0.0),
            "evaluate_at": row["evaluate_at"].isoformat() if row["evaluate_at"] else None,
        }
        for row in rows
    ]


def _act_rows(rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
    return [
        {
            "id": str(row["id"]),
            "title": row["title"],
            "state": row["state"],
            "relation": row["relation"],
        }
        for row in rows
    ]


def _empty_preview(reason: str) -> dict[str, Any]:
    return {
        "target": None,
        "operation": None,
        "affected_commitments": [],
        "affected_goals": [],
        "affected_decisions": [],
        "linked_predictions": [],
        "cascade_warnings": [],
        "impact_count": 0,
        "reason": reason,
    }


__all__ = ["build_consequence_preview"]
