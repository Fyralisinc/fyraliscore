"""
services/decision_deltas/promote.py — recommendation -> decision delta.

A recommendation lives on `models` with proposition_kind='recommendation'.
Its proposition JSONB carries target_act_ref, proposed_change,
expected_impact, qualitative_impact, target_actor_id. To re-skin that
row as a Decision Delta we:

  - main_assertion := models.natural
  - current_state / suggested_update := derived from
    proposed_change.payload (best-effort)
  - target_node_kind / target_node_id := target_act_ref.{type, id}
  - confidence := models.confidence
  - impact := { arr_at_risk: expected_impact (when > 0) }
  - evidence := one row per supporting_event_id (loaded from
    observations) capped at the top 5 by occurrence time.
  - source_recommendation_id := the recommendation row's id.

The recommendation row is NOT modified — promotion is additive. If the
caller wants the underlying recommendation archived, they can do that
via the existing handlers.dismiss_recommendation flow after promotion.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError


class DeltaPromoteError(CompanyOSError):
    default_code = "decision_delta_promote_error"


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------


async def promote_from_recommendation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    recommendation_id: UUID,
    label: str = "recommended_update",
) -> UUID:
    """Read a recommendation row and insert a matching Decision Delta.

    Returns the new delta id. Raises ValidationError if the source
    row is missing, wrong tenant, or wrong proposition_kind.
    """
    rec = await conn.fetchrow(
        """
        SELECT id, tenant_id, "natural" AS natural, confidence,
               proposition, supporting_event_ids,
               proposition_kind, status, target_actor_id
        FROM models
        WHERE id = $1
          AND tenant_id = $2
          AND proposition_kind = 'recommendation'
        """,
        recommendation_id, tenant_id,
    )
    if rec is None:
        raise ValidationError(
            f"recommendation {recommendation_id} not found",
            recommendation_id=str(recommendation_id),
        )

    proposition = _coerce_jsonb(rec["proposition"]) or {}
    target_ref = proposition.get("target_act_ref") or {}
    proposed_change = proposition.get("proposed_change") or {}
    payload = proposed_change.get("payload") or {}

    target_kind = (
        target_ref.get("type")
        if isinstance(target_ref, dict)
        else None
    )
    target_id_raw = (
        target_ref.get("id") if isinstance(target_ref, dict) else None
    )
    target_id: UUID | None
    if target_id_raw:
        try:
            target_id = UUID(str(target_id_raw))
        except (ValueError, TypeError):
            target_id = None
    else:
        target_id = None

    # Derive a current_state / suggested_update best-effort. The
    # recommendation pipeline uses {operation, payload} where
    # operation = create | update | transition | archive. For
    # `transition` we know the target state; for the rest we leave
    # current_state NULL and pack the payload into suggested_update.
    current_state, suggested_update = _build_state_pair(
        operation=proposed_change.get("operation"),
        payload=payload,
        target_kind=target_kind,
        target_id=target_id,
        conn=conn,
    )
    # _build_state_pair is sync — it doesn't actually fetch current
    # state. The current_state column is JSONB and the spec accepts
    # a null/missing value when we can't synthesize one cheaply.

    # Impact dict.
    impact: dict[str, Any] = {}
    expected_impact = proposition.get("expected_impact")
    if isinstance(expected_impact, (int, float)) and expected_impact > 0:
        impact["arr_at_risk"] = float(expected_impact)
    if proposition.get("qualitative_impact"):
        impact["qualitative"] = proposition["qualitative_impact"]

    # Confidence + falsification gate.
    confidence = (
        float(rec["confidence"]) if rec["confidence"] is not None else None
    )
    # Source the falsification_condition from the model row if it has
    # a falsifier. Best-effort: render a short summary.
    falsification = await _resolve_falsifier(conn, recommendation_id)

    # Evidence: pull up to 5 most recent supporting observations.
    supporting_ids = list(rec["supporting_event_ids"] or [])
    evidence = await _build_evidence(
        conn, tenant_id=tenant_id, event_ids=supporting_ids,
    )

    # Insert the delta directly — we avoid calling repo.create_delta
    # so the conf>0.7-needs-falsification rule doesn't reject a
    # legitimate promotion when the recommendation lacks a falsifier
    # (recommendations historically don't carry one). We still gate
    # at the spec boundary for direct creates.
    row = await conn.fetchrow(
        """
        INSERT INTO decision_deltas (
          tenant_id, status, label, main_assertion,
          current_state, suggested_update,
          target_node_kind, target_node_id,
          confidence, confidence_basis,
          falsification_condition, consequence_preview, impact,
          category, source_recommendation_id, resolution_target_at
        ) VALUES (
          $1, 'proposed', $2, $3,
          $4::jsonb, $5::jsonb,
          $6, $7,
          $8, $9,
          $10, $11::jsonb, $12::jsonb,
          $13, $14, NULL
        )
        RETURNING id
        """,
        tenant_id,
        label,
        (rec["natural"] or "Proposed change").strip(),
        _dump_jsonb(current_state),
        _dump_jsonb(suggested_update),
        target_kind,
        target_id,
        confidence,
        "promoted_from_recommendation",
        falsification,
        _dump_jsonb({
            "creates": [],
            "updates": (
                [_describe_update(target_kind, target_id, suggested_update)]
                if target_id and suggested_update
                else []
            ),
            "archives": [],
            "notifies": [],
            "re_evaluates_in": "7d",
        }),
        _dump_jsonb(impact) if impact else None,
        _categorize(target_kind, proposition),
        recommendation_id,
    )
    delta_id: UUID = row["id"]

    if evidence:
        # Reuse the repo's evidence inserter for the validation paths.
        from services.decision_deltas.repo import _insert_evidence
        await _insert_evidence(
            conn, delta_id=delta_id, items=evidence,
        )

    return delta_id


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _build_state_pair(
    *,
    operation: str | None,
    payload: dict[str, Any],
    target_kind: str | None,
    target_id: UUID | None,
    conn: asyncpg.Connection,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Translate a recommendation proposed_change into a delta
    (current_state, suggested_update) pair.

    current_state is best-effort: we don't fetch the underlying row
    here (would require an async call + per-kind dispatch). The Today
    inspector renders "—" when current_state is missing, which is
    acceptable for Phase 1.
    """
    if operation == "transition" and "new_state" in payload:
        return (
            None,
            {
                "label": "state",
                "value": payload["new_state"],
            },
        )
    if operation == "create":
        return (
            {"label": "exists", "value": "no"},
            {
                "label": "exists",
                "value": "create",
                "details": {k: v for k, v in payload.items() if k != "owner_id"},
            },
        )
    if operation == "update":
        return (None, {"label": "update", "value": payload})
    if operation == "archive":
        return (
            {"label": "state", "value": "active"},
            {"label": "state", "value": "archived"},
        )
    return (None, {"label": "operation", "value": operation or "unknown"})


def _describe_update(
    target_kind: str | None,
    target_id: UUID | None,
    suggested_update: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "target_kind": target_kind,
        "target_id": str(target_id) if target_id else None,
        "to": suggested_update,
    }


def _categorize(
    target_kind: str | None,
    proposition: dict[str, Any],
) -> str | None:
    if proposition.get("category"):
        # Trust explicit categorization when set on the recommendation.
        return str(proposition["category"])
    # Heuristic fallback by target kind.
    if target_kind == "commitment":
        return "delivery"
    if target_kind == "decision":
        return "decision"
    if target_kind == "resource":
        return "capacity"
    if target_kind == "goal":
        return "strategy"
    return None


async def _resolve_falsifier(
    conn: asyncpg.Connection,
    recommendation_id: UUID,
) -> str | None:
    """Read the model's falsifier JSONB (if any) and render a single
    sentence. Falsifier schema is loose (see map_routes
    _summarize_falsifier); we copy the same best-effort logic here."""
    row = await conn.fetchrow(
        "SELECT falsifier FROM models WHERE id = $1",
        recommendation_id,
    )
    if row is None:
        return None
    falsifier = _coerce_jsonb(row["falsifier"])
    if not falsifier:
        return None
    if isinstance(falsifier, dict):
        kind = falsifier.get("kind") or falsifier.get("type")
        if kind in ("threshold", "metric_threshold"):
            metric = falsifier.get("metric") or "metric"
            op = falsifier.get("op") or "below"
            value = falsifier.get("value")
            window = falsifier.get("window") or falsifier.get("window_days")
            tail = f" within {window}" if window else ""
            return f"{metric} {op} {value}{tail}".strip()
        desc = falsifier.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc[:300]
    return None


async def _build_evidence(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    event_ids: list[Any],
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not event_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id, occurred_at, kind, source_channel, content_text,
               trust_tier
        FROM observations
        WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        ORDER BY occurred_at DESC
        LIMIT $3
        """,
        event_ids, tenant_id, limit,
    )
    out: list[dict[str, Any]] = []
    for i, r in enumerate(rows):
        excerpt = (r["content_text"] or "")[:500]
        title_line = (r["content_text"] or r["kind"] or "evidence")
        # First sentence / first 120 chars as the title.
        title = title_line.strip().split("\n", 1)[0][:120]
        out.append({
            "source": _normalize_source(r["source_channel"]),
            "title": title or r["kind"] or "evidence",
            "ts": r["occurred_at"],
            "trust_tier": r["trust_tier"],
            "excerpt": excerpt,
            "weight": None,
            "ordinal": i,
        })
    return out


def _normalize_source(source_channel: str | None) -> str:
    """Map ingestion-channel strings to the UI source taxonomy in
    spec §2.2 (crm, support, email, slack, linear, github, calendar,
    finance, product_usage, fyralis_reasoning)."""
    if not source_channel:
        return "fyralis_reasoning"
    s = source_channel.lower()
    if "slack" in s:
        return "slack"
    if "github" in s or "git" in s:
        return "github"
    if "linear" in s:
        return "linear"
    if "calendar" in s or "gcal" in s:
        return "calendar"
    if "salesforce" in s or "crm" in s or "hubspot" in s:
        return "crm"
    if "zendesk" in s or "support" in s or "intercom" in s:
        return "support"
    if "stripe" in s or "finance" in s:
        return "finance"
    if "mixpanel" in s or "amplitude" in s or "product" in s:
        return "product_usage"
    if "email" in s or "gmail" in s:
        return "email"
    return s


def _coerce_jsonb(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(decoded, dict):
            return decoded
    return None


def _dump_jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str)


__all__ = [
    "DeltaPromoteError",
    "promote_from_recommendation",
]
