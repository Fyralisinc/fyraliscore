"""services/today/map.py — substrate → Map (Tier 2 redesign).

Three asymmetric rows, each answering a single CEO question:

  * DECIDE   — what requires me, sized by stake (top 3 from recommendations)
  * MOVED    — what changed since I last looked (named actors, sentences)
  * HANDLED  — what the system did for me while I was away (signal counts)

Every line names a real actor (commitment / model / resource / goal).
Counts are explicitly receipt-shaped ("processed N · escalated M") not
dashboards. The Map is the index; the cards are the chapters.

This module is intentionally chatty in sentence form — the frontend
renders `sentence_html` with the actor in serif italic via `<em>`.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from services.recommendations.repo import RecommendationView


_DECIDE_SEVERITY = ("critical", "strategic", "high")
_MOVED_LIMIT = 5
_DECIDE_LIMIT = 3
_DEFAULT_WINDOW_HOURS = 18
_DOWN_COMMITMENT_STATES = ("blocked", "paused", "stalled")
_UP_COMMITMENT_STATES = ("active", "in_progress", "doneverified")
_DOWN_GOAL_STATES = ("paused", "archived", "abandoned")


async def build_map(
    *,
    tenant_id: UUID,
    actor_id: UUID,
    previous_last_seen_at: datetime | None,
    cards: list[dict[str, Any]],
    recommendations: list[RecommendationView],
    now: datetime,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    """Compose the three-row Map payload."""
    decide = _build_decide(cards)
    moved = await _build_moved(
        tenant_id=tenant_id,
        previous_last_seen_at=previous_last_seen_at,
        now=now,
        conn=conn,
    )
    handled = await _build_handled(
        tenant_id=tenant_id,
        actor_id=actor_id,
        previous_last_seen_at=previous_last_seen_at,
        now=now,
        conn=conn,
    )
    return {
        "decide": decide,
        "moved": moved,
        "handled": handled,
        "total_moved": len(moved),
    }


# ---------------------------------------------------------------------
# DECIDE — top decisions owed to the user, by stake
# ---------------------------------------------------------------------


def _build_decide(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for c in cards:
        sev = c.get("severity")
        if sev not in _DECIDE_SEVERITY:
            continue
        rank = _decide_rank(c)
        candidates.append((rank, c))
    candidates.sort(key=lambda t: t[0], reverse=True)

    out: list[dict[str, Any]] = []
    for _, c in candidates[:_DECIDE_LIMIT]:
        sentence = _decide_sentence(c)
        if not sentence:
            continue
        out.append({
            "id": c["id"],
            "sentence_html": sentence,
            "drill_card_id": c["id"],
        })
    return out


def _decide_rank(card: dict[str, Any]) -> int:
    stake = card.get("stake") or {}
    if stake.get("unit") == "usd":
        return int(stake.get("value") or 0)
    if stake.get("unit") == "risk":
        return int(stake.get("value") or 0) * 1_000_000
    sev = card.get("severity")
    return {"critical": 750_000, "strategic": 500_000, "high": 250_000}.get(sev, 0)


def _decide_sentence(card: dict[str, Any]) -> str:
    head = (card.get("headline_html") or "").strip()
    if not head:
        return ""
    suffix: list[str] = []
    stake = card.get("stake")
    headline_has_dollar = "$" in head
    if isinstance(stake, dict):
        # Only append a stake suffix when the headline doesn't already
        # mention the money. Otherwise we render the same dollar twice
        # ("$487K · $487K").
        if stake.get("unit") == "usd" and not headline_has_dollar:
            suffix.append(f"${_humanize_usd(stake.get('value') or 0)}")
        elif stake.get("unit") == "risk":
            v = int(stake.get("value") or 0)
            suffix.append(f"risk {v}/3")
    meta = (card.get("meta") or "").strip()
    if meta and ("min" in meta or "ratify" in meta or "decide" in meta.lower()):
        suffix.append(meta)
    if suffix:
        return f"{head} · {' · '.join(suffix)}"
    return head


# ---------------------------------------------------------------------
# MOVED — named state changes since the previous visit
# ---------------------------------------------------------------------


async def _build_moved(
    *,
    tenant_id: UUID,
    previous_last_seen_at: datetime | None,
    now: datetime,
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    if previous_last_seen_at is None:
        return []
    out: list[dict[str, Any]] = []

    # commitments — state shifts
    rows = await conn.fetch(
        """
        SELECT id, title, state, last_state_change_at
        FROM commitments
        WHERE tenant_id = $1
          AND last_state_change_at > $2
        ORDER BY last_state_change_at DESC
        LIMIT 10
        """,
        tenant_id, previous_last_seen_at,
    )
    for r in rows:
        state = (r["state"] or "").lower()
        if state in _DOWN_COMMITMENT_STATES:
            delta = "down"
        elif state in _UP_COMMITMENT_STATES:
            delta = "up"
        else:
            delta = "up"
        sentence = (
            f"<em>{_escape(r['title'] or 'untitled commitment')}</em> "
            f"moved to {_escape(state) or 'a new state'}"
        )
        out.append({
            "id": str(r["id"]),
            "sentence_html": sentence,
            "delta": delta,
        })

    # goals — state shifts
    rows = await conn.fetch(
        """
        SELECT id, title, state, last_state_change_at, cached_health
        FROM goals
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND last_state_change_at > $2
        ORDER BY last_state_change_at DESC
        LIMIT 10
        """,
        tenant_id, previous_last_seen_at,
    )
    for r in rows:
        state = (r["state"] or "").lower()
        delta = "down" if state in _DOWN_GOAL_STATES else "up"
        sentence = (
            f"<em>{_escape(r['title'] or 'untitled goal')}</em> "
            f"is now {_escape(state) or 'active'}"
        )
        out.append({
            "id": str(r["id"]),
            "sentence_html": sentence,
            "delta": delta,
        })

    # models — newly created (excluding recommendations themselves)
    rows = await conn.fetch(
        """
        SELECT id, "natural" AS natural, created_at, confidence
        FROM models
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND status = 'active'
          AND proposition_kind <> 'recommendation'
          AND created_at > $2
        ORDER BY created_at DESC
        LIMIT 5
        """,
        tenant_id, previous_last_seen_at,
    )
    for r in rows:
        nat = (r["natural"] or "new belief").strip()
        nat = nat if len(nat) <= 70 else nat[:67].rstrip() + "…"
        sentence = f"New belief: <em>{_escape(nat)}</em>"
        out.append({
            "id": str(r["id"]),
            "sentence_html": sentence,
            "delta": "up",
        })

    # customer resources — updates
    rows = await conn.fetch(
        """
        SELECT id, identity, current_value, last_updated_at
        FROM resources
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND kind = ANY($2::text[])
          AND last_updated_at > $3
        ORDER BY last_updated_at DESC
        LIMIT 5
        """,
        tenant_id, ["customer", "relational", "account", "organization"],
        previous_last_seen_at,
    )
    for r in rows:
        cv = _coerce_jsonb(r["current_value"])
        health = (cv.get("health") if isinstance(cv, dict) else None) or None
        if isinstance(health, str) and health.lower() in ("warning", "critical"):
            sentence = (
                f"<em>{_escape(r['identity'] or 'customer')}</em> "
                f"health → {_escape(health.lower())}"
            )
            delta = "down"
        else:
            sentence = f"<em>{_escape(r['identity'] or 'customer')}</em> updated"
            delta = "up"
        out.append({
            "id": str(r["id"]),
            "sentence_html": sentence,
            "delta": delta,
        })

    # cap to MOVED_LIMIT, prefer downs first then most recent
    out.sort(key=lambda d: (0 if d["delta"] == "down" else 1,))
    return out[:_MOVED_LIMIT]


# ---------------------------------------------------------------------
# HANDLED — signals processed + escalation receipt
# ---------------------------------------------------------------------


async def _build_handled(
    *,
    tenant_id: UUID,
    actor_id: UUID,
    previous_last_seen_at: datetime | None,
    now: datetime,
    conn: asyncpg.Connection,
) -> dict[str, Any] | None:
    if previous_last_seen_at is not None:
        window_start = previous_last_seen_at
        window_label = _humanize_window(now - previous_last_seen_at)
    else:
        window_start = now - timedelta(hours=_DEFAULT_WINDOW_HOURS)
        window_label = f"in the last {_DEFAULT_WINDOW_HOURS}h"

    signals = await conn.fetchval(
        """
        SELECT count(*) FROM observations
        WHERE tenant_id = $1 AND ingested_at >= $2
        """,
        tenant_id, window_start,
    ) or 0

    escalated = await conn.fetchval(
        """
        SELECT count(*) FROM models
        WHERE tenant_id = $1
          AND proposition_kind = 'recommendation'
          AND (target_actor_id = $2 OR target_actor_id IS NULL)
          AND created_at >= $3
        """,
        tenant_id, actor_id, window_start,
    ) or 0

    signals = int(signals)
    escalated = int(escalated)
    if signals == 0 and escalated == 0:
        return None
    absorbed = max(signals - escalated, 0)
    return {
        "signals_processed": signals,
        "escalated": escalated,
        "absorbed": absorbed,
        "window_label": window_label,
    }


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _humanize_usd(v: float | int) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "0"
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.1f}B".rstrip("0").rstrip(".")
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M".rstrip("0").rstrip(".")
    if v >= 1_000:
        return f"{int(round(v / 1_000))}K"
    return f"{int(round(v))}"


def _humanize_window(delta: timedelta) -> str:
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"since {minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"in the last {hours}h"
    days = hours // 24
    return f"in the last {days}d"


def _escape(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _coerce_jsonb(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


__all__ = ["build_map"]
