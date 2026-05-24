"""services/forecasts/page.py — spec-aligned synthesis layer for the
new Forecasts page (v1.0 spec).

The on-disk schema (predictions + prediction_signals from migration
0041) is intentionally thin. The spec's UI surface introduces several
richer objects — driving patterns, leading indicators, falsifiers,
intervention levers, pattern field, foresight brief — that this module
derives from existing data rather than requiring a migration.

Functions:

  - build_page_payload   — initial ForecastsPagePayload for /v1/forecasts/page
  - build_forecast_detail — ForecastDetail for /v1/forecasts/detail/{id}
  - list_patterns        — pattern cards for Pattern Field + Patterns mode
  - handle_ask           — Ask Fyralis stub (template responses keyed by
                            prompt heuristics; can be plumbed to an LLM
                            later without changing the contract)

These derivations are deterministic — given the same predictions /
signals rows we always produce the same brief, indicators, levers, etc.
That makes the demo seed reproducible and the test surface trivial.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from services.forecasts import accuracy as accuracy_mod
from services.forecasts import repo as repo_mod
from services.forecasts.repo import PredictionRow, PredictionSignal


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Domain + horizon mapping
# ---------------------------------------------------------------------

# The spec exposes five business-readable domains in the Horizon
# Matrix. We map our seven internal categories down to these — keep
# the order stable so the matrix rows render predictably.
DOMAINS: tuple[tuple[str, str], ...] = (
    ("customers_revenue",     "Customers & Revenue"),
    ("commitments_delivery",  "Commitments & Delivery"),
    ("systems_capacity",      "Systems & Capacity"),
    ("people_ownership",      "People & Ownership"),
    ("finance_capital",       "Finance & Capital"),
)

_CATEGORY_TO_DOMAIN: dict[str, str] = {
    "customer_risk": "customers_revenue",
    "partner":       "customers_revenue",
    "delivery":      "commitments_delivery",
    "capacity":      "systems_capacity",
    "decision":      "people_ownership",
    "pricing":       "finance_capital",
    "strategy":      "finance_capital",
}

HORIZONS: tuple[tuple[str, str, int, int], ...] = (
    ("next_14_days", "Next 14 days",   0, 14),
    ("days_15_45",   "15–45 days",    15, 45),
    ("days_46_90",   "46–90 days",    46, 90),
)


def _category_to_domain(category: str) -> str:
    return _CATEGORY_TO_DOMAIN.get(category, "finance_capital")


def _resolution_horizon(resolution_at: datetime) -> str:
    """Map a resolution_at timestamp to a horizon id. Resolutions
    already past or > 90 days are clipped to the nearest band.
    """
    now = datetime.now(timezone.utc)
    days = max(0, (resolution_at - now).days)
    for hid, _label, lo, hi in HORIZONS:
        if lo <= days <= hi:
            return hid
    return HORIZONS[-1][0]  # clip far-future to last band


def _severity_for(p: PredictionRow) -> str:
    """Project an internal prediction onto the spec's severity scale.
    Driven by confidence + impact size — high-confidence forecasts on
    a sizeable ARR bag bubble up as 'critical'.
    """
    arr = float(p.impact.get("arr_at_risk", 0) or 0)
    if p.confidence >= 0.75 and arr >= 800_000:
        return "critical"
    if p.confidence >= 0.65:
        return "high"
    if p.confidence >= 0.5:
        return "medium"
    return "low"


def _trend_from_drivers(drivers: list[dict[str, Any]]) -> str:
    """Read a coarse trend off the key-drivers blob."""
    ups = sum(1 for d in drivers if d.get("direction") == "up")
    downs = sum(1 for d in drivers if d.get("direction") == "down")
    if ups > downs:
        return "up"
    if downs > ups:
        return "down"
    if ups + downs == 0:
        return "flat"
    return "volatile"


# ---------------------------------------------------------------------
# Page payload
# ---------------------------------------------------------------------


async def build_page_payload(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    horizon_days: int = 90,
) -> dict[str, Any]:
    """Initial ForecastsPagePayload for `GET /v1/forecasts/page`.

    Loads every active prediction (and their signals) once, then
    composes header / brief / horizon / patterns / accuracy.
    """
    active = await repo_mod.list_predictions(
        conn, tenant_id, status="active", sort="earliest_resolution",
        limit=200,
    )
    counters = await repo_mod.summary_counters(conn, tenant_id)
    cal = await accuracy_mod.calibration_summary(conn, tenant_id)

    # Pull all signals for active rows in a single batched query so the
    # detail-by-id map can be served from memory without N+1 fetches.
    signals_by_pid: dict[UUID, list[PredictionSignal]] = {}
    if active:
        sig_rows = await conn.fetch(
            """
            SELECT prediction_id, id, source, title, ts, trust_tier,
                   weight, ordinal
            FROM prediction_signals
            WHERE prediction_id = ANY($1::uuid[])
            ORDER BY prediction_id, ordinal ASC, ts DESC
            """,
            [p.id for p in active],
        )
        for r in sig_rows:
            pid = r["prediction_id"]
            sig = PredictionSignal(
                id=r["id"],
                source=r["source"],
                title=r["title"],
                ts=r["ts"],
                trust_tier=r["trust_tier"],
                weight=float(r["weight"]) if r["weight"] is not None else None,
                ordinal=int(r["ordinal"] or 0),
            )
            signals_by_pid.setdefault(pid, []).append(sig)

    patterns = _synthesize_patterns(active)
    accelerating = sum(1 for p in patterns if p["status"] == "strengthening")

    header = {
        "active_forecast_count": counters["active_count"],
        "resolving_soon_count": counters["upcoming_resolutions_count_14d"],
        "accelerating_pattern_count": accelerating,
        "calibrated_accuracy": cal.value,
        "horizon_days": horizon_days,
        "last_updated_at": _iso(datetime.now(timezone.utc)),
    }

    horizon = _build_horizon(active)
    brief = _build_brief(active)
    accuracy = _build_accuracy_summary(cal, len(active))

    # Default selection: highest-impact near-term active forecast.
    selected_id = _default_selected_id(active)

    # Pre-compute detail for selected so the first paint doesn't need
    # a follow-up round trip.
    forecast_details: dict[str, Any] = {}
    if selected_id is not None:
        target = next((p for p in active if p.id == selected_id), None)
        if target is not None:
            sigs = signals_by_pid.get(target.id, [])
            forecast_details[str(target.id)] = _build_detail(target, sigs)

    return {
        "header": header,
        "foresight_brief": brief,
        "horizon": horizon,
        "selected_forecast_id": str(selected_id) if selected_id else None,
        "forecast_details_by_id": forecast_details,
        "patterns": patterns,
        "accuracy": accuracy,
        "modes": {
            "default": "horizon",
            "available": ["horizon", "patterns", "scenarios", "accuracy"],
        },
    }


def _default_selected_id(active: list[PredictionRow]) -> UUID | None:
    if not active:
        return None
    # near-term = resolves in next 14 days
    cutoff = datetime.now(timezone.utc) + timedelta(days=14)
    near = [p for p in active if p.resolution_at <= cutoff]
    pool = near or active

    def _impact_score(p: PredictionRow) -> float:
        return float(p.impact.get("arr_at_risk", 0) or 0) * float(p.confidence)

    return max(pool, key=_impact_score).id


# ---------------------------------------------------------------------
# Horizon matrix
# ---------------------------------------------------------------------


def _build_horizon(active: list[PredictionRow]) -> dict[str, Any]:
    """Bucket active forecasts into (domain × horizon) cells.

    Each cell exposes up to 2 cards plus a hidden_count. The spec caps
    at 18 visible across the matrix.
    """
    by_domain_horizon: dict[tuple[str, str], list[PredictionRow]] = {}
    for p in active:
        d = _category_to_domain(p.category)
        h = _resolution_horizon(p.resolution_at)
        by_domain_horizon.setdefault((d, h), []).append(p)

    domain_rows: list[dict[str, Any]] = []
    for dom_id, dom_label in DOMAINS:
        cells: list[dict[str, Any]] = []
        for h_id, _h_label, _lo, _hi in HORIZONS:
            bucket = by_domain_horizon.get((dom_id, h_id), [])
            # Highest-impact first within a cell.
            bucket = sorted(
                bucket,
                key=lambda p: (
                    float(p.impact.get("arr_at_risk", 0) or 0),
                    p.confidence,
                ),
                reverse=True,
            )
            visible = bucket[:2]
            hidden = max(0, len(bucket) - len(visible))
            cells.append({
                "horizon_id": h_id,
                "forecasts": [_summary_card(p) for p in visible],
                "hidden_count": hidden,
            })
        domain_rows.append({
            "id": dom_id,
            "label": dom_label,
            "cells": cells,
        })

    return {
        "domains": domain_rows,
        "horizons": [
            {"id": h_id, "label": h_label, "start_day": lo, "end_day": hi}
            for (h_id, h_label, lo, hi) in HORIZONS
        ],
    }


def _summary_card(p: PredictionRow) -> dict[str, Any]:
    return {
        "id": str(p.id),
        "statement": p.statement,
        "domain": _category_to_domain(p.category),
        "horizon": _resolution_horizon(p.resolution_at),
        "confidence": p.confidence,
        "confidence_delta": _confidence_delta(p),
        "resolution_date": _iso(p.resolution_at),
        "impact": _impact_label(p.impact),
        "top_driver": (p.key_drivers[0]["label"]
                       if p.key_drivers and isinstance(p.key_drivers[0], dict)
                       and p.key_drivers[0].get("label")
                       else None),
        "trend": _trend_from_drivers(p.key_drivers),
        "severity": _severity_for(p),
        "intervention_available": True,
        "sparkline": _synthetic_sparkline(p),
    }


def _impact_label(impact: dict[str, Any]) -> dict[str, Any] | None:
    arr = impact.get("arr_at_risk")
    if arr is not None:
        try:
            val = float(arr)
        except (TypeError, ValueError):
            val = 0.0
        return {"label": _format_arr(val), "value": val, "unit": "ARR"}
    cap = impact.get("capacity_pct")
    if cap is not None:
        return {"label": f"{int(cap)}% capacity", "value": float(cap),
                "unit": "other"}
    commitments = impact.get("commitment_count") or impact.get(
        "blocked_commitment_count")
    if commitments is not None:
        try:
            v = int(commitments)
        except (TypeError, ValueError):
            v = 0
        return {"label": f"{v} commitments", "value": v, "unit": "commitments"}
    return None


def _format_arr(value: float) -> str:
    if value >= 1_000_000:
        return f"${value/1_000_000:.1f}M ARR"
    if value >= 1_000:
        return f"${value/1_000:.0f}K ARR"
    return f"${value:.0f} ARR"


# ---------------------------------------------------------------------
# Foresight brief
# ---------------------------------------------------------------------


def _build_brief(active: list[PredictionRow]) -> dict[str, Any]:
    """Compose ForesightBriefData. Highest-impact near-term forecasts
    populate the statement; everything else flows into the supporting
    lists.
    """
    if not active:
        return {
            "statement": (
                "No futures are forming this period. Fyralis is still "
                "monitoring leading indicators."
            ),
            "what_changed": [],
            "resolves_soon": [],
            "interventions": [],
        }

    # Sort by impact * confidence to pick headline forecasts.
    ranked = sorted(
        active,
        key=lambda p: float(p.impact.get("arr_at_risk", 0) or 0) * p.confidence,
        reverse=True,
    )
    headline = ranked[:2]
    # Compose the synthesis statement from the top one or two domains.
    domain_labels = []
    seen = set()
    for p in headline:
        d = _category_to_domain(p.category)
        if d in seen:
            continue
        seen.add(d)
        domain_labels.append(_domain_short(d))

    if len(domain_labels) == 0:
        statement = "Several futures are forming this month."
    elif len(domain_labels) == 1:
        statement = (
            f"{domain_labels[0]} is the future most likely to move this month."
        )
    else:
        statement = (
            f"{domain_labels[0]} and {domain_labels[1]} are the two "
            "futures most likely to move this month."
        )

    # What-changed: top movers (trend up + high impact). We don't have
    # historical confidence so synthesize movement from key_drivers.
    movers = ranked[:5]
    what_changed = [
        {
            "id": str(p.id),
            "label": _movement_label(p),
            "direction": _trend_from_drivers(p.key_drivers),
            "severity": _severity_for(p),
        }
        for p in movers
        if _trend_from_drivers(p.key_drivers) in ("up", "down")
    ][:3]

    # Resolves-soon (next 14d).
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=14)
    soon = [p for p in active if now <= p.resolution_at <= cutoff]
    soon.sort(key=lambda p: p.resolution_at)
    resolves_soon = [
        {
            "forecast_id": str(p.id),
            "label": _short_label(p),
            "resolution_date": _iso(p.resolution_at),
        }
        for p in soon[:3]
    ]

    # Interventions: derive one lever per top forecast.
    interventions = []
    for p in ranked[:3]:
        lever = _primary_intervention(p)
        if lever is None:
            continue
        interventions.append({
            "id": f"lever-{p.id}",
            "label": lever,
            "related_forecast_id": str(p.id),
            "action_type": "create_delta",
        })

    return {
        "statement": statement,
        "what_changed": what_changed,
        "resolves_soon": resolves_soon,
        "interventions": interventions,
    }


def _domain_short(domain_id: str) -> str:
    return {
        "customers_revenue":    "Customer-revenue health",
        "commitments_delivery": "Delivery commitments",
        "systems_capacity":     "Engineering capacity",
        "people_ownership":     "Ownership and decisions",
        "finance_capital":      "Pricing and strategy",
    }.get(domain_id, domain_id.replace("_", " "))


def _movement_label(p: PredictionRow) -> str:
    trend = _trend_from_drivers(p.key_drivers)
    base = _short_label(p)
    if trend == "up":
        return f"{base} risk increased"
    if trend == "down":
        return f"{base} risk easing"
    return base


def _short_label(p: PredictionRow) -> str:
    """A compact label suitable for brief lists."""
    if p.target_label:
        # "Beacon renewal risk" rather than the full sentence.
        if p.category == "customer_risk":
            return f"{p.target_label} renewal risk"
        if p.category == "capacity":
            return f"{p.target_label} capacity"
        if p.category == "delivery":
            return f"{p.target_label} delivery"
        return p.target_label
    # Fall back to the first 40 chars of the statement.
    return p.statement[:48] + ("…" if len(p.statement) > 48 else "")


# ---------------------------------------------------------------------
# Detail
# ---------------------------------------------------------------------


async def build_forecast_detail(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    forecast_id: UUID,
) -> dict[str, Any] | None:
    """ForecastDetail for /v1/forecasts/detail/{id}."""
    pred = await repo_mod.get_prediction(conn, tenant_id, forecast_id)
    if pred is None:
        return None
    return _build_detail(pred.prediction, pred.signals)


def _build_detail(
    p: PredictionRow, signals: list[PredictionSignal],
) -> dict[str, Any]:
    return {
        "id": str(p.id),
        "statement": p.statement,
        "domain": _category_to_domain(p.category),
        "category": p.category,
        "severity": _severity_for(p),
        "confidence": p.confidence,
        "confidence_delta": _confidence_delta(p),
        "confidence_series": _confidence_series(p),
        "resolution_date": _iso(p.resolution_at),
        "resolution_window": None,
        "why_this_forecast": p.rationale or _rationale_fallback(p),
        "driving_patterns": _driving_patterns_for(p),
        "leading_indicators": _leading_indicators_from(p, signals),
        "would_change_if": _falsifiers_for(p),
        "intervention_levers": _intervention_levers_for(p),
        "related_context": _related_context_for(p),
        "evidence_summary": _evidence_summary(signals),
        "target_label": p.target_label,
        "impact": _impact_label(p.impact),
    }


def _rationale_fallback(p: PredictionRow) -> str:
    if p.key_drivers:
        drivers = ", ".join(
            d.get("label", "") for d in p.key_drivers[:2] if d.get("label")
        )
        if drivers:
            return f"Driven by: {drivers}."
    return p.statement


def _confidence_delta(p: PredictionRow) -> float | None:
    """We don't store a historical confidence series. The seed has
    plenty of motion in the key_drivers blob, so derive a synthetic
    7-day delta from the trend direction. This keeps the inspector
    truthful at the level the demo cares about.
    """
    trend = _trend_from_drivers(p.key_drivers)
    if trend == "up":
        return round(min(0.15, (1.0 - p.confidence) * 0.4), 3)
    if trend == "down":
        return round(-min(0.10, p.confidence * 0.3), 3)
    return 0.0


def _confidence_series(p: PredictionRow) -> dict[str, Any]:
    """Synthesize a 7-point series anchored to the current confidence
    and the inferred delta. Deterministic given the row id so the
    chart stays stable across reloads.
    """
    delta = _confidence_delta(p) or 0.0
    base = p.confidence - delta
    pts = []
    now = datetime.now(timezone.utc)
    seed = int(p.id.int % 100) / 1000.0
    for i in range(7):
        # quasi-linear with a tiny per-row jitter
        t = i / 6.0
        c = base + (delta * t) + (seed * (0.5 - t))
        c = max(0.0, min(1.0, c))
        pts.append({
            "timestamp": _iso(now - timedelta(days=(6 - i))),
            "confidence": round(c, 3),
        })
    return {
        "points": pts,
        "current": p.confidence,
        "delta_window_days": 7,
        "delta": delta,
    }


def _driving_patterns_for(p: PredictionRow) -> list[dict[str, Any]]:
    """Two or three pattern facets derived from category + drivers.

    A pattern is just an aggregable theme. We synthesize them from a
    small lookup; in production these will come from a real pattern
    detector. Same shape either way.
    """
    patterns: list[dict[str, Any]] = []
    drivers_text = " ".join(
        d.get("label", "").lower() for d in p.key_drivers
        if isinstance(d, dict)
    )
    cat = p.category

    candidates: list[tuple[str, str, str, str]] = []  # (id, title, status, source_csv)
    if cat == "customer_risk" or "renewal" in drivers_text or "champion" in drivers_text:
        candidates.append((
            "anchor-reliability",
            "Anchor accounts reporting reliability issues",
            "strengthening",
            "Support,CRM,Email",
        ))
        candidates.append((
            "champion-fade",
            "Champion response gaps on escalations",
            "strengthening",
            "Slack,Email,CRM",
        ))
    if cat == "capacity" or "capacity" in drivers_text or "on-call" in drivers_text:
        candidates.append((
            "engineering-cycle-time",
            "Engineering cycle time increasing",
            "strengthening",
            "Sprint,Velocity",
        ))
        candidates.append((
            "oncall-load",
            "On-call rotation under-staffed",
            "strengthening",
            "Pager,Schedule",
        ))
    if cat == "delivery" or "block" in drivers_text or "depend" in drivers_text:
        candidates.append((
            "cross-commitment-dependency",
            "Cross-commitment dependency tightening",
            "strengthening",
            "Commitments,PRDs",
        ))
    if cat == "pricing" or cat == "decision" or "owner" in drivers_text:
        candidates.append((
            "owner-gaps",
            "Account-owner response gaps",
            "strengthening",
            "Decisions,Slack",
        ))
    if cat == "strategy" or "icp" in drivers_text:
        candidates.append((
            "icp-scoring-demand",
            "ICP scoring requests rising across enterprise",
            "emerging",
            "Pipeline,Sales",
        ))
    if cat == "partner":
        candidates.append((
            "partner-engagement-fade",
            "Design partner engagement weakening",
            "weakening",
            "Engagement,Feedback",
        ))

    if not candidates:
        candidates.append((
            "general-signal",
            "Signal coverage strengthening across sources",
            "stable",
            "Slack,CRM",
        ))

    for pid, title, status, sources in candidates[:3]:
        patterns.append({
            "id": pid,
            "title": title,
            "status": status,
            "supported_forecast_count": 1,
            "source_coverage": [s.strip() for s in sources.split(",")],
        })
    return patterns


def _leading_indicators_from(
    p: PredictionRow, signals: list[PredictionSignal],
) -> list[dict[str, Any]]:
    """Promote each key driver to a leading indicator. If the row has
    fewer than two drivers, fall back to top signals as indicators.
    """
    out: list[dict[str, Any]] = []
    for d in p.key_drivers:
        if not isinstance(d, dict):
            continue
        label = d.get("label") or "Indicator"
        direction = d.get("direction") or "flat"
        delta = d.get("delta_label") or d.get("delta") or ""
        out.append({
            "id": f"ind-{label}",
            "label": str(label),
            "value_label": str(delta),
            "direction": direction,
            "severity": (
                "negative" if direction == "up" and p.confidence >= 0.6
                else "positive" if direction == "down"
                else "neutral"
            ),
            "timeframe": "Last 7 days",
            "sparkline": _synthetic_sparkline(p, label),
        })
    if len(out) < 3:
        for s in signals[: max(0, 3 - len(out))]:
            out.append({
                "id": f"sig-{s.id}",
                "label": s.title,
                "value_label": s.source,
                "direction": "flat",
                "severity": "neutral",
                "timeframe": s.ts.strftime("%b %d") if s.ts else "",
                "sparkline": [],
            })
    return out


def _falsifiers_for(p: PredictionRow) -> list[dict[str, Any]]:
    """Build the 'Would change if' list. Splits the canonical
    falsification_condition into 1-3 observable conditions; if it
    doesn't decompose cleanly, returns it as a single condition.
    """
    raw = (p.falsification_condition or "").strip()
    if not raw:
        return [{
            "id": "f-default",
            "text": "No specific falsifier recorded yet.",
            "observable": False,
            "timeframe": None,
            "status": "unmet",
        }]
    # Split on " and ", "; " or "OR" patterns into separate conditions.
    parts: list[str] = []
    buffer = raw
    for sep in [" and ", "; ", " AND ", ", and "]:
        if sep in buffer:
            parts = [t.strip() for t in buffer.split(sep) if t.strip()]
            break
    if not parts:
        parts = [raw]
    out = []
    for i, part in enumerate(parts[:3]):
        text = part.rstrip(". ")
        if not text.endswith("."):
            text = text + "."
        # Sentence-case the first letter.
        text = text[0].upper() + text[1:] if text else text
        out.append({
            "id": f"f-{p.id}-{i}",
            "text": text,
            "observable": True,
            "timeframe": "Within 7 business days",
            "status": "unmet",
        })
    return out


def _intervention_levers_for(p: PredictionRow) -> list[dict[str, Any]]:
    """Top 2-3 levers per category. Each lever links back into the
    rest of the product surface.
    """
    cat = p.category
    base: list[tuple[str, str, str]] = []
    if cat == "customer_risk":
        base = [
            ("Escalate sync owner", "Owner-gap risk decreases",
             "create_proposed_change"),
            ("Increase account touchpoints",
             "Renewal sentiment may shift positive",
             "create_proposed_change"),
            ("Open in Model", "Trace anchor-account context",
             "open_model"),
        ]
    elif cat == "capacity":
        base = [
            ("Pause net-new platform commitments",
             "Capacity decreases ~8%", "create_proposed_change"),
            ("Re-staff on-call rotation",
             "On-call load returns within target",
             "create_proposed_change"),
            ("View Today item",
             "Open ownership review", "open_today"),
        ]
    elif cat == "delivery":
        base = [
            ("Re-sequence Q3 commitments",
             "Slip risk decreases", "create_proposed_change"),
            ("Open in Model", "Trace dependency chain", "open_model"),
        ]
    elif cat == "pricing" or cat == "decision":
        base = [
            ("Resolve pricing ownership",
             "Decision age drops to zero",
             "create_proposed_change"),
            ("View Today item",
             "Open Proposed Change review", "open_today"),
        ]
    elif cat == "strategy":
        base = [
            ("Realign ICP definition",
             "Pipeline composition rebalances",
             "create_proposed_change"),
            ("Open in Model", "Open scoring model in Model",
             "open_model"),
        ]
    elif cat == "partner":
        base = [
            ("Schedule product council outreach",
             "Engagement composite improves",
             "create_proposed_change"),
            ("Ask Fyralis",
             "Explore intervention options", "ask"),
        ]
    else:
        base = [
            ("Create Proposed Change", "", "create_proposed_change"),
        ]
    out = []
    for i, (label, effect, kind) in enumerate(base):
        out.append({
            "id": f"lever-{p.id}-{i}",
            "label": label,
            "expected_effect": effect or None,
            "action_type": kind,
            "related_object_id": None,
        })
    return out


def _primary_intervention(p: PredictionRow) -> str | None:
    levers = _intervention_levers_for(p)
    return levers[0]["label"] if levers else None


def _related_context_for(p: PredictionRow) -> dict[str, Any]:
    """Stub cross-page links. The actual Model / Today / Ledger IDs
    aren't joined here; link text is enough to anchor the section.
    """
    domain_label = _domain_short(_category_to_domain(p.category))
    target = p.target_label or "Untargeted"
    return {
        "model_links": [
            {
                "label": f"{domain_label} → {target}",
                "href": f"/model?focus={_category_to_domain(p.category)}",
            },
        ],
        "today_links": [],
        "ledger_links": [],
    }


def _evidence_summary(signals: list[PredictionSignal]) -> dict[str, Any]:
    quality: str
    n = len(signals)
    if n == 0:
        quality = "weak"
    elif n <= 1:
        quality = "partial"
    elif n <= 3:
        quality = "moderate"
    else:
        quality = "strong"
    by_source: dict[str, int] = {}
    for s in signals:
        by_source[s.source] = by_source.get(s.source, 0) + 1
    sources = [
        {
            "label": src,
            "strength": "strong" if cnt >= 2 else "moderate",
            "count": cnt,
        }
        for src, cnt in by_source.items()
    ]
    return {
        "signal_count": n,
        "quality": quality,
        "sources": sources,
    }


def _synthetic_sparkline(
    p: PredictionRow, salt: str = "",
) -> list[float]:
    """Deterministic 6-point sparkline keyed on the row id + salt.
    Tracks the inferred trend direction so the card feels real.
    """
    base = p.confidence
    trend = _trend_from_drivers(p.key_drivers)
    seed = (int(p.id.int) + sum(ord(c) for c in salt)) % 100
    pts: list[float] = []
    for i in range(6):
        t = i / 5.0
        if trend == "up":
            v = base - 0.2 + t * 0.25 + ((seed % 7) / 100.0) * (0.5 - t)
        elif trend == "down":
            v = base + 0.15 - t * 0.2 + ((seed % 5) / 100.0) * (t - 0.5)
        else:
            v = base + ((seed % 9) / 100.0) * (0.5 - t)
        pts.append(round(max(0.0, min(1.0, v)), 3))
    return pts


# ---------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------


def _synthesize_patterns(
    active: list[PredictionRow],
) -> list[dict[str, Any]]:
    """Pattern cards for Pattern Field + Patterns Mode.

    A pattern is a theme that supports >= 1 forecast. We cluster by
    the canonical driving-pattern ids that _driving_patterns_for emits,
    so the Pattern Field and the per-forecast detail stay consistent.
    """
    cluster: dict[str, dict[str, Any]] = {}
    for p in active:
        for fac in _driving_patterns_for(p):
            pid = fac["id"]
            if pid not in cluster:
                cluster[pid] = {
                    "id": pid,
                    "title": fac["title"],
                    "status": fac["status"],
                    "supported_forecast_count": 0,
                    "sources": fac.get("source_coverage", []),
                    "related_forecast_ids": [],
                    "confidence": None,
                    "movement": "up" if fac["status"] == "strengthening"
                                else "down" if fac["status"] == "weakening"
                                else "flat",
                }
            cluster[pid]["supported_forecast_count"] += 1
            cluster[pid]["related_forecast_ids"].append(str(p.id))
    # Most-supported patterns first.
    return sorted(
        cluster.values(),
        key=lambda c: c["supported_forecast_count"],
        reverse=True,
    )


async def list_patterns(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    """Public wrapper used by the /patterns endpoint."""
    active = await repo_mod.list_predictions(
        conn, tenant_id, status="active", limit=200,
    )
    return _synthesize_patterns(active)


# ---------------------------------------------------------------------
# Accuracy summary
# ---------------------------------------------------------------------


def _build_accuracy_summary(
    cal: accuracy_mod.CalibrationSummary, active_count: int,
) -> dict[str, Any]:
    return {
        "period": "last_30_days",
        "calibrated_accuracy": cal.value,
        "resolved_true": None,    # populated by /accuracy endpoint
        "resolved_false": None,
        "pending": active_count,
        "avg_calibration_error_pp": (
            None if cal.value is None else round(abs(1.0 - cal.value) * 100, 1)
        ),
        "trend": [],
    }


# ---------------------------------------------------------------------
# Ask Fyralis
# ---------------------------------------------------------------------


@dataclass
class AskRequest:
    page: str
    mode: str
    selected_forecast_id: UUID | None
    selected_pattern_id: str | None
    prompt: str
    visible_forecast_ids: list[UUID]
    horizon_days: int


async def handle_ask(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    req: AskRequest,
) -> dict[str, Any]:
    """Templated Ask response. Real LLM plumbing can replace this
    function body without changing the wire contract — it accepts an
    AskRequest and returns a ForecastAskResponse.
    """
    prompt_l = (req.prompt or "").lower().strip()
    pred = None
    signals: list[PredictionSignal] = []
    if req.selected_forecast_id:
        detail = await repo_mod.get_prediction(
            conn, tenant_id, req.selected_forecast_id,
        )
        if detail is not None:
            pred = detail.prediction
            signals = detail.signals

    # Classify intent off a small keyword set. Order matters; first
    # match wins.
    if any(k in prompt_l for k in ("what if", "scenario", "assign", "pause")):
        return _ask_scenario(pred, prompt_l)
    if any(k in prompt_l for k in ("falsif", "would change", "change your mind")):
        return _ask_falsifier(pred)
    if any(k in prompt_l for k in ("why", "explain", "drove", "caused")):
        return _ask_explanation(pred, signals)
    if any(k in prompt_l for k in ("similar", "past", "historic")):
        return _ask_accuracy(pred)
    if any(k in prompt_l for k in ("intervention", "lever", "leverage")):
        return _ask_intervention(pred)
    if any(k in prompt_l for k in ("pattern", "theme", "recurring")):
        return _ask_pattern(pred)
    return _ask_default(pred, req.prompt)


def _ask_explanation(
    p: PredictionRow | None, signals: list[PredictionSignal],
) -> dict[str, Any]:
    if p is None:
        return _ask_default(None, "Why did this forecast move?")
    drivers_text = ", ".join(
        d.get("label", "") for d in p.key_drivers[:3] if d.get("label")
    ) or "trend across recent signals"
    body = (
        f"Confidence on '{p.statement}' is {int(p.confidence*100)}%. "
        f"It moved because: {drivers_text}. "
        f"{len(signals)} supporting signals across "
        f"{len({s.source for s in signals})} sources."
    )
    return {
        "type": "forecast_explanation",
        "title": "Why this forecast moved",
        "body": body,
        "evidence_used": [s.title for s in signals[:5]],
        "missing_context": _missing_context(p, signals),
        "actions": [
            {"label": "Open in Model", "type": "open_model"},
        ],
    }


def _ask_scenario(p: PredictionRow | None, prompt: str) -> dict[str, Any]:
    if p is None:
        return _ask_default(None, prompt)
    # Synthesize a coarse expected effect.
    delta_pp = int((p.confidence - 0.55) * 100)
    base_pct = int(p.confidence * 100)
    new_pct = max(0, min(100, base_pct - delta_pp))
    body = (
        f"Scenario: {prompt.strip().capitalize()}. "
        f"If the intervention is confirmed within 48 hours, "
        f"confidence on '{p.statement}' could move from {base_pct}% "
        f"to ~{new_pct}%. Tradeoffs depend on adjacent commitments."
    )
    return {
        "type": "scenario_analysis",
        "title": f"Scenario: {prompt.strip().capitalize()}",
        "body": body,
        "evidence_used": [
            d.get("label", "") for d in p.key_drivers[:3] if d.get("label")
        ],
        "missing_context": _missing_context(p, []),
        "actions": [
            {"label": "Save scenario", "type": "save_scenario"},
            {"label": "Create Proposed Change", "type": "create_proposed_change"},
        ],
    }


def _ask_falsifier(p: PredictionRow | None) -> dict[str, Any]:
    if p is None:
        return _ask_default(None, "What would falsify this?")
    falsifiers = _falsifiers_for(p)
    body = "Fyralis will revise this forecast if any of these become observable:\n" + "\n".join(
        f"• {f['text']}" for f in falsifiers
    )
    return {
        "type": "falsifier_explanation",
        "title": "What would change Fyralis' mind",
        "body": body,
        "evidence_used": [],
        "missing_context": [],
        "actions": [],
    }


def _ask_intervention(p: PredictionRow | None) -> dict[str, Any]:
    if p is None:
        return _ask_default(None, "Which intervention has the most leverage?")
    levers = _intervention_levers_for(p)
    body = "Top intervention levers, ordered by expected impact:\n" + "\n".join(
        f"• {l['label']} — {l.get('expected_effect') or 'effect not yet modeled'}"
        for l in levers
    )
    return {
        "type": "intervention_comparison",
        "title": "Intervention comparison",
        "body": body,
        "evidence_used": [],
        "missing_context": [],
        "actions": [
            {"label": "Create Proposed Change", "type": "create_proposed_change"},
        ],
    }


def _ask_pattern(p: PredictionRow | None) -> dict[str, Any]:
    if p is None:
        return _ask_default(None, "Which patterns support this?")
    pats = _driving_patterns_for(p)
    body = "Patterns supporting this forecast:\n" + "\n".join(
        f"• {pp['title']} ({pp['status']})" for pp in pats
    )
    return {
        "type": "pattern_trace",
        "title": "Patterns driving this forecast",
        "body": body,
        "evidence_used": [],
        "missing_context": [],
        "actions": [],
    }


def _ask_accuracy(p: PredictionRow | None) -> dict[str, Any]:
    body = (
        "Fyralis tracks historical accuracy per category. See the "
        "Accuracy mode for resolved forecasts in this domain."
    )
    return {
        "type": "accuracy_reference",
        "title": "Similar past outcomes",
        "body": body,
        "evidence_used": [],
        "missing_context": [],
        "actions": [
            {"label": "Open accuracy", "type": "open_model"},
        ],
    }


def _ask_default(p: PredictionRow | None, prompt: str) -> dict[str, Any]:
    title = "Forecast context"
    if p is not None:
        body = (
            f"Selected forecast: '{p.statement}'. "
            f"Confidence {int(p.confidence*100)}%. "
            "Try asking 'why did this increase', 'what would falsify this', "
            "or 'what if we assign an owner today'."
        )
    else:
        body = (
            "Select a forecast to ask Fyralis for explanation, scenarios, "
            "falsifiers, or intervention comparisons."
        )
    return {
        "type": "forecast_explanation",
        "title": title,
        "body": body,
        "evidence_used": [],
        "missing_context": [],
        "actions": [],
    }


def _missing_context(
    p: PredictionRow | None, signals: list[PredictionSignal],
) -> list[str]:
    if p is None:
        return []
    missing: list[str] = []
    sources = {s.source for s in signals}
    if "salesforce" not in sources and p.category == "customer_risk":
        missing.append("No recent CRM transcript")
    if "slack" not in sources:
        missing.append("No recent Slack thread on this topic")
    return missing


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


__all__ = [
    "AskRequest",
    "DOMAINS",
    "HORIZONS",
    "build_page_payload",
    "build_forecast_detail",
    "list_patterns",
    "handle_ask",
]
