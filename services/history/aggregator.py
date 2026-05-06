"""services/history/aggregator.py — derive the History payload.

The History UI consumes `events`, `predictions`, `arcs`, `calibration`,
`layer_counts`, plus narrative-band statements. There's no dedicated
history table — every shape is derived from the substrate:

  * Events ← observations(kind='state_change') joined to commitments /
    decisions / models, plus models lifecycle events (prediction-made,
    prediction-resolved, pattern-emerged, pattern-dissolved).
  * Predictions ← models WHERE proposition_kind='prediction'.
  * Arcs ← models WHERE proposition_kind IN ('pattern','pattern_instance');
    arc events = the model's supporting_event_ids resolved to event ids.
  * Calibration ← resolved models grouped by proposition_kind; score is
    (correct / total) over the period window.
  * Layer counts + narrative statements ← computed from the above.

This module is a translator. It owns no state — every call is a fresh
read against the substrate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg


# ---------------------------------------------------------------------
# Period handling
# ---------------------------------------------------------------------


_PERIOD_DAYS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
    "365d": 365,
}


def _cutoff_for(period: str, now: datetime) -> datetime | None:
    """Return the inclusive lower bound for the period, or None for 'all'."""
    days = _PERIOD_DAYS.get(period)
    if days is None:
        return None
    return now - timedelta(days=days)


# ---------------------------------------------------------------------
# Output shape — JSON-serializable dicts, mirrors history/types.ts
# ---------------------------------------------------------------------


@dataclass
class HistoryPayload:
    events: list[dict[str, Any]] = field(default_factory=list)
    predictions: list[dict[str, Any]] = field(default_factory=list)
    arcs: list[dict[str, Any]] = field(default_factory=list)
    calibration: dict[str, Any] = field(default_factory=dict)
    layer_counts: dict[str, Any] = field(default_factory=dict)
    chronicle_statement: list[dict[str, Any]] = field(default_factory=list)
    predictions_statement: list[dict[str, Any]] = field(default_factory=list)
    arcs_statement: list[dict[str, Any]] = field(default_factory=list)
    period: str = "90d"

    def to_dict(self) -> dict[str, Any]:
        return {
            "events": self.events,
            "predictions": self.predictions,
            "arcs": self.arcs,
            "calibration": self.calibration,
            "layer_counts": self.layer_counts,
            "chronicle_statement": self.chronicle_statement,
            "predictions_statement": self.predictions_statement,
            "arcs_statement": self.arcs_statement,
            "period": self.period,
        }


# ---------------------------------------------------------------------
# Event derivation — state_change observations
# ---------------------------------------------------------------------


# Commitment state → event type. The state_change observation's
# `state_change_kind` is `commitment_<new_state>` (see emit_state_change
# callers in services.acts.commitments).
_COMMITMENT_STATE_EVENT_TYPE = {
    "doneverified": "commitment-completed",
    "doneunverified": "commitment-completed",
    "closed": "commitment-completed",
    "blocked": "commitment-blocked",
    "paused": "commitment-blocked",
}


_DECISION_STATE_EVENT_TYPE = {
    "drafted": "decision-made",
    "active": "decision-ratified",
    "revisited": "decision-contested",
    "archived": "decision-superseded",
}


def _commitment_event_type(new_state: str) -> str | None:
    return _COMMITMENT_STATE_EVENT_TYPE.get(new_state)


def _decision_event_type(new_state: str) -> str | None:
    return _DECISION_STATE_EVENT_TYPE.get(new_state)


def _short_id(uid: Any) -> str:
    s = str(uid)
    return s.split("-", 1)[0][:8]


def _prominence_for_decision(state: str) -> str:
    if state in ("active", "archived"):
        return "major"
    return "standard"


def _prominence_for_commitment(state: str) -> str:
    if state == "blocked":
        return "standard"
    if state in ("doneverified", "doneunverified", "closed"):
        return "minor"
    return "minor"


# Pull state_change observations within the window. Filter to states
# that map to known event types. The content JSON carries entity_kind
# and entity_id; we LEFT JOIN to commitments/decisions in Python so we
# can hydrate titles selectively.
_STATE_CHANGES_SQL = """
SELECT
  o.id            AS obs_id,
  o.occurred_at   AS occurred_at,
  o.actor_id      AS actor_id,
  o.content       AS content,
  o.cause_id      AS cause_id
FROM observations o
WHERE o.tenant_id = $1
  AND o.kind = 'state_change'
  AND ($2::timestamptz IS NULL OR o.occurred_at >= $2)
ORDER BY o.occurred_at DESC
LIMIT 500
"""


async def _fetch_state_change_events(
    *,
    tenant_id: UUID,
    cutoff: datetime | None,
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_STATE_CHANGES_SQL, tenant_id, cutoff)
    if not rows:
        return []

    # Bucket entity ids per kind so we can do one SELECT per kind.
    commitment_ids: set[UUID] = set()
    decision_ids: set[UUID] = set()
    actor_ids: set[UUID] = set()

    parsed: list[dict[str, Any]] = []
    for r in rows:
        content = r["content"]
        if isinstance(content, str):
            import json
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                continue
        if not isinstance(content, dict):
            continue
        entity_kind = content.get("entity_kind")
        sc_kind = content.get("state_change_kind") or ""
        entity_id_raw = content.get("entity_id")
        if not entity_id_raw or not entity_kind:
            continue
        try:
            entity_id = UUID(str(entity_id_raw))
        except (ValueError, TypeError):
            continue

        new_state: str | None = None
        if sc_kind.startswith(f"{entity_kind}_"):
            new_state = sc_kind.split("_", 1)[1]

        parsed.append(
            {
                "obs_id": r["obs_id"],
                "occurred_at": r["occurred_at"],
                "actor_id": r["actor_id"],
                "entity_kind": entity_kind,
                "entity_id": entity_id,
                "state_change_kind": sc_kind,
                "new_state": new_state,
                "metadata": content.get("metadata") or {},
            }
        )
        if entity_kind == "commitment":
            commitment_ids.add(entity_id)
        elif entity_kind == "decision":
            decision_ids.add(entity_id)
        if r["actor_id"] is not None:
            actor_ids.add(r["actor_id"])

    titles_by_commitment: dict[UUID, str] = {}
    if commitment_ids:
        crows = await conn.fetch(
            "SELECT id, title FROM commitments WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
            tenant_id,
            list(commitment_ids),
        )
        titles_by_commitment = {r["id"]: r["title"] for r in crows}

    titles_by_decision: dict[UUID, str] = {}
    if decision_ids:
        drows = await conn.fetch(
            "SELECT id, title FROM decisions WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
            tenant_id,
            list(decision_ids),
        )
        titles_by_decision = {r["id"]: r["title"] for r in drows}

    actor_names: dict[UUID, str] = {}
    if actor_ids:
        arows = await conn.fetch(
            "SELECT id, display_name FROM actors WHERE tenant_id = $1 AND id = ANY($2::uuid[])",
            tenant_id,
            list(actor_ids),
        )
        actor_names = {r["id"]: r["display_name"] for r in arows}

    out: list[dict[str, Any]] = []
    for p in parsed:
        if p["entity_kind"] == "commitment":
            event_type = _commitment_event_type(p["new_state"] or "")
            if event_type is None:
                continue
            title = titles_by_commitment.get(p["entity_id"]) or _short_id(p["entity_id"])
            actor_name = actor_names.get(p["actor_id"]) if p["actor_id"] else None
            descriptor = (
                f"{title} {p['new_state'].replace('_', ' ')}"
                if p["new_state"]
                else title
            )
            if actor_name:
                descriptor = f"{descriptor} — {actor_name}"
            links = [
                {
                    "type": "commitment",
                    "id": str(p["entity_id"]),
                    "label": title,
                }
            ]
            if p["actor_id"] and actor_name:
                links.append(
                    {
                        "type": "person",
                        "id": str(p["actor_id"]),
                        "label": actor_name,
                    }
                )
            out.append(
                {
                    "id": f"evt-{p['obs_id']}",
                    "timestamp": p["occurred_at"].isoformat()
                    if hasattr(p["occurred_at"], "isoformat")
                    else str(p["occurred_at"]),
                    "type": event_type,
                    "prominence": _prominence_for_commitment(p["new_state"] or ""),
                    "title": title.upper(),
                    "descriptor": descriptor,
                    "links": links,
                }
            )
        elif p["entity_kind"] == "decision":
            event_type = _decision_event_type(p["new_state"] or "")
            if event_type is None:
                continue
            title = titles_by_decision.get(p["entity_id"]) or _short_id(p["entity_id"])
            descriptor_verb = {
                "decision-made": "drafted",
                "decision-ratified": "ratified",
                "decision-contested": "revisited",
                "decision-superseded": "archived",
            }.get(event_type, p["new_state"] or "updated")
            out.append(
                {
                    "id": f"evt-{p['obs_id']}",
                    "timestamp": p["occurred_at"].isoformat()
                    if hasattr(p["occurred_at"], "isoformat")
                    else str(p["occurred_at"]),
                    "type": event_type,
                    "prominence": _prominence_for_decision(p["new_state"] or ""),
                    "title": title.upper(),
                    "descriptor": f"{title} {descriptor_verb}.",
                    "links": [
                        {
                            "type": "decision",
                            "id": str(p["entity_id"]),
                            "label": title,
                        }
                    ],
                }
            )
        # model state_changes are emitted alongside model lifecycle —
        # we surface those via the dedicated model query below.
    return out


# ---------------------------------------------------------------------
# Model-derived events: predictions made/resolved, patterns emerged/dissolved
# ---------------------------------------------------------------------


_MODEL_EVENTS_SQL = """
SELECT
  id, "natural", proposition_kind, confidence,
  confidence_at_assertion,
  created_at, resolved_at, resolution_outcome,
  status, archived_at, archive_reason,
  supporting_event_ids
FROM models
WHERE tenant_id = $1
  AND proposition_kind IN ('prediction','pattern','pattern_instance')
  AND (
    ($2::timestamptz IS NULL) OR
    (created_at >= $2) OR
    (resolved_at IS NOT NULL AND resolved_at >= $2) OR
    (archived_at IS NOT NULL AND archived_at >= $2)
  )
ORDER BY created_at DESC
LIMIT 500
"""


def _model_event_records(rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        kind = r["proposition_kind"]
        natural = (r["natural"] or "").strip()
        title = (natural[:80] or kind.upper())
        if kind == "prediction":
            # made
            out.append(
                {
                    "id": f"evt-pred-made-{r['id']}",
                    "timestamp": r["created_at"].isoformat(),
                    "type": "prediction-made",
                    "prominence": "standard",
                    "title": "PREDICTION MADE",
                    "descriptor": natural
                    or "Prediction filed.",
                    "links": [
                        {"type": "prediction", "id": str(r["id"]), "label": _short_id(r["id"])}
                    ],
                }
            )
            if r["resolved_at"] is not None:
                outcome = (
                    "correct" if bool(r["resolution_outcome"]) else "wrong"
                )
                out.append(
                    {
                        "id": f"evt-pred-res-{r['id']}",
                        "timestamp": r["resolved_at"].isoformat(),
                        "type": "prediction-resolved",
                        "prominence": "standard",
                        "title": f"PREDICTION RESOLVED · {outcome.upper()}",
                        "descriptor": natural[:240] or "Prediction resolved.",
                        "links": [
                            {"type": "prediction", "id": str(r["id"]), "label": _short_id(r["id"])}
                        ],
                    }
                )
        elif kind in ("pattern", "pattern_instance"):
            out.append(
                {
                    "id": f"evt-pat-emerged-{r['id']}",
                    "timestamp": r["created_at"].isoformat(),
                    "type": "pattern-emerged",
                    "prominence": "major" if (r["confidence"] or 0) >= 0.75 else "standard",
                    "title": title.upper(),
                    "descriptor": natural or "Pattern crossed threshold.",
                    "links": [
                        {"type": "pattern", "id": str(r["id"]), "label": _short_id(r["id"])}
                    ],
                    "arc": str(r["id"]),
                }
            )
            if r["status"] in ("archived", "superseded") and r["archived_at"] is not None:
                out.append(
                    {
                        "id": f"evt-pat-dissolved-{r['id']}",
                        "timestamp": r["archived_at"].isoformat(),
                        "type": "pattern-dissolved",
                        "prominence": "standard",
                        "title": f"{title.upper()} DISSOLVED",
                        "descriptor": (r["archive_reason"] or "").replace("_", " ")
                        or "Pattern faded.",
                        "links": [
                            {"type": "pattern", "id": str(r["id"]), "label": _short_id(r["id"])}
                        ],
                        "arc": str(r["id"]),
                    }
                )
    return out


# ---------------------------------------------------------------------
# Predictions list
# ---------------------------------------------------------------------


_PREDICTIONS_SQL = """
SELECT
  id, "natural", proposition, confidence, confidence_at_assertion,
  created_at, resolved_at, resolution_outcome,
  scope_actors, scope_entities,
  supporting_event_ids
FROM models
WHERE tenant_id = $1
  AND proposition_kind = 'prediction'
  AND (
    ($2::timestamptz IS NULL) OR
    (created_at >= $2) OR
    (resolved_at IS NOT NULL AND resolved_at >= $2)
  )
ORDER BY created_at DESC
LIMIT 200
"""


# Map prediction proposition.domain to the UI's PredictionDomain literal
# (defined in ui/src/components/history/types.ts). Falls back to a
# best-effort textual match. Unknown domains map to "predictions".
_DOMAIN_ALIASES = {
    "patterns": "patterns",
    "pattern": "patterns",
    "decisions": "decisions",
    "decision": "decisions",
    "personnel": "personnel",
    "people": "personnel",
    "person": "personnel",
    "customer health": "customer health",
    "customer": "customer health",
    "customers": "customer health",
    "predictions": "predictions",
    "prediction": "predictions",
}


def _classify_domain(proposition: dict[str, Any], natural: str) -> str:
    raw = (proposition or {}).get("domain") or (proposition or {}).get("topic")
    if isinstance(raw, str):
        d = _DOMAIN_ALIASES.get(raw.strip().lower())
        if d is not None:
            return d
    blob = " ".join(
        str(x).lower()
        for x in (natural, (proposition or {}).get("about") or "", (proposition or {}).get("category") or "")
    )
    if "customer" in blob or "churn" in blob or "renewal" in blob:
        return "customer health"
    if "decision" in blob or "ratif" in blob or "drift" in blob:
        return "decisions"
    if "hire" in blob or "headcount" in blob or "personnel" in blob or "team" in blob:
        return "personnel"
    if "pattern" in blob or "cluster" in blob or "threshold" in blob:
        return "patterns"
    return "predictions"


def _prediction_status(row: asyncpg.Record) -> str:
    if row["resolved_at"] is None:
        return "pending"
    return "correct" if bool(row["resolution_outcome"]) else "wrong"


def _build_predictions(rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        proposition = r["proposition"]
        if isinstance(proposition, str):
            import json
            try:
                proposition = json.loads(proposition)
            except json.JSONDecodeError:
                proposition = {}
        if not isinstance(proposition, dict):
            proposition = {}
        natural = (r["natural"] or "").strip()
        item: dict[str, Any] = {
            "id": str(r["id"]),
            "made_on": r["created_at"].isoformat(),
            "domain": _classify_domain(proposition, natural),
            "prediction_text": natural or "Prediction filed.",
            "confidence": float(r["confidence_at_assertion"] or r["confidence"] or 0.0),
            "status": _prediction_status(r),
        }
        reasoning = proposition.get("reasoning") or proposition.get("rationale")
        if isinstance(reasoning, str) and reasoning.strip():
            item["reasoning_at_time"] = reasoning.strip()
        if r["resolved_at"] is not None:
            item["resolved_on"] = r["resolved_at"].isoformat()
        out.append(item)
    return out


# ---------------------------------------------------------------------
# Arcs — derived from pattern / pattern_instance models
# ---------------------------------------------------------------------


_ARCS_SQL = """
SELECT
  id, "natural", proposition, status,
  created_at, archived_at, archive_reason,
  supporting_event_ids, supporting_model_ids
FROM models
WHERE tenant_id = $1
  AND proposition_kind IN ('pattern','pattern_instance')
ORDER BY created_at DESC
LIMIT 100
"""


def _arc_status(row: asyncpg.Record) -> str:
    return "open" if row["status"] == "active" else "resolved"


def _arc_name(natural: str, proposition: dict[str, Any]) -> str:
    if isinstance(proposition, dict):
        for key in ("title", "name", "label"):
            v = proposition.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()[:80]
    return (natural[:60] or "Pattern").rstrip()


def _build_arcs(
    rows: list[asyncpg.Record],
    *,
    obs_id_to_event_id: dict[UUID, str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        proposition = r["proposition"]
        if isinstance(proposition, str):
            import json
            try:
                proposition = json.loads(proposition)
            except json.JSONDecodeError:
                proposition = {}
        if not isinstance(proposition, dict):
            proposition = {}
        natural = (r["natural"] or "").strip()
        # Translate supporting_event_ids (observation UUIDs) into
        # HistoryEvent ids (`evt-<obs_uuid>`). Observations not in the
        # current period are dropped.
        supporting = r["supporting_event_ids"] or []
        event_ids: list[str] = []
        for oid in supporting:
            ev = obs_id_to_event_id.get(oid)
            if ev:
                event_ids.append(ev)
        # Fall back to including the model-derived event id (the arc's
        # own pattern-emerged event) so the arc panel never reads empty.
        emerged_id = f"evt-pat-emerged-{r['id']}"
        if emerged_id not in event_ids:
            event_ids.append(emerged_id)

        arc: dict[str, Any] = {
            "id": str(r["id"]),
            "name": _arc_name(natural, proposition),
            "status": _arc_status(r),
            "started": r["created_at"].date().isoformat(),
            "narrative": natural or "Pattern detected by Fyralis.",
            "events": event_ids,
        }
        if r["archived_at"] is not None:
            arc["ended"] = r["archived_at"].date().isoformat()
        out.append(arc)
    return out


# ---------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------


_CALIBRATION_SQL = """
SELECT
  proposition_kind,
  proposition,
  confidence_at_assertion,
  resolution_outcome,
  resolved_at
FROM models
WHERE tenant_id = $1
  AND resolved_at IS NOT NULL
  AND ($2::timestamptz IS NULL OR resolved_at >= $2)
LIMIT 5000
"""


_CALIBRATION_TREND_SQL = """
SELECT
  date_trunc('day', resolved_at) AS bucket,
  avg(CASE WHEN resolution_outcome THEN 1 ELSE 0 END)::float AS hit_rate
FROM models
WHERE tenant_id = $1
  AND resolved_at IS NOT NULL
  AND resolved_at >= now() - interval '120 days'
GROUP BY bucket
ORDER BY bucket
"""


async def _build_calibration(
    *,
    tenant_id: UUID,
    cutoff: datetime | None,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    rows = await conn.fetch(_CALIBRATION_SQL, tenant_id, cutoff)
    by_domain: dict[str, list[bool]] = {}
    for r in rows:
        prop = r["proposition"]
        if isinstance(prop, str):
            import json
            try:
                prop = json.loads(prop)
            except json.JSONDecodeError:
                prop = {}
        if not isinstance(prop, dict):
            prop = {}
        # Use the same domain classification as predictions for consistency.
        domain = _classify_domain(prop, r.get("proposition_kind") or "")
        # For non-prediction kinds, fall back to a kind-based bucket.
        pk = r.get("proposition_kind")
        if pk and pk != "prediction":
            domain = "patterns" if "pattern" in pk else "predictions"
        by_domain.setdefault(domain, []).append(bool(r["resolution_outcome"]))

    domains: list[dict[str, Any]] = []
    overall_correct = 0
    overall_total = 0
    for name, outcomes in by_domain.items():
        total = len(outcomes)
        correct = sum(1 for o in outcomes if o)
        score = (correct / total) if total else 0.0
        domains.append(
            {
                "name": name,
                "correct": correct,
                "total": total,
                "score": round(score, 3),
            }
        )
        overall_correct += correct
        overall_total += total
    overall = (overall_correct / overall_total) if overall_total else 0.0

    out: dict[str, Any] = {
        "overall": round(overall, 3),
        "domains": sorted(domains, key=lambda d: -d["score"]),
    }

    # Trend: split last 120 days at the midpoint and compare hit-rates.
    trend_rows = await conn.fetch(_CALIBRATION_TREND_SQL, tenant_id)
    if len(trend_rows) >= 4:
        mid = len(trend_rows) // 2
        first_half = trend_rows[:mid]
        second_half = trend_rows[mid:]
        avg_first = sum(r["hit_rate"] for r in first_half) / max(1, len(first_half))
        avg_second = sum(r["hit_rate"] for r in second_half) / max(1, len(second_half))
        if abs(avg_second - avg_first) < 0.02:
            direction = "flat"
        elif avg_second > avg_first:
            direction = "improving"
        else:
            direction = "declining"
        out["trend"] = {
            "direction": direction,
            "from_score": round(avg_first, 3),
            "from_date": first_half[0]["bucket"].date().isoformat(),
            "to_score": round(avg_second, 3),
            "to_date": second_half[-1]["bucket"].date().isoformat(),
        }
    return out


# ---------------------------------------------------------------------
# Narrative band statements
# ---------------------------------------------------------------------


def _chronicle_statement(
    *,
    period: str,
    events: list[dict[str, Any]],
    arcs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """A short, data-driven sentence framing the period. ShapeToken
    array — the UI renders {kind:"text"} as plain text and {kind:"ref"}
    as clickable chips."""
    period_label = {
        "7d": "the last week",
        "30d": "the last 30 days",
        "90d": "the last quarter",
        "365d": "the last year",
        "all": "the full record",
    }.get(period, "this period")

    if not events:
        return [
            {"kind": "text", "text": f"No substrate events landed in {period_label} yet."}
        ]

    major = [e for e in events if e.get("prominence") == "major"]
    open_arcs = [a for a in arcs if a.get("status") == "open"]
    out: list[dict[str, Any]] = [
        {
            "kind": "text",
            "text": f"{len(events)} events in {period_label}. ",
        }
    ]
    if major:
        out.append(
            {
                "kind": "text",
                "text": f"{len(major)} major. ",
            }
        )
    if open_arcs:
        first = open_arcs[0]
        out.append({"kind": "text", "text": "Most active arc: "})
        out.append(
            {
                "kind": "ref",
                "ref": {"type": "arc", "id": first["id"], "text": first["name"]},
            }
        )
        out.append({"kind": "text", "text": "."})
    return out


def _predictions_statement(calibration: dict[str, Any]) -> list[dict[str, Any]]:
    overall = calibration.get("overall") or 0.0
    domains = calibration.get("domains") or []
    if not domains:
        return [
            {
                "kind": "text",
                "text": "No resolved predictions yet — calibration window is still warming.",
            }
        ]
    best = max(domains, key=lambda d: d["score"])
    worst = min(domains, key=lambda d: d["score"])
    text = (
        f"Overall calibration {overall:.2f}. "
        f"Strongest domain: {best['name']} ({best['correct']} of {best['total']}). "
        f"Weakest: {worst['name']} ({worst['correct']} of {worst['total']})."
    )
    trend = calibration.get("trend")
    if isinstance(trend, dict):
        text += f" Trend: {trend.get('direction')}."
    return [{"kind": "text", "text": text}]


def _arcs_statement(arcs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not arcs:
        return [
            {"kind": "text", "text": "No arcs detected yet — patterns will surface here as Fyralis identifies them."}
        ]
    open_arcs = [a for a in arcs if a["status"] == "open"]
    resolved = [a for a in arcs if a["status"] == "resolved"]
    text = (
        f"{len(open_arcs)} open arc{'s' if len(open_arcs) != 1 else ''}, "
        f"{len(resolved)} resolved."
    )
    return [{"kind": "text", "text": text}]


# ---------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------


async def build_history(
    *,
    tenant_id: UUID,
    period: str = "90d",
    conn: asyncpg.Connection,
) -> HistoryPayload:
    """Read the substrate; return the full History payload for one tenant."""
    now = datetime.now(timezone.utc)
    if period not in ("7d", "30d", "90d", "365d", "all"):
        period = "90d"
    cutoff = _cutoff_for(period, now)

    state_change_events = await _fetch_state_change_events(
        tenant_id=tenant_id, cutoff=cutoff, conn=conn,
    )

    model_event_rows = await conn.fetch(_MODEL_EVENTS_SQL, tenant_id, cutoff)
    model_events = _model_event_records(model_event_rows)

    # Combine + sort newest first.
    events = state_change_events + model_events
    events.sort(key=lambda e: e["timestamp"], reverse=True)
    # Cap so the page stays snappy. Older events fall out of view.
    events = events[:300]

    # Build a lookup so arc.events can reference the correct event id
    # (state_change events are keyed by `evt-<obs_id>`).
    obs_id_to_event_id: dict[UUID, str] = {}
    for e in events:
        eid = e["id"]
        if eid.startswith("evt-") and len(eid) == 4 + 36:
            try:
                obs_id_to_event_id[UUID(eid[4:])] = eid
            except ValueError:
                continue

    prediction_rows = await conn.fetch(_PREDICTIONS_SQL, tenant_id, cutoff)
    predictions = _build_predictions(prediction_rows)

    arc_rows = await conn.fetch(_ARCS_SQL, tenant_id)
    arcs = _build_arcs(arc_rows, obs_id_to_event_id=obs_id_to_event_id)

    calibration = await _build_calibration(
        tenant_id=tenant_id, cutoff=cutoff, conn=conn,
    )

    correct = sum(1 for p in predictions if p["status"] == "correct")
    resolved_total = sum(1 for p in predictions if p["status"] != "pending")
    layer_counts = {
        "chronicle": {
            "events": len(events),
            "period_label": {
                "7d": "this week",
                "30d": "this month",
                "90d": "this quarter",
                "365d": "this year",
                "all": "all time",
            }.get(period, "this period"),
        },
        "predictions": {
            "calibration": calibration.get("overall") or 0.0,
            "correct": correct,
            "total": resolved_total,
        },
        "arcs": {
            "active": sum(1 for a in arcs if a["status"] == "open"),
            "resolved": sum(1 for a in arcs if a["status"] == "resolved"),
        },
    }

    return HistoryPayload(
        events=events,
        predictions=predictions,
        arcs=arcs,
        calibration=calibration,
        layer_counts=layer_counts,
        chronicle_statement=_chronicle_statement(
            period=period, events=events, arcs=arcs,
        ),
        predictions_statement=_predictions_statement(calibration),
        arcs_statement=_arcs_statement(arcs),
        period=period,
    )
