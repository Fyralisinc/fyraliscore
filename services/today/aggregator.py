"""services/today/aggregator.py — derive the Today payload.

The Today UI consumes a single `TodayResponse` shape (see
ui/src/api/today-types.ts). Backend builds that shape by:

  1. Listing active recommendations for the actor (services.recommendations.repo)
  2. Mapping each recommendation to the spec's card shape — severity is
     derived from `impact × confidence`, kind label from proposition_kind
     and target_act_ref.type, tag from age + qualitative_impact
  3. Pulling in supporting events for the evidence section
  4. Computing signal-strip metrics from commitments, calibration, and
     financial Resources
  5. Computing vitals from active recommendation counts + commit health

This module is a translator. It owns no state — every call is a fresh
read against the substrate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from services.recommendations.repo import RecommendationView, list_for_actor


# ---------------------------------------------------------------------
# Output shape — JSON-serializable dicts, mirrors today-types.ts
# ---------------------------------------------------------------------


@dataclass
class TodayPayload:
    brand: dict[str, Any]
    page: dict[str, Any]
    signal_strip: list[dict[str, Any]]
    vitals: list[dict[str, Any]]
    nav: list[dict[str, Any]]
    cards: list[dict[str, Any]]
    cleared_today: int = 0
    just_updated: dict[str, Any] | None = None
    routed_coda: dict[str, Any] | None = None
    ask_suggestions: list[str] = field(default_factory=list)
    calibration_alert: dict[str, Any] | None = None
    empty_state: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "brand": self.brand,
            "page": self.page,
            "signal_strip": self.signal_strip,
            "vitals": self.vitals,
            "nav": self.nav,
            "cards": self.cards,
            "cleared_today": self.cleared_today,
            "ask_suggestions": self.ask_suggestions,
        }
        if self.just_updated is not None:
            out["just_updated"] = self.just_updated
        if self.routed_coda is not None:
            out["routed_coda"] = self.routed_coda
        if self.calibration_alert is not None:
            out["calibration_alert"] = self.calibration_alert
        if self.empty_state is not None:
            out["empty_state"] = self.empty_state
        return out


# ---------------------------------------------------------------------
# Severity derivation
# ---------------------------------------------------------------------

# Severity bucketing. Two regimes are supported because the substrate
# encodes `expected_impact` two different ways:
#
#   1. Normalized form (0..1) — older fixtures + some Think outputs.
#      Severity = impact × confidence:
#        critical  >= 0.80  — both high impact AND very high confidence
#        strategic >= 0.55
#        high      >= 0.30
#        med       >= 0.12
#        low       <  0.12
#
#   2. Dollar form (>1.0) — the demo recommendations encode actual ARR
#      exposure. Multiplying that by `confidence` yields scores in the
#      hundreds of thousands and saturates everything to "critical",
#      which paints every card red and erases the urgency signal.
#      Bucket by dollar bands instead, with confidence as a softener:
#        critical  >= $1M  AND  confidence >= 0.70
#        strategic >= $250K
#        high      >= $50K
#        med       >= $10K
#        low       <  $10K
def _derive_severity(view: RecommendationView) -> str:
    impact = view.expected_impact if view.expected_impact is not None else 0.5
    conf = view.confidence

    # Dollar regime — impact > 1.0 means it's not a probability.
    if impact > 1.0:
        if impact >= 1_000_000 and conf >= 0.70:
            return "critical"
        if impact >= 250_000:
            return "strategic"
        if impact >= 50_000:
            return "high"
        if impact >= 10_000:
            return "med"
        return "low"

    # Normalized regime — impact * confidence.
    score = impact * conf
    if score >= 0.80:
        return "critical"
    if score >= 0.55:
        return "strategic"
    if score >= 0.30:
        return "high"
    if score >= 0.12:
        return "med"
    return "low"


# Operational vs strategic split. The filter strip uses these.
# Strategic: severity is strategic; or kind label suggests strategy.
# Operational: everything else (decision drift, customer reciprocity,
# quick approvals, vp signal conflicts, etc).
_STRATEGIC_OPS = {"create", "archive"}


def _derive_category(view: RecommendationView, severity: str) -> str:
    if severity == "strategic":
        return "strategic"
    op = (view.proposed_change or {}).get("operation")
    if op in _STRATEGIC_OPS:
        return "strategic"
    return "operational"


# ---------------------------------------------------------------------
# Kind label derivation
# ---------------------------------------------------------------------

_OPERATION_TO_KIND_PREFIX = {
    "transition": {
        "commitment": "Commitment shift",
        "goal":       "Goal direction",
        "decision":   "Decision drift",
    },
    "create": {
        "goal": "Strategic · feature",
    },
    "archive": {
        "decision": "Strategic · prioritization",
    },
    "update": {
        "resource": "Resource update",
    },
}


def _derive_kind_label(view: RecommendationView) -> str:
    op = (view.proposed_change or {}).get("operation")
    ref_type = (view.target_act_ref or {}).get("type")
    if op in _OPERATION_TO_KIND_PREFIX and ref_type in _OPERATION_TO_KIND_PREFIX[op]:
        prefix = _OPERATION_TO_KIND_PREFIX[op][ref_type]
        if view.target_entity is not None and ref_type == "decision":
            return f"{prefix} · {_short_id(view.target_entity.id)}"
        return prefix
    return "Recommendation"


def _short_id(uuid_val: UUID) -> str:
    s = str(uuid_val)
    return f"{s.split('-', 1)[0][:5]}"


# ---------------------------------------------------------------------
# Tag derivation
# ---------------------------------------------------------------------


def _derive_tag(view: RecommendationView, now: datetime) -> dict[str, str] | None:
    """`new` if the recommendation crossed Fyralis's action threshold
    in the last 24 hours; `quiet` weak-calibration / routed-to-you tag
    otherwise; None when there's nothing notable to surface."""
    age = now - view.created_at
    if age.total_seconds() < 24 * 3600:
        return {"kind": "new", "label": "new"}
    if view.confidence < 0.6:
        return {"kind": "quiet", "label": "weak calibration"}
    return {"kind": "quiet", "label": "routed to you"}


# ---------------------------------------------------------------------
# Action set derivation
# ---------------------------------------------------------------------


def _derive_actions(view: RecommendationView) -> list[str]:
    """Per spec §5.1 — actions are decided at substrate-output time and
    encoded in the data. v1 picks a sensible default per recommendation
    shape: every card supports Act + Hold; Route appears when there's
    a recipient mentioned in scope_actors; Snooze appears for VP-signal
    style items; Dismiss is allowed on strategic items."""
    actions: list[str] = ["act", "hold"]
    op = (view.proposed_change or {}).get("operation")
    ref_type = (view.target_act_ref or {}).get("type")
    if op == "create" or op == "archive":
        actions.append("dismiss")
    elif ref_type == "commitment" and op == "transition":
        actions.append("route")
    elif ref_type == "decision":
        actions.append("route")
    else:
        actions.append("snooze")
    return actions


# ---------------------------------------------------------------------
# Stats block derivation
# ---------------------------------------------------------------------


def _derive_stats(view: RecommendationView) -> list[dict[str, str]]:
    stats: list[dict[str, str]] = [
        {
            "label": "Confidence",
            "value": f"{int(round(view.confidence * 100))}%",
        }
    ]
    if view.expected_impact is not None:
        stats.append(
            {
                "label": "Expected impact",
                "value": f"{view.expected_impact:.2f}",
                "tone": "amber" if view.expected_impact > 0.6 else "default",
            }
        )
    elif view.qualitative_impact is not None:
        stats.append(
            {
                "label": "Impact",
                "value": view.qualitative_impact[:32],
                "tone": "default",
            }
        )
    target = view.target_entity
    if target is not None:
        label = "Target"
        stats.append(
            {
                "label": label,
                "value": (target.title or target.type)[:42],
                "tone": "default",
            }
        )
    return stats


# ---------------------------------------------------------------------
# Suggested paths derivation
# ---------------------------------------------------------------------


_OP_LABEL = {
    "transition": "Reaffirm",
    "create":     "Adopt",
    "archive":    "Reject",
    "update":     "Revisit",
}


def _derive_paths(view: RecommendationView) -> list[dict[str, str]]:
    op = (view.proposed_change or {}).get("operation")
    payload = (view.proposed_change or {}).get("payload") or {}
    ref_type = (view.target_act_ref or {}).get("type")
    label = _OP_LABEL.get(op or "transition", "Reaffirm")
    target_label = view.target_entity.title if view.target_entity else (ref_type or "this")
    primary_body: str
    if op == "transition":
        new_state = payload.get("new_state", "the new state")
        primary_body = (
            f"<strong>Move <em>{target_label}</em> to {new_state}</strong>. "
            f"This is the change I'm recommending."
        )
    elif op == "create":
        title = payload.get("title", "the proposed item")
        primary_body = f"<strong>Create <em>{title}</em></strong> as scoped above."
    elif op == "archive":
        primary_body = (
            f"<strong>Archive <em>{target_label}</em></strong>. "
            f"<em>Removes it from active consideration.</em>"
        )
    elif op == "update":
        primary_body = (
            f"<strong>Apply the update to <em>{target_label}</em></strong>."
        )
    else:
        primary_body = "<strong>Take the recommended action</strong>."

    return [
        {"id": "p-act",  "label": label,  "body_html": primary_body},
        {
            "id": "p-defer",
            "label": "Wait",
            "body_html": (
                "<strong>Wait for one more data point</strong>. "
                "<em>I'll surface again if the signal sharpens or fades.</em>"
            ),
        },
        {
            "id": "p-reject",
            "label": "Reject",
            "body_html": (
                "<strong>Tell me I'm reading this wrong</strong> — and I'll recalibrate."
            ),
        },
    ]


# ---------------------------------------------------------------------
# Evidence derivation — pull supporting Observations
# ---------------------------------------------------------------------


_EVIDENCE_SQL = """
SELECT id, occurred_at, kind, source_channel, content_text
FROM observations
WHERE id = ANY($1::uuid[]) AND tenant_id = $2
ORDER BY occurred_at ASC
LIMIT 5
"""


async def _fetch_evidence(
    *, ids: list[UUID], tenant_id: UUID, conn: asyncpg.Connection,
) -> list[dict[str, str]]:
    if not ids:
        return []
    rows = await conn.fetch(_EVIDENCE_SQL, ids, tenant_id)
    out: list[dict[str, str]] = []
    for r in rows:
        occurred = r["occurred_at"]
        date_part = occurred.strftime("%b %-d").lower() if occurred else ""
        kind_part = (r["source_channel"] or r["kind"] or "signal")[:14].lower()
        out.append(
            {
                "src": f"{date_part} · {kind_part}",
                "quote_html": _truncate(r["content_text"] or "", 240),
                "attribution": r["source_channel"] or "",
            }
        )
    return out


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


# ---------------------------------------------------------------------
# Supporting-model fetch + reasoning-chain renderer
# ---------------------------------------------------------------------


_SUPPORTING_MODELS_SQL = """
SELECT id, "natural", confidence, proposition_kind,
       proposition->>'qualitative_impact' AS qualitative_impact
FROM models
WHERE id = ANY($1::uuid[])
  AND tenant_id = $2
  AND status = 'active'
"""


# Section ordering for the reasoning chain. Each entry maps a
# proposition_kind to the human-facing heading used in the rendered
# narrative. Order matters: the chain reads as state → relation →
# pattern → concern → prediction → recommendation, which mirrors the
# epistemic flow ("here's what's happening, here's how it connects,
# here's the pattern, here's what worries me, here's what I expect,
# here's what I'm asking you to do").
_SECTION_ORDER: list[tuple[str, str]] = [
    ("state",                 "What I'm seeing right now"),
    ("relation",              "How the pieces connect"),
    ("pattern",               "The pattern this fits"),
    ("pattern_instance",      "How it shows up here"),
    ("capability_assessment", "What this says about us"),
    ("hypothesis",            "What I'm hypothesizing"),
    ("concern",               "What worries me"),
    ("prediction",            "What I think happens next"),
    ("market_assessment",     "Market backdrop"),
    ("environmental_trend",   "Environmental trend"),
]


async def _fetch_supporting_models(
    *, ids: list[UUID], tenant_id: UUID, conn: asyncpg.Connection,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch the named supporting Models for a recommendation, grouped
    by proposition_kind so the reasoning chain can render them in the
    canonical order."""
    if not ids:
        return {}
    rows = await conn.fetch(_SUPPORTING_MODELS_SQL, ids, tenant_id)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        grouped.setdefault(r["proposition_kind"] or "unknown", []).append(
            {
                "natural": r["natural"] or "",
                "confidence": float(r["confidence"] or 0.0),
            }
        )
    # Stable sort within each kind: highest-confidence first.
    for k in grouped:
        grouped[k].sort(key=lambda m: -m["confidence"])
    return grouped


def _render_reasoning_chain(
    view: RecommendationView,
    models_by_kind: dict[str, list[dict[str, Any]]],
) -> str:
    """Compose a multi-section reasoning chain from the recommendation's
    supporting Models. Falls back gracefully when no supporting Models
    are wired (the chain shrinks to opening + closing only)."""
    parts: list[str] = []

    # Opening: short framing sentence so the reader has context.
    target_label = (
        _escape(view.target_entity.title) if view.target_entity else "this"
    )
    impact = view.expected_impact
    if impact:
        impact_str = _format_impact_short(impact)
        parts.append(
            f"<p class=\"reasoning-lede\">"
            f"<strong>{impact_str}</strong> rides on "
            f"<em>{target_label}</em>. Here is how I got there.</p>"
        )
    else:
        parts.append(
            f"<p class=\"reasoning-lede\">"
            f"Here is how I am reading <em>{target_label}</em> right now.</p>"
        )

    # Sectioned middle. Walk the canonical order and only emit sections
    # we actually have evidence for.
    for kind, heading in _SECTION_ORDER:
        bucket = models_by_kind.get(kind, [])
        if not bucket:
            continue
        # Cap to 2 entries per section so the panel doesn't sprawl.
        items = bucket[:2]
        bullets = "".join(
            f"<li>{_escape(m['natural'])} "
            f"<span class=\"reasoning-conf\">"
            f"({int(round(m['confidence'] * 100))}%)"
            f"</span></li>"
            for m in items if m["natural"].strip()
        )
        if not bullets:
            continue
        parts.append(
            f"<h4 class=\"reasoning-heading\">{_escape(heading)}</h4>"
            f"<ul class=\"reasoning-list\">{bullets}</ul>"
        )

    # Closing: what I'm asking the user to do, in plain language.
    closing_action = _action_phrase(view)
    parts.append(
        f"<h4 class=\"reasoning-heading\">What I'm asking you to do</h4>"
        f"<p class=\"reasoning-action\">{_escape(closing_action)}</p>"
    )

    return "\n".join(parts)


def _format_impact_short(usd: float) -> str:
    abs_v = abs(usd)
    if abs_v >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if abs_v >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


# ---------------------------------------------------------------------
# Card builder
# ---------------------------------------------------------------------


async def _build_card(
    view: RecommendationView,
    *,
    now: datetime,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    severity = _derive_severity(view)
    category = _derive_category(view, severity)
    evidence = await _fetch_evidence(
        ids=view.supporting_event_ids[:5], tenant_id=tenant_id, conn=conn,
    )
    supporting_models = await _fetch_supporting_models(
        ids=view.supporting_model_ids[:12], tenant_id=tenant_id, conn=conn,
    )
    headline_html = _render_headline(view)
    supporting_html = _render_supporting(view)
    detail = {
        "reasoning_html": _render_reasoning_chain(view, supporting_models),
        "evidence": evidence if evidence else None,
        "evidence_label": "The signals" if evidence else None,
        "confidence": [
            {
                "label": "On pattern",
                "value_html": (
                    f"{int(round(view.confidence * 100))}% — "
                    f"<em>{view.qualitative_impact or 'derived from supporting signals'}</em>"
                ),
            },
            {
                "label": "On action",
                "value_html": (
                    f"{int(round(min(view.confidence + 0.05, 0.95) * 100))}% — "
                    f"<em>{_action_phrase(view)}</em>"
                ),
            },
            {
                "label": "Falsifier",
                "value_html": "<em>I'd revise down if a stronger contrary signal appears.</em>",
            },
        ],
        "paths": _derive_paths(view),
        "show_ask": True,
        # Driftwood revision: substrate-suggested probes shown above the
        # in-card Ask field. The legacy detail sections (reasoning,
        # evidence, confidence, paths) are still emitted for clients on
        # the old contract, but the revised UI ignores them in favour of
        # on-demand probe responses.
        "probe_chips": _derive_probe_chips(view),
        # Stable per-card conversation id so the UI can persist
        # exchanges across sessions. We derive from the recommendation
        # id deterministically — there's exactly one conversation per
        # (actor, card), and the actor scoping happens at the API layer.
        "conversation_id": f"conv-{view.id}",
    }
    # Decorate phrases with <probe> markup so they're clickable.
    headline_html = _add_probe_markup(headline_html, str(view.id), kind_hint=view)
    if supporting_html:
        supporting_html = _add_probe_markup(
            supporting_html, str(view.id), kind_hint=view, prefix="s"
        )
    # Drop None entries from detail
    detail = {k: v for k, v in detail.items() if v is not None}

    return {
        "id": str(view.id),
        "severity": severity,
        "category": category,
        "kind_label": _derive_kind_label(view),
        "meta": _derive_meta(view),
        "tag": _derive_tag(view, now),
        "headline_html": headline_html,
        "supporting_html": supporting_html,
        "stats": _derive_stats(view),
        "expand_cta": "See evidence" if evidence else "Open",
        "actions": _derive_actions(view),
        "detail": detail,
    }


def _derive_probe_chips(view: RecommendationView) -> list[dict[str, str]]:
    """Driftwood revision §4: emit 3–5 substrate-suggested probes per
    card. Chips are kind-specific so they don't read generic. Probe ids
    are stable per (card, chip) so the persistence layer can dedupe
    used chips across sessions.

    The handler in services.conversations resolves these ids back to a
    response — for v1 the resolution is a deterministic template that
    quotes the recommendation; future iterations route to the QRY
    pathway with the chip text as the seed query.
    """
    rid = str(view.id)
    pk = (view.proposition_kind or "").lower()
    if "decision" in pk or "drift" in pk:
        return [
            {"id": f"{rid}:why", "text": "Why this decision specifically?"},
            {"id": f"{rid}:contradicting", "text": "What's contradicting it?"},
            {"id": f"{rid}:history", "text": "Have we ratified before?"},
            {"id": f"{rid}:drift-cost", "text": "What if I let it drift?"},
        ]
    if "feature" in pk or "strategic" in pk:
        return [
            {"id": f"{rid}:why-pattern", "text": "Why this pattern matters?"},
            {"id": f"{rid}:customer-asks", "text": "Show me the customer asks"},
            {"id": f"{rid}:cost", "text": "What's the engineering cost?"},
            {"id": f"{rid}:change-mind", "text": "What would change your mind?"},
        ]
    if "personnel" in pk or "people" in pk:
        return [
            {"id": f"{rid}:signals", "text": "Show me the signals"},
            {"id": f"{rid}:calibration", "text": "Your calibration on personnel"},
            {"id": f"{rid}:wait", "text": "What if I wait?"},
        ]
    return [
        {"id": f"{rid}:why", "text": "Why are you flagging this?"},
        {"id": f"{rid}:evidence", "text": "Show me the evidence"},
        {"id": f"{rid}:options", "text": "What are my options?"},
    ]


def _add_probe_markup(
    html: str, card_id: str, *, kind_hint: Any = None, prefix: str = "h",
) -> str:
    """Wrap each `<em>...</em>` in the given HTML with a `<span
    data-probe-id="...">` so the UI can render it as a probable phrase.

    We deliberately layer on top of the existing `<em>` tags rather
    than re-tokenising the proposition text — keeps the change to the
    aggregator narrow and means typography (emphasis) is preserved.

    Probe ids are derived from the slugged phrase text so the same
    phrase produces the same id across renders. Collisions across
    multiple ems are resolved by appending a 1-based index.
    """
    import re
    counter = {"i": 0}

    def repl(m: "re.Match[str]") -> str:
        counter["i"] += 1
        text = m.group(1)
        slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:32] or f"p{counter['i']}"
        pid = f"{prefix}-{card_id}-{slug}-{counter['i']}"
        # Keep the <em> wrapper inside the span so the visual emphasis
        # carries through; the dotted underline lives on the span.
        return f'<span data-probe-id="{pid}"><em>{text}</em></span>'

    return re.sub(r"<em>([^<]+)</em>", repl, html)


def _escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _render_headline(view: RecommendationView) -> str:
    text = _escape(view.proposition_text)
    target = view.target_entity
    if target and target.title:
        title_safe = _escape(target.title)
        if title_safe in text:
            text = text.replace(title_safe, f"<em>{title_safe}</em>", 1)
    return text


def _render_supporting(view: RecommendationView) -> str | None:
    qi = view.qualitative_impact
    if not qi:
        return None
    return f"<em>{_escape(qi)}</em>"


def _action_phrase(view: RecommendationView) -> str:
    """Plain-language statement of the concrete action being asked of the
    user — derived from the recommendation's `proposed_change` so it
    actually says what to do (e.g. "Move SSO partners commitment to
    blocked"), not a generic line about reversibility."""
    op = (view.proposed_change or {}).get("operation")
    payload = (view.proposed_change or {}).get("payload") or {}
    ref_type = (view.target_act_ref or {}).get("type")
    target_label = view.target_entity.title if view.target_entity else (ref_type or "this")
    if op == "transition":
        new_state = payload.get("new_state")
        if new_state:
            return f"Move {target_label} to {new_state}."
        return f"Transition {target_label} to the proposed state."
    if op == "create":
        title = payload.get("title") or "the proposed item"
        return f"Create {title} as scoped above."
    if op == "archive":
        return f"Archive {target_label} so it stops drawing attention."
    if op == "update":
        field_name = payload.get("field") or "the value"
        return f"Update {field_name} on {target_label}."
    return "Take the action above so the signal does not keep accumulating."


def _derive_meta(view: RecommendationView) -> str | None:
    target = view.target_entity
    if target is None:
        return None
    if target.type == "commitment" and target.state:
        return f"{target.type} · {target.state}"
    return target.type


# ---------------------------------------------------------------------
# Signal strip
# ---------------------------------------------------------------------


async def _commitments_metric(
    *, tenant_id: UUID, target_actor: UUID, conn: asyncpg.Connection,
) -> dict[str, Any]:
    """Fraction of active commitments whose state is on-track (active /
    proposed) vs slipping (blocked / paused). Active state buckets per
    lib/shared/types.py CommitmentState."""
    on_track_states = ("active", "doneunverified", "doneverified")
    rows = await conn.fetch(
        """
        SELECT state, count(*) AS n
        FROM commitments
        WHERE tenant_id = $1 AND terminal_at IS NULL
        GROUP BY state
        """,
        tenant_id,
    )
    by_state = {r["state"]: r["n"] for r in rows}
    total = sum(by_state.values())
    on_track = sum(by_state.get(s, 0) for s in on_track_states)
    slipping = total - on_track
    if total == 0:
        return {
            "id": "commitments",
            "label": "Commitments",
            "value": "—",
            "trend_html": "no active commitments",
            "tone": "default",
            "unavailable": True,
        }
    tone = "amber" if slipping >= 3 else ("default" if slipping > 0 else "accent")
    trend = (
        f"↓ <em>{slipping} slipped</em> this week"
        if slipping > 0
        else "all on track"
    )
    return {
        "id": "commitments",
        "label": "Commitments",
        "value": f"{on_track} / {total}",
        "trend_html": trend,
        "tone": tone,
    }


async def _calibration_metric(
    *, tenant_id: UUID, target_actor: UUID, conn: asyncpg.Connection,
) -> dict[str, Any]:
    """Substrate-wide calibration. We use the actor's last-30-day mean
    confidence_at_assertion on resolved Models with a resolution_outcome
    as a proxy for v1; once calibration_offsets has stable sample sizes,
    this can read from there directly."""
    row = await conn.fetchrow(
        """
        SELECT avg(confidence_at_assertion) AS mean_conf, count(*) AS n
        FROM models
        WHERE tenant_id = $1 AND resolved_at IS NOT NULL
          AND resolved_at > now() - interval '30 days'
        """,
        tenant_id,
    )
    n = (row or {}).get("n") or 0
    mean = (row or {}).get("mean_conf")
    if not mean or n < 5:
        return {
            "id": "calibration",
            "label": "My calibration",
            "value": "—",
            "trend_html": "calibration window still warming",
            "unavailable": True,
        }
    return {
        "id": "calibration",
        "label": "My calibration",
        "value": f"{mean:.2f}",
        "trend_html": f"<em>{n}</em> resolved · last 30 days",
        "tone": "default",
    }


async def _financial_resource_metric(
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    label: str,
    identity_match: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT current_value, last_updated_at
        FROM resources
        WHERE tenant_id = $1
          AND kind = 'financial'
          AND identity ILIKE $2
          AND archived_at IS NULL
        ORDER BY last_updated_at DESC
        LIMIT 1
        """,
        tenant_id, f"%{identity_match}%",
    )
    if row is None:
        return None
    cv = row["current_value"] or {}
    if isinstance(cv, str):
        import json
        try:
            cv = json.loads(cv)
        except json.JSONDecodeError:
            cv = {}
    if not isinstance(cv, dict):
        return None
    value = cv.get("value")
    unit = cv.get("unit", "")
    if value is None:
        return None
    return {
        "id": label.lower(),
        "label": label,
        "value": str(value),
        "value_unit": unit if unit else None,
        "trend_html": "current snapshot",
        "tone": "default",
    }


async def _build_signal_strip(
    *, tenant_id: UUID, target_actor: UUID, conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    arr = (
        await _financial_resource_metric(
            tenant_id=tenant_id, conn=conn, label="ARR", identity_match="ARR",
        )
        or {
            "id": "arr", "label": "ARR", "value": "—",
            "trend_html": "no ARR resource configured",
            "unavailable": True,
        }
    )
    runway = (
        await _financial_resource_metric(
            tenant_id=tenant_id, conn=conn, label="Runway", identity_match="runway",
        )
        or {
            "id": "runway", "label": "Runway", "value": "—",
            "trend_html": "no runway resource configured",
            "unavailable": True,
        }
    )
    commits = await _commitments_metric(
        tenant_id=tenant_id, target_actor=target_actor, conn=conn,
    )
    cal = await _calibration_metric(
        tenant_id=tenant_id, target_actor=target_actor, conn=conn,
    )
    return [arr, runway, commits, cal]


# ---------------------------------------------------------------------
# Vitals zone
# ---------------------------------------------------------------------


async def _build_vitals(
    *,
    tenant_id: UUID,
    target_actor: UUID,
    recommendations: list[RecommendationView],
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    """Five rows: what Fyralis is currently watching for the actor."""
    drift_count = sum(
        1
        for v in recommendations
        if (v.target_act_ref or {}).get("type") == "decision"
    )
    pattern_threshold = sum(
        1 for v in recommendations if v.confidence >= 0.7 and v.expected_impact and v.expected_impact >= 0.5
    )
    held = await conn.fetchval(
        """
        SELECT count(*)
        FROM models
        WHERE tenant_id = $1 AND target_actor_id = $2
          AND status = 'archived' AND archive_reason = 'manual'
        """,
        tenant_id, target_actor,
    ) or 0
    slipping = await conn.fetchval(
        """
        SELECT count(*)
        FROM commitments
        WHERE tenant_id = $1 AND state IN ('blocked','paused')
          AND terminal_at IS NULL
        """,
        tenant_id,
    ) or 0
    total = await conn.fetchval(
        """
        SELECT count(*)
        FROM commitments
        WHERE tenant_id = $1 AND terminal_at IS NULL
        """,
        tenant_id,
    ) or 0
    customer_at_risk = await conn.fetchval(
        """
        SELECT count(*)
        FROM resources
        WHERE tenant_id = $1 AND kind = 'relational'
          AND (metadata->>'health' = 'at_risk' OR utilization_state = 'depleted')
          AND archived_at IS NULL
        """,
        tenant_id,
    ) or 0

    rows: list[dict[str, Any]] = []
    if customer_at_risk > 0:
        rows.append({"id": "v1", "label": "Customers at risk", "value": f"{customer_at_risk}", "tone": "warn"})
    rows.append({
        "id": "v2", "label": "Decision drift",
        "value": f"{drift_count} active" if drift_count else "none",
        "tone": "amber" if drift_count >= 2 else "default",
    })
    rows.append({
        "id": "v3", "label": "Slipping commits",
        "value": f"{slipping} of {total}",
        "tone": "amber" if slipping > 0 else "default",
    })
    rows.append({
        "id": "v4", "label": "Pattern threshold",
        "value": f"{pattern_threshold} forming" if pattern_threshold else "—",
    })
    rows.append({
        "id": "v5", "label": "Held by you",
        "value": f"{held} items" if held else "0",
    })
    return rows


# ---------------------------------------------------------------------
# Page header / state line
# ---------------------------------------------------------------------


def _build_page_header(
    *, recommendations: list[RecommendationView], now: datetime,
) -> dict[str, Any]:
    crit = sum(1 for v in recommendations if _derive_severity(v) == "critical")
    strat = sum(1 for v in recommendations if _derive_severity(v) == "strategic")
    total = len(recommendations)

    if total == 0:
        tone = "quiet"
        text = "Quiet morning. Nothing pressing — I'll surface again if anything material changes."
    elif crit >= 1:
        tone = "tense"
        if total > 1:
            text = f"{total} items need you today; {strat} are strategic."
        else:
            text = "One critical item is the main thing on my mind."
    elif strat >= 2:
        tone = "unsettled"
        text = f"Heavy day. {strat} strategic decisions and {total - strat} operational."
    elif strat >= 1:
        tone = "loaded"
        text = f"{strat} strategic item to think about; {total - strat} operational."
    else:
        tone = "steady"
        text = f"Slow morning — {total} small things, none urgent."

    return {
        "date_label": now.strftime("%A, %B %-d.").rstrip("."),
        "state_tone": tone,
        "state_text": text,
    }


# ---------------------------------------------------------------------
# Nav menu (static for v1 — Today is the only active surface)
# ---------------------------------------------------------------------


def _build_nav(
    *, today_count: int, hold_count: int,
) -> list[dict[str, Any]]:
    return [
        {
            "id": "operate",
            "label": "Operate",
            "items": [
                {"id": "today",     "label": "Today",     "active": True,  "badge": str(today_count), "shortcut": "⌘7"},
                {"id": "structure", "label": "Structure"},
                {"id": "history",   "label": "History"},
                {"id": "hold",      "label": "Hold",      "badge": str(hold_count), "shortcut": "⌘3"},
            ],
        },
        {
            "id": "communicate",
            "label": "Communicate",
            "items": [
                {"id": "threads",   "label": "Threads",   "disabled": True, "badge": "soon"},
                {"id": "people",    "label": "People",    "disabled": True, "badge": "soon"},
                {"id": "customers", "label": "Customers", "disabled": True, "badge": "soon"},
            ],
        },
        {
            "id": "account",
            "label": "Account",
            "items": [
                {"id": "ledger",  "label": "Ledger",  "disabled": True, "badge": "soon"},
                {"id": "capital", "label": "Capital", "disabled": True, "badge": "soon"},
            ],
        },
    ]


# ---------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------


async def build_today(
    *,
    tenant_id: UUID,
    actor_id: UUID,
    actor_display_name: str | None,
    brand_name: str = "Fyralis",
    conn: asyncpg.Connection,
    limit: int = 12,
    days_since_inception: int = 1,
    cleared_today: int = 0,
) -> TodayPayload:
    """Read the substrate; return the full Today payload for one actor."""
    now = datetime.now(timezone.utc)

    recommendations = await list_for_actor(
        tenant_id=tenant_id,
        target_actor_id=actor_id,
        limit=limit,
        conn=conn,
    )

    cards = [
        await _build_card(v, now=now, tenant_id=tenant_id, conn=conn)
        for v in recommendations
    ]

    signal_strip = await _build_signal_strip(
        tenant_id=tenant_id, target_actor=actor_id, conn=conn,
    )
    vitals = await _build_vitals(
        tenant_id=tenant_id,
        target_actor=actor_id,
        recommendations=recommendations,
        conn=conn,
    )
    page = _build_page_header(recommendations=recommendations, now=now)

    held = await conn.fetchval(
        """
        SELECT count(*)
        FROM models
        WHERE tenant_id = $1 AND target_actor_id = $2
          AND status = 'archived' AND archive_reason = 'manual'
        """,
        tenant_id, actor_id,
    ) or 0

    nav = _build_nav(today_count=len(cards), hold_count=held)

    # Just-updated banner: show the most recent Models the Think loop
    # produced in the last 10 minutes. Without this, status-update
    # signals (e.g. "I started working on rate limiting") look like the
    # system ignored them — Think DID emit a state Model, the user
    # just had no UI feedback that anything happened.
    just_updated = await _build_just_updated(
        tenant_id=tenant_id, conn=conn, now=now,
    )

    # Calibration alert per spec §10.7 — show if mean calibration < 0.6.
    cal_metric = next((m for m in signal_strip if m.get("id") == "calibration"), None)
    calibration_alert: dict[str, Any] | None = None
    if (
        cal_metric is not None
        and not cal_metric.get("unavailable")
        and float(cal_metric["value"]) < 0.6
    ):
        calibration_alert = {
            "text": (
                "I'm noticing my track record has weakened recently. "
                "Treat my recommendations with extra skepticism today."
            )
        }

    return TodayPayload(
        brand={
            "name": brand_name,
            "mark": (brand_name or "D")[0].upper(),
            "pulse_day": days_since_inception,
        },
        page=page,
        signal_strip=signal_strip,
        vitals=vitals,
        nav=nav,
        cards=cards,
        cleared_today=cleared_today,
        ask_suggestions=[
            "What are you least sure about?",
            f"What's on Hold I should look at?",
            f"Show me {actor_display_name.split()[0]}'s recent work" if actor_display_name else "Show me what's slipping",
        ],
        calibration_alert=calibration_alert,
        just_updated=just_updated,
        empty_state=(
            None
            if cards
            else {
                "headline": "You're at zero.",
                "body": "Nothing else needs your attention today. I'll surface again if anything material changes.",
            }
        ),
    )


# ---------------------------------------------------------------------
# Just-updated banner
# ---------------------------------------------------------------------


_JUST_UPDATED_SQL = """
SELECT id, "natural", proposition_kind, confidence, created_at
FROM models
WHERE tenant_id = $1
  AND status = 'active'
  AND created_at >= $2
  AND proposition_kind <> 'recommendation'
ORDER BY created_at DESC
LIMIT 5
"""


async def _build_just_updated(
    *, tenant_id: UUID, conn: asyncpg.Connection, now: datetime,
) -> dict[str, Any] | None:
    """Surface a short banner describing what Think just learned. We
    show non-recommendation Models created in the last 10 minutes, since
    recommendations already have their own card; the goal here is to
    confirm to the user that an inbound signal landed and produced
    epistemic state, even when no card was warranted."""
    cutoff = now - timedelta(minutes=10)
    rows = await conn.fetch(_JUST_UPDATED_SQL, tenant_id, cutoff)
    if not rows:
        return None
    items = []
    for r in rows[:3]:
        natural = (r["natural"] or "").strip()
        if not natural:
            continue
        items.append(
            f"<strong>{_escape((r['proposition_kind'] or '').replace('_', ' '))}</strong>"
            f" · {_escape(natural[:160])}"
            f" <span class=\"reasoning-conf\">"
            f"({int(round(float(r['confidence'] or 0.0) * 100))}%)</span>"
        )
    if not items:
        return None
    body = "<br/>".join(items)
    return {
        "text_html": (
            f"<strong>Just learned</strong> · {body}"
        )
    }


