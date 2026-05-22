"""services/gateway/today_routes.py — Today page (v2) endpoints.

Implements the wire contract for the revamped Today page:

  GET    /today
  GET    /today/deltas/{delta_id}
  GET    /today/deltas/{delta_id}/evidence
  POST   /today/deltas/{delta_id}/apply
  POST   /today/deltas/{delta_id}/delegate
  POST   /today/deltas/{delta_id}/correction

The page is an executive re-entry surface centered on Proposed Changes
(internally: Decision Deltas). Existing storage in `decision_deltas` is
preserved unchanged; this router synthesizes the spec's richer wire
shape from those rows + evidence + topology_events + observations.

Synthesis layer
---------------
The spec extends the existing model in three ways:

  * Status taxonomy — spec uses {needs_authority, delegatable,
    monitoring, contested, accepted, delegated, correction_submitted,
    archived, failed_apply}. The DB still stores {proposed, accepted,
    delegated, contested, superseded, dismissed}. Mapping rules are in
    `_synth_status` below. Persisted state is unchanged.

  * Field shape — spec adds title/summaryLine/whyThisMatters/keyMetrics
    /evidenceSummary/missingContext/impactIfAccepted/relatedModelLinks
    /applyPreview. These are all derived from main_assertion,
    current_state, suggested_update, consequence_preview, impact, and
    the evidence rows. New per-delta state (notably correction
    submissions and missingContext lists) lives inside the impact JSONB
    so no schema migration is required to ship the spec.

  * Page-level shape — spec adds a summary strip + handled-without-you
    panel. These are computed from observations + topology_events +
    delta counts in a single read pass per request.

Correction storage
------------------
`POST /today/deltas/{id}/correction` transitions the delta to status
'contested' and patches `impact.correction_submitted = true` plus
`impact.correction = {type, explanation, ...}`. The synth layer then
exposes the spec status as `correction_submitted` so the UI sees the
richer state, while the underlying DB enum stays unchanged.

Auth: tenant from request.state.auth (BearerAuthMiddleware).
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from services.decision_deltas import apply as apply_mod
from services.decision_deltas import repo as dd_repo


# =====================================================================
# Spec enums
# =====================================================================

# UI status — spec §13. The wire shape exposes this; the DB stores the
# narrower legacy enum.
SPEC_STATUSES: frozenset[str] = frozenset({
    "needs_authority",
    "delegatable",
    "monitoring",
    "contested",
    "accepted",
    "delegated",
    "correction_submitted",
    "archived",
    "failed_apply",
})

# Spec ModelCategoryKey (the 8-category lattice shared with Model page).
SPEC_CATEGORIES: tuple[str, ...] = (
    "goals_priorities",
    "commitments",
    "decisions",
    "risks_constraints",
    "customers_revenue",
    "people_teams",
    "systems_capacity",
    "finance_capital",
)

# Map legacy DB categories → spec categories. Open-ended on the right;
# unknown values fall back to commitments (the most common bucket).
_CATEGORY_MAP: dict[str, str] = {
    "customer_risk": "customers_revenue",
    "revenue":       "customers_revenue",
    "pricing":       "customers_revenue",
    "capacity":      "systems_capacity",
    "delivery":      "commitments",
    "decision":      "decisions",
    "strategy":      "goals_priorities",
    "people":        "people_teams",
    "finance":       "finance_capital",
    "risk":          "risks_constraints",
}

# Map target_node_kind → spec category for related-model links.
_KIND_TO_CATEGORY: dict[str, str] = {
    "customer":   "customers_revenue",
    "commitment": "commitments",
    "goal":       "goals_priorities",
    "decision":   "decisions",
    "risk":       "risks_constraints",
    "resource":   "systems_capacity",
    "actor":      "people_teams",
}

_CATEGORY_LABELS: dict[str, str] = {
    "goals_priorities":  "Goals & Priorities",
    "commitments":       "Commitments",
    "decisions":         "Decisions",
    "risks_constraints": "Risks & Constraints",
    "customers_revenue": "Customers & Revenue",
    "people_teams":      "People & Teams",
    "systems_capacity":  "Systems & Capacity",
    "finance_capital":   "Finance & Capital",
}


# =====================================================================
# Status + label synthesis
# =====================================================================


def _synth_status(
    *, db_status: str, label: str, impact: dict[str, Any] | None,
) -> str:
    """Map (db_status, label, impact) → spec status.

    `proposed` rows fan out into {needs_authority, delegatable,
    monitoring} based on the row's label. `contested` rows expose as
    `correction_submitted` when the user submitted a structured
    correction (impact.correction_submitted=True); plain contests stay
    `contested`. Terminal {superseded, dismissed} both expose as
    `archived` — the user-facing surface doesn't need the distinction.
    """
    if db_status == "proposed":
        if label == "authority_required":
            return "needs_authority"
        if label == "recommended_update":
            return "monitoring"
        # `proposed_change` and `needs_review` are delegatable by default.
        return "delegatable"
    if db_status == "accepted":
        return "accepted"
    if db_status == "delegated":
        return "delegated"
    if db_status == "contested":
        if impact and impact.get("correction_submitted") is True:
            return "correction_submitted"
        return "contested"
    if db_status in ("superseded", "dismissed"):
        return "archived"
    # Unknown legacy state: degrade to contested so the UI can show it
    # and the user can clear it.
    return "contested"


def _synth_proposed_by(impact: dict[str, Any] | None) -> str:
    """Default proposer is Fyralis; the user-correction flow may flip
    it to 'user' but that arrives as a contest, not a fresh delta."""
    if impact and impact.get("proposed_by_user") is True:
        return "user"
    return "fyralis"


# =====================================================================
# Diff (Current → Proposed) synthesis
# =====================================================================


def _to_field_list(
    raw: dict[str, Any] | None,
    *,
    fallback_label: str = "Value",
) -> list[dict[str, Any]]:
    """Normalize current_state / suggested_update into the wire's
    DeltaField list shape. The DB stores these as free-form JSONB; we
    accept either {label, value} singletons (legacy) or {fields:[...]}.
    """
    if not raw:
        return []
    # Legacy shape: a single field {label, value, valueType?, severity?}.
    if "fields" not in raw and ("label" in raw or "value" in raw):
        return [_field_row(raw, fallback_label, key="status")]
    # Spec shape: {fields: [{key,label,value,valueType?,severity?}, ...]}
    rows: list[dict[str, Any]] = []
    fields = raw.get("fields") or []
    if isinstance(fields, list):
        for i, f in enumerate(fields):
            if isinstance(f, dict):
                rows.append(_field_row(f, fallback_label, key=str(f.get("key", f"field_{i}"))))
    return rows


def _field_row(
    f: dict[str, Any], fallback_label: str, *, key: str,
) -> dict[str, Any]:
    val = f.get("value")
    return {
        "key":       str(f.get("key", key)),
        "label":     str(f.get("label", fallback_label)),
        "value":     "" if val is None else str(val),
        "valueType": str(f.get("valueType", "text")),
        "severity":  str(f.get("severity", "neutral")),
    }


def _summary_line(
    current: list[dict[str, Any]],
    proposed: list[dict[str, Any]],
) -> str:
    """One-line transition (e.g. "Watch → Critical"). Picks the first
    diffing pair so the UI has a glanceable summary for cards."""
    if proposed and current and proposed[0]["value"] != current[0]["value"]:
        return f"{current[0]['value']} → {proposed[0]['value']}"
    if proposed and not current:
        return f"None → {proposed[0]['value']}"
    if current and not proposed:
        return f"{current[0]['value']} → Removed"
    if proposed:
        return str(proposed[0]["value"])
    return ""


# =====================================================================
# Key metrics synthesis
# =====================================================================


def _format_money(amount: float | int) -> str:
    """Render impact amounts like the spec's "$2.04M ARR" / "$720K"."""
    n = float(amount)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M".replace(".00M", "M")
    if n >= 1_000:
        return f"${int(round(n / 1_000))}K"
    return f"${int(round(n))}"


def _synth_key_metrics(
    *,
    confidence: float | None,
    impact: dict[str, Any] | None,
    evidence_count: int,
) -> list[dict[str, Any]]:
    """Spec §5.3: 3–5 key impact chips. We synthesize from impact + the
    delta's confidence. Order is deliberate: revenue first, then breadth,
    then evidence-strength, then confidence — that's the order the spec
    examples show on the Primary Judgment card."""
    chips: list[dict[str, Any]] = []
    imp = impact or {}
    arr = imp.get("arr_at_risk")
    if isinstance(arr, (int, float)) and arr > 0:
        chips.append({
            "label":    f"{_format_money(arr)} ARR",
            "value":    _format_money(arr),
            "unit":     "ARR",
            "severity": _severity_for_arr(float(arr)),
        })
    accounts = imp.get("accounts_affected") or imp.get("accounts")
    if isinstance(accounts, int) and accounts > 0:
        chips.append({
            "label":    f"{accounts} customer{'s' if accounts != 1 else ''}",
            "value":    accounts,
            "unit":     "customers",
            "severity": "medium" if accounts >= 3 else "low",
        })
    signals = imp.get("signals")
    if not isinstance(signals, int) or signals <= 0:
        signals = evidence_count
    if signals > 0:
        chips.append({
            "label":    f"{signals} signal{'s' if signals != 1 else ''}",
            "value":    signals,
            "unit":     "signals",
            "severity": "medium" if signals >= 8 else "low",
        })
    if isinstance(confidence, (int, float)) and confidence > 0:
        pct = int(round(float(confidence) * 100))
        chips.append({
            "label":    f"{pct}% confidence",
            "value":    pct,
            "unit":     "percent",
            "severity": "high" if pct >= 70 else "medium" if pct >= 50 else "low",
        })
    return chips[:5]


def _severity_for_arr(amount: float) -> str:
    if amount >= 1_000_000:
        return "critical"
    if amount >= 500_000:
        return "high"
    if amount >= 100_000:
        return "medium"
    return "low"


# =====================================================================
# Evidence synthesis
# =====================================================================


_TRUST_TO_QUALITY: dict[str, str] = {
    "attested":   "strong",
    "reputable":  "strong",
    "verified":   "strong",
    "secondhand": "partial",
    "inferred":   "partial",
    "rumored":    "weak",
    "weak":       "weak",
}

_QUALITY_RANK: dict[str, int] = {"weak": 0, "partial": 1, "medium": 2, "strong": 3}


def _evidence_quality(trust_tier: str | None) -> str:
    if not trust_tier:
        return "partial"
    return _TRUST_TO_QUALITY.get(str(trust_tier).lower(), "partial")


def _synth_evidence_summary(
    evidence: Iterable[dd_repo.EvidenceItem],
) -> dict[str, Any]:
    """Group evidence rows by source and aggregate trust into a per-
    group quality + an overall quality. Matches spec §6.6.5."""
    by_source: dict[str, list[dd_repo.EvidenceItem]] = defaultdict(list)
    total = 0
    for ev in evidence:
        by_source[ev.source].append(ev)
        total += 1

    groups: list[dict[str, Any]] = []
    overall_rank = 0
    for src, items in by_source.items():
        qualities = [_evidence_quality(e.trust_tier) for e in items]
        ranks = [_QUALITY_RANK[q] for q in qualities]
        # Group quality = average rank rounded down to a band.
        avg = sum(ranks) / len(ranks) if ranks else 0
        if avg >= 2.5:
            grp_q = "strong"
        elif avg >= 1.5:
            grp_q = "medium"
        elif avg >= 0.5:
            grp_q = "partial"
        else:
            grp_q = "weak"
        overall_rank = max(overall_rank, _QUALITY_RANK[grp_q])
        groups.append({
            "id":            f"src-{src}",
            "label":         _source_label(src),
            "sourceType":    src,
            "count":         len(items),
            "quality":       grp_q,
            "strengthScore": round(avg / 3.0, 2),
        })

    # Overall quality requires breadth too — a single strong source is
    # not strong overall. Cap at "medium" when only one source group.
    if not groups:
        overall = "weak"
    elif len(groups) == 1 and overall_rank >= _QUALITY_RANK["strong"]:
        overall = "medium"
    else:
        overall = {0: "weak", 1: "partial", 2: "medium", 3: "strong"}[overall_rank]

    return {
        "totalSignals": total,
        "quality":      overall,
        "groups":       sorted(
            groups, key=lambda g: -_QUALITY_RANK[g["quality"]],
        ),
    }


def _source_label(source: str) -> str:
    return {
        "crm":               "CRM logs",
        "support":           "Support tickets",
        "email":             "Email & threads",
        "slack":             "Slack threads",
        "linear":            "Linear",
        "github":            "GitHub",
        "calendar":          "Calendar",
        "finance":           "Finance system",
        "product":           "Product events",
        "product_usage":     "Product usage",
        "fyralis":           "Fyralis reasoning",
        "fyralis_reasoning": "Fyralis reasoning",
    }.get(source.lower(), source.replace("_", " ").title())


# =====================================================================
# Missing context + impact-if-accepted synthesis
# =====================================================================


def _synth_missing_context(
    impact: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Pull the missing-context list out of impact JSONB. The UI shows
    "No major context gaps identified." when the list is empty (spec
    §6.6.6) so we don't need to fabricate anything here."""
    if not impact:
        return []
    raw = impact.get("missing_context") or impact.get("missingContext")
    if not isinstance(raw, list):
        return []
    items: list[dict[str, Any]] = []
    for i, m in enumerate(raw):
        if not isinstance(m, dict):
            continue
        text = m.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        items.append({
            "id":            str(m.get("id", f"miss-{i}")),
            "text":          text.strip(),
            "severity":      str(m.get("severity", "medium")),
            "relatedSource": m.get("relatedSource"),
        })
    return items


def _synth_impact_if_accepted(
    *,
    consequence_preview: dict[str, Any] | None,
    target_node_kind: str | None,
) -> list[dict[str, Any]]:
    """Walk consequence_preview.{creates,updates,archives,notifies,
    re_evaluates_in} and tag each entry with a spec operationType. The
    final entry is always a ledger event since accept_and_apply emits
    one unconditionally (apply.py emits a topology_events row)."""
    items: list[dict[str, Any]] = []
    cp = consequence_preview or {}
    creates = cp.get("creates") or []
    updates = cp.get("updates") or []
    archives = cp.get("archives") or []
    notifies = cp.get("notifies") or []

    if isinstance(updates, list):
        for u in updates:
            label = _describe_op(u, verb="Update", default_kind=target_node_kind)
            items.append({
                "id":            f"op-update-{len(items)}",
                "text":          label,
                "operationType": "update_node",
                "severity":      "neutral",
            })
    if isinstance(creates, list):
        for c in creates:
            label = _describe_op(c, verb="Create")
            items.append({
                "id":            f"op-create-{len(items)}",
                "text":          label,
                "operationType": "create_node",
                "severity":      "positive",
            })
    if isinstance(archives, list):
        for a in archives:
            label = _describe_op(a, verb="Archive")
            items.append({
                "id":            f"op-archive-{len(items)}",
                "text":          label,
                "operationType": "archive_node",
                "severity":      "watch",
            })
    if isinstance(notifies, list):
        for n in notifies:
            who = "actor"
            if isinstance(n, dict):
                who = n.get("display") or n.get("actor") or n.get("role") or "actor"
            elif isinstance(n, str):
                who = n
            items.append({
                "id":            f"op-notify-{len(items)}",
                "text":          f"Notify {who}",
                "operationType": "notify_actor",
                "severity":      "neutral",
            })
    re_eval = cp.get("re_evaluates_in")
    if isinstance(re_eval, str) and re_eval.strip():
        items.append({
            "id":            f"op-reeval-{len(items)}",
            "text":          f"Schedule re-evaluation in {re_eval.strip()}",
            "operationType": "schedule_re_evaluation",
            "severity":      "neutral",
        })

    items.append({
        "id":            "op-ledger",
        "text":          "Record ledger event for audit trail",
        "operationType": "create_ledger_event",
        "severity":      "neutral",
    })
    return items


def _describe_op(
    op: Any, *, verb: str, default_kind: str | None = None,
) -> str:
    if isinstance(op, str) and op.strip():
        return f"{verb} {op.strip()}"
    if isinstance(op, dict):
        kind = op.get("target_kind") or op.get("kind") or default_kind
        title = op.get("title") or op.get("name") or op.get("display")
        if title and kind:
            return f"{verb} {kind} {title}"
        if title:
            return f"{verb} {title}"
        if kind:
            return f"{verb} {kind}"
    if default_kind:
        return f"{verb} {default_kind}"
    return f"{verb} target"


# =====================================================================
# Related model links + apply preview synthesis
# =====================================================================


def _synth_related_links(
    *,
    target_node_kind: str | None,
    target_node_id: UUID | None,
    impact: dict[str, Any] | None,
    category: str,
) -> list[dict[str, Any]]:
    related: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(cat: str) -> None:
        if cat in seen:
            return
        seen.add(cat)
        href = f"/model?focus=category&categoryId={cat}"
        related.append({
            "category": cat,
            "label":    _CATEGORY_LABELS.get(cat, cat),
            "href":     href,
        })

    # Source category always present.
    add(category)
    # Primary target maps to its own category.
    if target_node_kind:
        add(_KIND_TO_CATEGORY.get(target_node_kind, "commitments"))
    # Optional explicit related categories on impact.
    if impact:
        rel = impact.get("related_categories")
        if isinstance(rel, list):
            for c in rel:
                if isinstance(c, str) and c in _CATEGORY_LABELS:
                    add(c)
    return related


def _synth_apply_preview(
    *,
    consequence_preview: dict[str, Any] | None,
    re_evaluates_in: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    cp = consequence_preview or {}
    creates = cp.get("creates") if isinstance(cp.get("creates"), list) else []
    updates = cp.get("updates") if isinstance(cp.get("updates"), list) else []
    archives = cp.get("archives") if isinstance(cp.get("archives"), list) else []
    notifies = cp.get("notifies") if isinstance(cp.get("notifies"), list) else []
    re_eval = re_evaluates_in or cp.get("re_evaluates_in")
    reeval_at = _resolve_reeval_at(re_eval, now or datetime.now(timezone.utc))
    return {
        "nodeOpsCount":               len(creates) + len(updates) + len(archives),
        "notificationsCount":         len(notifies),
        "reEvaluationScheduledAt":    reeval_at.isoformat() if reeval_at else None,
        "ledgerEventWillBeCreated":   True,
    }


def _resolve_reeval_at(spec: Any, now: datetime) -> datetime | None:
    if not isinstance(spec, str):
        return None
    s = spec.strip().lower()
    if not s:
        return None
    # Accept "48h", "7d", "2w".
    unit_factor = {"h": 3600, "d": 86400, "w": 604800}
    if s[-1] in unit_factor and s[:-1].isdigit():
        return now + timedelta(seconds=int(s[:-1]) * unit_factor[s[-1]])
    return None


# =====================================================================
# Available actions per spec status
# =====================================================================


def _available_actions(spec_status: str) -> list[str]:
    if spec_status == "needs_authority":
        return ["accept", "delegate", "review_evidence", "report_correction"]
    if spec_status == "delegatable":
        return ["delegate", "accept", "review_evidence", "report_correction"]
    if spec_status == "monitoring":
        return ["accept", "review_evidence", "open_model", "snooze"]
    if spec_status == "contested":
        return ["review_evidence", "report_correction", "accept", "delegate"]
    if spec_status == "correction_submitted":
        return ["review_evidence", "open_model"]
    if spec_status == "accepted":
        return ["review_evidence", "open_model"]
    if spec_status == "delegated":
        return ["review_evidence", "report_correction"]
    if spec_status == "archived":
        return ["review_evidence", "open_model"]
    if spec_status == "failed_apply":
        return ["accept", "review_evidence", "report_correction"]
    return ["review_evidence"]


# =====================================================================
# Delta → wire DTO
# =====================================================================


def _why_this_matters(
    *, impact: dict[str, Any] | None, main_assertion: str,
) -> str:
    """Best-effort: impact.why_this_matters > impact.qualitative >
    main_assertion. Capped to a sentence-ish length so cards stay tidy.
    """
    if impact:
        raw = impact.get("why_this_matters") or impact.get("qualitative")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return main_assertion.strip()


def _delta_to_wire(
    view: dd_repo.DecisionDeltaView,
    *,
    include_evidence: bool = False,
    priority_rank: int | None = None,
) -> dict[str, Any]:
    """Build the spec-shaped DecisionDelta wire DTO. Field order roughly
    matches the spec interface for readability when debugging."""
    impact = view.impact or {}
    spec_status = _synth_status(
        db_status=view.status, label=view.label, impact=impact,
    )
    legacy_cat = (view.category or "").lower()
    cat = _CATEGORY_MAP.get(legacy_cat, "commitments")
    current = _to_field_list(view.current_state, fallback_label="Current")
    proposed = _to_field_list(view.suggested_update, fallback_label="Proposed")
    evidence_count = len(view.evidence)
    out: dict[str, Any] = {
        "id":                view.id.hex if isinstance(view.id, UUID) else str(view.id),
        "title":             view.main_assertion,
        "userFacingType":    "proposed_change",
        "internalType":      "decision_delta",
        "status":            spec_status,
        "priorityRank":      priority_rank if priority_rank is not None else 0,
        "sourceCategory":    cat,
        "relatedCategories": [
            link["category"] for link in _synth_related_links(
                target_node_kind=view.target_node_kind,
                target_node_id=view.target_node_id,
                impact=impact,
                category=cat,
            )
        ],
        "proposedBy":        _synth_proposed_by(impact),
        "createdAt":         _isofmt(view.created_at),
        "updatedAt":         _isofmt(view.updated_at),
        "currentState":      current,
        "proposedState":     proposed,
        "summaryLine":       _summary_line(current, proposed),
        "whyThisMatters":    _why_this_matters(
            impact=impact, main_assertion=view.main_assertion,
        ),
        "keyMetrics":        _synth_key_metrics(
            confidence=view.confidence,
            impact=impact,
            evidence_count=evidence_count,
        ),
        "evidenceSummary":   _synth_evidence_summary(view.evidence),
        "missingContext":    _synth_missing_context(impact),
        "impactIfAccepted":  _synth_impact_if_accepted(
            consequence_preview=view.consequence_preview,
            target_node_kind=view.target_node_kind,
        ),
        "relatedModelLinks": _synth_related_links(
            target_node_kind=view.target_node_kind,
            target_node_id=view.target_node_id,
            impact=impact,
            category=cat,
        ),
        "availableActions":  _available_actions(spec_status),
        "applyPreview":      _synth_apply_preview(
            consequence_preview=view.consequence_preview,
        ),
        # Routing metadata for the inspector's "View in Model" link.
        "targetNodeKind":    view.target_node_kind,
        "targetNodeId":      (
            str(view.target_node_id) if view.target_node_id else None
        ),
        "confidence":        view.confidence,
        "resolutionTargetAt": _isofmt(view.resolution_target_at),
        # Surface the underlying contest/delegation/correction notes so
        # the focused-review card can show timeline annotations.
        "annotations":       _extract_annotations(impact),
    }
    if include_evidence:
        out["evidence"] = [_evidence_to_wire(e) for e in view.evidence]
    return out


def _extract_annotations(impact: dict[str, Any]) -> dict[str, Any]:
    """Surface the structured notes (delegation/contest/correction/
    context_notes) on the wire so the UI can render them in the timeline
    inside the focused-review card."""
    out: dict[str, Any] = {}
    for key in ("delegation", "contest", "correction", "context_notes"):
        if key in impact and impact[key] is not None:
            out[key] = impact[key]
    return out


def _evidence_to_wire(e: dd_repo.EvidenceItem) -> dict[str, Any]:
    return {
        "id":         str(e.id),
        "source":     e.source,
        "sourceLabel": _source_label(e.source),
        "title":      e.title,
        "occurredAt": _isofmt(e.ts),
        "trustTier":  e.trust_tier,
        "quality":    _evidence_quality(e.trust_tier),
        "excerpt":    e.excerpt,
        "weight":     e.weight,
        "ordinal":    e.ordinal,
    }


# =====================================================================
# Priority ranking — spec §15.2
# =====================================================================


_ARR_MATERIAL_THRESHOLD = 250_000.0
_RECENT_CHANGE_WINDOW = timedelta(hours=24)


def _priority_score(
    view: dd_repo.DecisionDeltaView,
    *,
    now: datetime,
) -> float:
    impact = view.impact or {}
    spec_status = _synth_status(
        db_status=view.status, label=view.label, impact=impact,
    )

    score = 0.0
    # Requires authority — the heaviest weight, per spec.
    if spec_status == "needs_authority":
        score += 50.0
    elif spec_status == "delegatable":
        score += 30.0
    elif spec_status == "contested":
        score += 25.0
    elif spec_status == "monitoring":
        score += 10.0

    # Material exposure.
    arr = impact.get("arr_at_risk")
    if isinstance(arr, (int, float)) and float(arr) >= _ARR_MATERIAL_THRESHOLD:
        score += 40.0
        if float(arr) >= 1_000_000:
            score += 10.0  # extra weight for $1M+

    # Customer impact (a customer count signals revenue concentration).
    accounts = impact.get("accounts_affected") or impact.get("accounts")
    if isinstance(accounts, int) and accounts >= 1:
        score += 25.0

    # Evidence quality.
    ev_summary = _synth_evidence_summary(view.evidence)
    if ev_summary["quality"] == "strong":
        score += 30.0
    elif ev_summary["quality"] == "weak":
        score -= 20.0

    # Urgency: short re-eval window.
    re_eval = (view.consequence_preview or {}).get("re_evaluates_in")
    if isinstance(re_eval, str) and re_eval.endswith("h"):
        try:
            hours = int(re_eval[:-1])
            if hours <= 48:
                score += 20.0
        except ValueError:
            pass

    # Confidence.
    if isinstance(view.confidence, (int, float)) and float(view.confidence) >= 0.7:
        score += 15.0

    # Recency.
    if view.updated_at and (now - _as_aware(view.updated_at)) <= _RECENT_CHANGE_WINDOW:
        score += 10.0

    # Missing-context penalty.
    miss = _synth_missing_context(impact)
    if any(m["severity"] == "high" for m in miss):
        score -= 15.0

    return score


def _as_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# =====================================================================
# Page-level aggregation
# =====================================================================


async def _last_review_at(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_id: UUID,
    now: datetime,
) -> datetime:
    """Best-effort: the most recent topology_events row authored by
    this actor's tenant. Falls back to 24h ago when nothing is logged."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT max(occurred_at) AS last_at
            FROM topology_events
            WHERE tenant_id = $1
              AND payload ->> 'event_kind' = 'decision_delta_accepted'
            """,
            tenant_id,
        )
    if row and row["last_at"]:
        return _as_aware(row["last_at"])
    return now - timedelta(hours=24)


async def _summary_counts(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    since: datetime,
    now: datetime,
) -> dict[str, Any]:
    """Compute the summary strip + handled-without-you metrics in a
    single read pass. Sparse-tolerant — every count clamps to >= 0."""
    async with pool.acquire() as conn:
        # Observations ingested since last review (signals processed).
        signals_processed = await conn.fetchval(
            """
            SELECT count(*) FROM observations
            WHERE tenant_id = $1 AND ingested_at >= $2
            """,
            tenant_id, since,
        ) or 0

        # Topology events since last review (model updates).
        model_updates = await conn.fetchval(
            """
            SELECT count(*) FROM topology_events
            WHERE tenant_id = $1 AND occurred_at >= $2
            """,
            tenant_id, since,
        ) or 0

        # Delta counts by status. ANY()'d so we pull one row per status.
        delta_rows = await conn.fetch(
            """
            SELECT status, label, impact
            FROM decision_deltas
            WHERE tenant_id = $1
            """,
            tenant_id,
        )

    # Bucket deltas into spec-status buckets.
    buckets: dict[str, int] = defaultdict(int)
    exposure_total = 0.0
    for r in delta_rows:
        imp = _maybe_json(r["impact"])
        s = _synth_status(
            db_status=r["status"], label=r["label"], impact=imp,
        )
        buckets[s] += 1
        if s in ("needs_authority", "delegatable", "contested"):
            arr = (imp or {}).get("arr_at_risk")
            if isinstance(arr, (int, float)):
                exposure_total += float(arr)

    need_judgment = (
        buckets["needs_authority"]
        + buckets["delegatable"]
        + buckets["contested"]
    )
    # Spec: "absorbed" = signals that did NOT require user judgment.
    # Approximate as processed minus the count of new judgment items
    # since last review (we don't track per-signal escalation links yet).
    signals_absorbed = max(0, int(signals_processed) - int(need_judgment))

    return {
        "summary": {
            "signalsProcessed":   int(signals_processed),
            "signalsAbsorbed":    int(signals_absorbed),
            "modelUpdates":       int(model_updates),
            "needJudgment":       int(need_judgment),
            "requiresAuthority":  int(buckets["needs_authority"]),
            "delegatable":        int(buckets["delegatable"]),
            "monitoring":         int(buckets["monitoring"]),
            "contested":          int(buckets["contested"] + buckets["correction_submitted"]),
            "exposure":           _money_envelope(exposure_total) if exposure_total > 0 else None,
        },
        "handledWithoutYou": {
            "signalsAbsorbed":     int(signals_absorbed),
            "modelUpdatesApplied": int(buckets["accepted"]),
            "itemsUnderMonitoring": int(buckets["monitoring"]),
            "delegatedChanges":     int(buckets["delegated"]),
            "contestedChanges":     int(buckets["contested"] + buckets["correction_submitted"]),
            "reassuranceCopy":      (
                "Fyralis continuously monitors the model and will resurface "
                "anything that needs you."
            ),
        },
    }


def _money_envelope(amount: float) -> dict[str, Any]:
    return {
        "amount":    int(round(amount)),
        "currency":  "USD",
        "formatted": _format_money(amount),
    }


def _maybe_json(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(v, dict):
            return v
    return None


# =====================================================================
# Misc helpers
# =====================================================================


def _isofmt(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return _as_aware(dt).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _read_json(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _auth_or_none(request: Request):
    return getattr(request.state, "auth", None)


def _deps(request: Request):
    from services.gateway.main import _deps as _gw_deps
    return _gw_deps(request)


def _unauth() -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized"},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


def _bad_request(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "bad_request", "reason": reason},
        status_code=status.HTTP_400_BAD_REQUEST,
    )


def _not_found() -> JSONResponse:
    return JSONResponse(
        {"error": "not_found"},
        status_code=status.HTTP_404_NOT_FOUND,
    )


# =====================================================================
# Correction-flow annotate helper
# =====================================================================


async def _annotate_correction(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    correction: dict[str, Any],
) -> None:
    """Patch the delta's impact JSONB with a structured correction
    record. The synth layer reads `impact.correction_submitted=True`
    and elevates the spec status from `contested` to `correction_submitted`.
    """
    row = await conn.fetchrow(
        "SELECT impact FROM decision_deltas WHERE id = $1 AND tenant_id = $2",
        delta_id, tenant_id,
    )
    existing = _maybe_json(row["impact"]) if row else None
    new_impact = dict(existing) if existing else {}
    new_impact["correction_submitted"] = True
    new_impact["correction"] = correction
    await conn.execute(
        "UPDATE decision_deltas SET impact = $2::jsonb "
        "WHERE id = $1 AND tenant_id = $3",
        delta_id, json.dumps(new_impact, default=str), tenant_id,
    )


async def _annotate_delegation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    delegation: dict[str, Any],
) -> None:
    row = await conn.fetchrow(
        "SELECT impact FROM decision_deltas WHERE id = $1 AND tenant_id = $2",
        delta_id, tenant_id,
    )
    existing = _maybe_json(row["impact"]) if row else None
    new_impact = dict(existing) if existing else {}
    new_impact["delegation"] = delegation
    await conn.execute(
        "UPDATE decision_deltas SET impact = $2::jsonb "
        "WHERE id = $1 AND tenant_id = $3",
        delta_id, json.dumps(new_impact, default=str), tenant_id,
    )


# =====================================================================
# Public registration
# =====================================================================


VALID_CORRECTION_TYPES: frozenset[str] = frozenset({
    "wrong_conclusion",
    "wrong_owner",
    "already_handled",
    "missing_context",
    "not_important",
    "misleading_source",
    "other",
})


def register_today_routes(app: FastAPI) -> None:
    """Attach /api/today/* routes for the v2 Today page."""

    # -----------------------------------------------------------------
    # GET /today
    # -----------------------------------------------------------------
    @app.get("/today")
    async def get_today(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        deps = _deps(request)
        pool: asyncpg.Pool = deps.pool

        now = datetime.now(timezone.utc)
        since_param = request.query_params.get("since")
        if since_param:
            try:
                since = datetime.fromisoformat(since_param.replace("Z", "+00:00"))
                since = _as_aware(since)
            except ValueError:
                return _bad_request("invalid_since")
        else:
            since = await _last_review_at(
                pool=pool,
                tenant_id=auth.tenant_id,
                actor_id=auth.actor_id,
                now=now,
            )

        async with pool.acquire() as conn:
            # We list ALL non-archived deltas, then rank in Python.
            views_proposed = await dd_repo.list_deltas(
                conn,
                tenant_id=auth.tenant_id,
                status="proposed",
                limit=200,
            )
            views_delegated = await dd_repo.list_deltas(
                conn,
                tenant_id=auth.tenant_id,
                status="delegated",
                limit=200,
            )
            views_contested = await dd_repo.list_deltas(
                conn,
                tenant_id=auth.tenant_id,
                status="contested",
                limit=200,
            )
            # Load evidence per delta (the list endpoint doesn't ship
            # it). Bulk-load with a single query.
            all_views = views_proposed + views_delegated + views_contested
            if all_views:
                ev_rows = await conn.fetch(
                    """
                    SELECT id, delta_id, source, title, ts, trust_tier,
                           excerpt, weight, ordinal
                    FROM decision_delta_evidence
                    WHERE delta_id = ANY($1::uuid[])
                    ORDER BY delta_id, ordinal ASC, ts ASC
                    """,
                    [v.id for v in all_views],
                )
                by_delta: dict[UUID, list[dd_repo.EvidenceItem]] = defaultdict(list)
                for r in ev_rows:
                    by_delta[r["delta_id"]].append(dd_repo.EvidenceItem(
                        id=r["id"],
                        delta_id=r["delta_id"],
                        source=r["source"],
                        title=r["title"],
                        ts=r["ts"],
                        trust_tier=r["trust_tier"],
                        excerpt=r["excerpt"],
                        weight=(
                            float(r["weight"]) if r["weight"] is not None else None
                        ),
                        ordinal=int(r["ordinal"]),
                    ))
                for v in all_views:
                    v.evidence = by_delta.get(v.id, [])

        # Rank.
        scored = sorted(
            all_views,
            key=lambda v: -_priority_score(v, now=now),
        )

        # Primary judgment = the highest-ranked actionable item.
        # Spec §5.3: needs_authority preferred when present.
        primary_view = None
        for v in scored:
            spec = _synth_status(
                db_status=v.status, label=v.label, impact=(v.impact or {}),
            )
            if spec in ("needs_authority", "delegatable", "contested"):
                primary_view = v
                break

        # Build wire DTOs with deterministic priorityRank.
        wire: list[dict[str, Any]] = []
        for i, v in enumerate(scored):
            wire.append(_delta_to_wire(v, priority_rank=i))

        primary = None
        other_changes: list[dict[str, Any]] = []
        for w, v in zip(wire, scored):
            if primary is None and primary_view is not None and v.id == primary_view.id:
                primary = w
            else:
                other_changes.append(w)

        counts = await _summary_counts(
            pool=pool,
            tenant_id=auth.tenant_id,
            since=since,
            now=now,
        )

        payload: dict[str, Any] = {
            "viewer": {
                "userId":   str(auth.actor_id),
                "name":     getattr(auth, "actor_display_name", "") or "",
                "role":     getattr(auth, "role", "") or "",
                "tenantId": str(auth.tenant_id),
            },
            "lastReviewAt":      _isofmt(since),
            "generatedAt":       _isofmt(now),
            "summary":           counts["summary"],
            "primaryJudgment":   primary,
            "otherChanges":      other_changes,
            "handledWithoutYou": counts["handledWithoutYou"],
        }
        return JSONResponse(payload)

    # -----------------------------------------------------------------
    # GET /today/deltas/{delta_id}
    # -----------------------------------------------------------------
    @app.get("/today/deltas/{delta_id}")
    async def get_delta(delta_id: str, request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            did = UUID(delta_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_delta_id")
        pool = _deps(request).pool
        async with pool.acquire() as conn:
            view = await dd_repo.get_delta(
                conn, tenant_id=auth.tenant_id, delta_id=did,
            )
        if view is None:
            return _not_found()
        return JSONResponse(_delta_to_wire(view, include_evidence=True))

    # -----------------------------------------------------------------
    # GET /today/deltas/{delta_id}/evidence
    # -----------------------------------------------------------------
    @app.get("/today/deltas/{delta_id}/evidence")
    async def get_delta_evidence(
        delta_id: str, request: Request,
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            did = UUID(delta_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_delta_id")
        pool = _deps(request).pool
        async with pool.acquire() as conn:
            view = await dd_repo.get_delta(
                conn, tenant_id=auth.tenant_id, delta_id=did,
            )
        if view is None:
            return _not_found()
        return JSONResponse({
            "deltaId":        str(did),
            "totalSignals":   len(view.evidence),
            "evidenceGroups": _synth_evidence_summary(view.evidence)["groups"],
            "items":          [_evidence_to_wire(e) for e in view.evidence],
        })

    # -----------------------------------------------------------------
    # POST /today/deltas/{delta_id}/apply
    # -----------------------------------------------------------------
    @app.post("/today/deltas/{delta_id}/apply")
    async def apply_delta(
        delta_id: str, request: Request,
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            did = UUID(delta_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_delta_id")
        pool = _deps(request).pool
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    view, triggered = await apply_mod.apply_acceptance(
                        conn=conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        user_id=auth.actor_id,
                    )
        except dd_repo.DeltaNotFoundError:
            return _not_found()
        except dd_repo.InvalidStatusTransitionError:
            # Stale model — caller should refetch + retry.
            return JSONResponse({
                "status":         "requires_refresh",
                "resultMessage": (
                    "The model changed while you were reviewing. "
                    "Fyralis refreshed the proposed change."
                ),
            }, status_code=409)
        # Pick next delta from the page list (excluding the one we just
        # accepted) to streamline the focused-review flow.
        next_id = await _next_delta_id(
            pool=pool, tenant_id=auth.tenant_id, exclude=did,
        )
        ledger_event_id = triggered.get("target_event_id")
        return JSONResponse({
            "status":          "applied",
            "resultMessage":   _result_message(view, triggered),
            "updatedDelta":    _delta_to_wire(view, include_evidence=True),
            "nextDeltaId":     next_id,
            "ledgerEventId":   (
                str(ledger_event_id) if ledger_event_id else None
            ),
            "triggered":       _coerce_uuids(triggered),
        })

    # -----------------------------------------------------------------
    # POST /today/deltas/{delta_id}/delegate
    # -----------------------------------------------------------------
    @app.post("/today/deltas/{delta_id}/delegate")
    async def delegate_delta(
        delta_id: str, request: Request,
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            did = UUID(delta_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_delta_id")
        body = await _read_json(request)
        owner_raw = body.get("delegateToActorId") or body.get("owner_id")
        if not isinstance(owner_raw, str) or not owner_raw.strip():
            return _bad_request("owner_required")
        try:
            owner_id = UUID(owner_raw)
        except (ValueError, TypeError):
            return _bad_request("invalid_owner_id")
        due_at = body.get("dueAt")
        message = body.get("message")
        notify_now = bool(body.get("notifyNow", True))
        monitor = bool(body.get("monitorConfirmation", True))

        pool = _deps(request).pool
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await dd_repo.update_status(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        status="delegated",
                        user_id=auth.actor_id,
                    )
                    await _annotate_delegation(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        delegation={
                            "owner_id":             str(owner_id),
                            "due_at":               due_at,
                            "message":              (
                                str(message).strip()
                                if isinstance(message, str) else None
                            ),
                            "notify_now":           notify_now,
                            "monitor_confirmation": monitor,
                            "at":                   _now_iso(),
                            "by":                   str(auth.actor_id),
                        },
                    )
                    view = await dd_repo.get_delta(
                        conn, tenant_id=auth.tenant_id, delta_id=did,
                    )
        except dd_repo.DeltaNotFoundError:
            return _not_found()
        except dd_repo.InvalidStatusTransitionError:
            return JSONResponse({
                "status":         "requires_refresh",
                "resultMessage": "Delta is no longer eligible for delegation.",
            }, status_code=409)
        assert view is not None
        return JSONResponse({
            "status":        "delegated",
            "resultMessage": "Delegated. Fyralis will monitor for confirmation.",
            "updatedDelta":  _delta_to_wire(view, include_evidence=True),
        })

    # -----------------------------------------------------------------
    # POST /today/deltas/{delta_id}/correction
    # -----------------------------------------------------------------
    @app.post("/today/deltas/{delta_id}/correction")
    async def correct_delta(
        delta_id: str, request: Request,
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            did = UUID(delta_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_delta_id")
        body = await _read_json(request)
        ctype = body.get("correctionType") or body.get("correction_type")
        if ctype not in VALID_CORRECTION_TYPES:
            return _bad_request("invalid_correction_type")
        explanation = body.get("explanation")
        if not isinstance(explanation, str) or not explanation.strip():
            return _bad_request("explanation_required")
        supporting = body.get("supportingLink") or body.get("supporting_link")
        apply_related = bool(
            body.get("applyToRelatedItems")
            or body.get("apply_to_related_items")
            or False
        )

        pool = _deps(request).pool
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Promote contested-from-any-eligible-state in one
                    # repo call. The repo enforces transition legality.
                    await dd_repo.update_status(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        status="contested",
                        user_id=auth.actor_id,
                    )
                    await _annotate_correction(
                        conn,
                        tenant_id=auth.tenant_id,
                        delta_id=did,
                        correction={
                            "type":               ctype,
                            "explanation":        explanation.strip(),
                            "supporting_link":    supporting,
                            "apply_to_related":   apply_related,
                            "by":                 str(auth.actor_id),
                            "at":                 _now_iso(),
                        },
                    )
                    view = await dd_repo.get_delta(
                        conn, tenant_id=auth.tenant_id, delta_id=did,
                    )
        except dd_repo.DeltaNotFoundError:
            return _not_found()
        except dd_repo.InvalidStatusTransitionError:
            return JSONResponse({
                "status":         "requires_refresh",
                "resultMessage": "Delta is no longer eligible for correction.",
            }, status_code=409)
        assert view is not None
        return JSONResponse({
            "status":        "correction_submitted",
            "resultMessage": (
                "Correction submitted. Fyralis will re-evaluate this change "
                "and any dependent model items."
            ),
            "updatedDelta":  _delta_to_wire(view, include_evidence=True),
        })


# =====================================================================
# Apply helpers
# =====================================================================


def _result_message(
    view: dd_repo.DecisionDeltaView, triggered: dict[str, Any],
) -> str:
    """Spec §9.3: present-tense narration of what just happened."""
    parts: list[str] = []
    if triggered.get("target_updated"):
        kind = view.target_node_kind or "model node"
        parts.append(f"Updated {kind}")
    notif = triggered.get("notifications_dispatched") or 0
    if notif:
        parts.append(f"Dispatched {notif} notification{'s' if notif != 1 else ''}")
    re_eval = (view.consequence_preview or {}).get("re_evaluates_in")
    if isinstance(re_eval, str) and re_eval.strip():
        parts.append(f"Scheduled re-evaluation in {re_eval.strip()}")
    parts.append("Recorded ledger event")
    return "Change accepted. " + ". ".join(parts) + "."


def _coerce_uuids(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _coerce_uuids(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_coerce_uuids(v) for v in data]
    if isinstance(data, UUID):
        return str(data)
    return data


async def _next_delta_id(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    exclude: UUID,
) -> str | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id FROM decision_deltas
            WHERE tenant_id = $1
              AND status = 'proposed'
              AND id <> $2
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant_id, exclude,
        )
    return str(row["id"]) if row else None


__all__ = ["register_today_routes"]
