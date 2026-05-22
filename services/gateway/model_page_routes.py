"""services/gateway/model_page_routes.py — Model page (v2) endpoints.

Implements the wire contract for the revamped Model page:

  GET /api/model/overview?mode=impact|dependencies|ownership|evidence
  GET /api/model/categories/{categoryId}/focus?mode=...
  GET /api/model/relationships/{bundleId}
  GET /api/model/items/{itemId}
  GET /api/model/items/{itemId}/trace?direction=cause|consequence&depth=N

The page exposes 8 user-facing categories (goals, commitments,
decisions, risks, customers, people, systems, finance). The existing
substrate (services/models/, services/model_trace/) uses 5 bands; we
extend the classifier and aggregate model_edges into category-pair
relationship bundles with verbs.

Relationship bundle id is deterministic and parseable from the URL:
  "{sourceCategoryId}__{verb}__{targetCategoryId}"

Auth: tenant from request.state.auth (BearerAuthMiddleware). Sparse
data is fine — endpoints return empty arrays / null fields so the
frontend can fall back to its spec-aligned fixture.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from services.gateway.auth import AuthContext
from services.model_trace.repo import (
    trace_back,
    trace_forward,
)


# =====================================================================
# Category model (spec §3.2). Eight user-facing groups.
# =====================================================================


CategoryId = Literal[
    "goals",
    "commitments",
    "decisions",
    "risks",
    "customers",
    "people",
    "systems",
    "finance",
]


_CATEGORY_LABELS: dict[str, str] = {
    "goals":       "Goals & Priorities",
    "commitments": "Commitments",
    "decisions":   "Decisions",
    "risks":       "Risks & Constraints",
    "customers":   "Customers & Revenue",
    "people":      "People & Teams",
    "systems":     "Systems & Capacity",
    "finance":     "Finance & Capital",
}


_CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "goals":       "Strategic objectives currently in play.",
    "commitments": "Promised work and present-tense execution claims.",
    "decisions":   "Open questions awaiting judgment.",
    "risks":       "Active concerns, constraints, and contested claims.",
    "customers":   "Customers and revenue exposure.",
    "people":      "Owners, contributors, and accountability gaps.",
    "systems":     "Operational systems and capacity.",
    "finance":     "Capital, runway, and funding posture.",
}


_CATEGORY_COLORS: dict[str, str] = {
    "goals":       "moss",
    "commitments": "lapis",
    "decisions":   "iris",
    "risks":       "garnet",
    "customers":   "teal",
    "people":      "ochre",
    "systems":     "blue",
    "finance":     "gold",
}


# Stable lattice position (spec §6.3 diamond layout). Coordinates in
# 0..1 normalized space; renderer scales to canvas. Tighter vertical
# spread than the spec example so Finance (bottom anchor) fits the
# viewport without clipping at standard desktop heights.
_CATEGORY_POSITIONS: dict[str, dict[str, float]] = {
    # Top: intent / governance (single strategic anchor)
    "goals":       {"x": 0.50, "y": 0.11},
    # Upper mid-row: governance + execution + outcomes
    "decisions":   {"x": 0.18, "y": 0.36},
    "commitments": {"x": 0.50, "y": 0.36},
    "customers":   {"x": 0.82, "y": 0.36},
    # Lower mid-row: ownership + friction + capacity
    "people":      {"x": 0.18, "y": 0.63},
    "risks":       {"x": 0.50, "y": 0.63},
    "systems":     {"x": 0.82, "y": 0.63},
    # Foundation: capital underpins everything above. Pulled up to
    # 0.86 so the card body never clips at the canvas bottom on
    # standard desktop viewports.
    "finance":     {"x": 0.50, "y": 0.86},
}


# Map old (5-band) → new (8-category). The substrate classifier
# classifies into goal/commitment/decision/risk/customer; we route the
# 3 missing categories (people, systems, finance) from secondary
# signals: proposition kind hints, natural-text tokens.
_BAND_TO_CATEGORY: dict[str, str] = {
    "goal":       "goals",
    "commitment": "commitments",
    "decision":   "decisions",
    "risk":       "risks",
    "customer":   "customers",
}


# Tokens that indicate People & Teams (owners, ownership, accountability,
# org-structure). Includes reporting relationships ("reports to",
# "manages", "leads") which the substrate emits as proposition_kind=
# "relation" and our 5-band classifier routes (incorrectly for the
# Model page) to "commitment". The fix per design spec §3: detect
# these patterns explicitly so org structure renders under People &
# Teams instead of Commitments.
_PEOPLE_TOKENS: tuple[str, ...] = (
    # ownership signals
    "owner", "owns", "ownership", "accountability",
    "team ", "head of", "no owner", "unassigned", "assign", "delegate",
    # org-structure / reporting (org graph relations)
    "reports to", "report to", "reporting to",
    "manages ", "managed by", "leads ", "led by",
    # role markers — when the natural text is dominated by a role tag
    # like "(vp_eng)", "(cto)", "(engineer)", "(manager)" the model
    # is structural rather than executional. Keep these conservative
    # so commitments that incidentally mention a role don't get
    # mis-routed.
    "(vp_", "(cto)", "(ceo)", "(cfo)", "(coo)", "(cmo)",
    "(engineer)", "(eng)", "(manager)", "(director)", "(lead)",
    "(designer)", "(pm)", "(product manager)",
)


# Tokens for Systems & Capacity (infrastructure, throughput, tooling).
_SYSTEMS_TOKENS: tuple[str, ...] = (
    "capacity", "platform", "infrastructure", "system ",
    "engineering capacity", "throughput", "sync ", "salesforce sync",
    "pipeline", "deployment",
)


# Tokens for Finance & Capital (runway, budget, ARR-funding).
_FINANCE_TOKENS: tuple[str, ...] = (
    "runway", "budget", "burn", "capital", "funding",
    "investor", "board", "spend", "finance",
)


def _classify_category(
    *, band: str | None, natural: str, proposition_kind: str = "",
) -> str:
    """Map a model row to one of the 8 categories. We use the
    substrate band as the default, then promote into people / systems
    / finance based on natural-text + proposition_kind signals.

    Ordering is important: org-structure detection runs BEFORE the
    band fallback, because the substrate's `relation` proposition_kind
    maps to the `commitment` band (it has no people-band). Without
    the override, "Yuki Tanaka reports to Tom Reilly" would land in
    Commitments per design spec §3 Problem 3.
    """
    n = (natural or "").lower()
    pk = (proposition_kind or "").lower()
    # Org-structure short-circuit: any `relation` proposition whose
    # natural contains a reporting / management verb is People & Teams.
    # The check is intentionally narrow (substring on three verbs) so
    # commitments that *mention* people in passing aren't mis-routed.
    if pk == "relation":
        if any(tok in n for tok in (
            "reports to", "report to", "reporting to",
            "manages ", "managed by", "leads ", "led by",
            " owns ", " owned by",
        )):
            return "people"
    if any(tok in n for tok in _PEOPLE_TOKENS):
        return "people"
    if any(tok in n for tok in _SYSTEMS_TOKENS):
        return "systems"
    if any(tok in n for tok in _FINANCE_TOKENS):
        return "finance"
    return _BAND_TO_CATEGORY.get(band or "", "commitments")


# Relationship verbs (spec §20.1) and the inverse table for direction
# inference from edge_kind. Bundles use one canonical verb per
# (source_category, target_category, edge_kind) triple.
_EDGE_KIND_VERB: dict[tuple[str, str], str] = {
    # (source_category, edge_kind) → verb
    ("goals", "supports"):                     "serves",
    ("commitments", "supports"):               "serves",
    ("commitments", "contributes_to_resolution"): "affects",
    ("decisions", "supports"):                 "supports",
    ("decisions", "contributes_to_resolution"): "blocks",
    ("risks", "supports"):                     "exposes",
    ("risks", "contributes_to_resolution"):    "exposes",
    ("systems", "supports"):                   "constrains",
    ("systems", "contributes_to_resolution"):  "constrains",
    ("people", "supports"):                    "owns",
    ("finance", "supports"):                   "funds",
    ("finance", "contributes_to_resolution"):  "funds",
}


# Default verb when (src_cat, edge_kind) is unknown. Falls back to a
# meaning-bearing English verb so the UI is never decorative.
_DEFAULT_VERB: dict[str, str] = {
    "supports":                 "supports",
    "contributes_to_resolution": "affects",
    "instance_of":              "exemplifies",
    "superseded_by":            "supersedes",
}


def _verb_for(src_cat: str, edge_kind: str) -> str:
    return (
        _EDGE_KIND_VERB.get((src_cat, edge_kind))
        or _DEFAULT_VERB.get(edge_kind, "relates to")
    )


# Per-mode bundle templates (design fix spec §3 Problem 5). Each
# (mode, src_cat, tgt_cat) carries a canonical verb. When the mode
# bar switches, the frontend gets a different set of bundles — and
# when real model_edges are sparse, the synthesizer below fabricates
# plausible bundles for the locked mode using category populations so
# the canvas reflects the mode selection.
_MODE_BUNDLE_TEMPLATES: dict[str, list[tuple[str, str, str]]] = {
    "impact": [
        ("commitments", "affects",   "customers"),
        ("risks",       "exposes",   "customers"),
        ("systems",     "affects",   "customers"),
        ("finance",     "funds",     "systems"),
        ("goals",       "serves",    "commitments"),
        ("decisions",   "blocks",    "commitments"),
        ("people",      "owns",      "commitments"),
    ],
    "dependencies": [
        ("decisions",   "blocks",    "commitments"),
        ("systems",     "constrains", "commitments"),
        ("risks",       "constrains", "commitments"),
        ("goals",       "serves",    "commitments"),
        ("finance",     "funds",     "systems"),
        ("decisions",   "blocks",    "risks"),
    ],
    "ownership": [
        ("people",      "owns",      "commitments"),
        ("people",      "owns",      "decisions"),
        ("people",      "owns",      "risks"),
        ("people",      "owns",      "systems"),
    ],
    "evidence": [
        ("systems",     "evidences", "risks"),
        ("customers",   "evidences", "risks"),
        ("customers",   "evidences", "decisions"),
        ("systems",     "evidences", "commitments"),
    ],
}


def _is_bundle_for_mode(mode: str, src: str, verb: str, tgt: str) -> bool:
    """Is a (src, verb, tgt) bundle relevant under `mode`? Used to
    filter REAL edge-derived bundles per mode bar selection."""
    template = _MODE_BUNDLE_TEMPLATES.get(mode, [])
    # Match by (src, tgt) — verb may vary slightly between substrate
    # edges and templates (e.g. real edges produce "affects" while a
    # dependencies-mode template wants "blocks"). Pair-level match
    # is the right granularity.
    pairs = {(s, t) for (s, _v, t) in template}
    if (src, tgt) in pairs:
        return True
    # In impact mode, anything reaching customers or finance is
    # impact-shaped — keep it visible.
    if mode == "impact" and (tgt == "customers" or src == "finance"):
        return True
    # In dependencies mode, allow any blocking/constraining verb
    # regardless of category pair, because those are the conceptual
    # dependency signals.
    if mode == "dependencies" and verb in ("blocks", "constrains", "limits", "depends on"):
        return True
    return False


def _color_for_verb(verb: str) -> str:
    """Map a verb to a semantic color family (spec §5.1)."""
    if verb in ("blocks", "exposes", "falsifies"):
        return "garnet"
    if verb in ("constrains", "limits"):
        return "blue"
    if verb in ("serves", "supports", "evidences", "owns", "affects"):
        return "moss"
    if verb in ("funds", "contributes to"):
        return "gold"
    if verb in ("contradicts",):
        return "iris"
    return "moss"


# =====================================================================
# Public registration
# =====================================================================


def register_model_page_routes(app: FastAPI) -> None:
    """Attach /api/model/* routes for the v2 Model page."""

    @app.get("/model/overview")
    async def get_overview(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        deps = _deps(request)
        mode = _parse_mode(request.query_params.get("mode"))
        payload = await _build_overview(
            pool=deps.pool, tenant_id=auth.tenant_id, mode=mode,
        )
        return JSONResponse(payload)

    @app.get("/model/categories/{category_id}/focus")
    async def get_category_focus(
        category_id: str, request: Request
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        if category_id not in _CATEGORY_LABELS:
            return _bad_request("invalid_category")
        deps = _deps(request)
        mode = _parse_mode(request.query_params.get("mode"))
        payload = await _build_category_focus(
            pool=deps.pool,
            tenant_id=auth.tenant_id,
            category_id=category_id,
            mode=mode,
        )
        return JSONResponse(payload)

    @app.get("/model/relationships/{bundle_id}")
    async def get_relationship_focus(
        bundle_id: str, request: Request
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        parts = bundle_id.split("__")
        if len(parts) != 3:
            return _bad_request("invalid_bundle_id")
        src, verb, tgt = parts
        if src not in _CATEGORY_LABELS or tgt not in _CATEGORY_LABELS:
            return _bad_request("invalid_bundle_id")
        deps = _deps(request)
        payload = await _build_relationship_focus(
            pool=deps.pool,
            tenant_id=auth.tenant_id,
            src=src,
            verb=verb,
            tgt=tgt,
        )
        return JSONResponse(payload)

    @app.get("/model/items/{item_id}")
    async def get_item_detail(
        item_id: str, request: Request
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            iid = UUID(item_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_item_id")
        deps = _deps(request)
        payload = await _build_item_detail(
            pool=deps.pool, tenant_id=auth.tenant_id, item_id=iid,
        )
        if payload is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(payload)

    @app.get("/model/items/{item_id}/trace")
    async def get_item_trace(
        item_id: str, request: Request
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        try:
            iid = UUID(item_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_item_id")
        direction = request.query_params.get("direction", "consequence")
        if direction not in ("cause", "consequence"):
            return _bad_request("invalid_direction")
        try:
            depth = int(request.query_params.get("depth", "4"))
        except (ValueError, TypeError):
            return _bad_request("invalid_depth")
        depth = max(1, min(depth, 8))
        deps = _deps(request)
        payload = await _build_item_trace(
            pool=deps.pool,
            tenant_id=auth.tenant_id,
            item_id=iid,
            direction=direction,
            depth=depth,
        )
        return JSONResponse(payload)


# =====================================================================
# Builders
# =====================================================================


def _parse_mode(raw: str | None) -> str:
    if raw in ("impact", "dependencies", "ownership", "evidence"):
        return raw
    return "impact"


async def _fetch_models(pool, tenant_id: UUID) -> list[dict[str, Any]]:
    """Fetch active models with band classification. We re-run the
    substrate's band classifier in Python here because we only need a
    coarse mapping and don't want to re-fit UMAP / topology data.
    """
    rows = await pool.fetch(
        """
        SELECT m.id, m."natural" AS natural, m.proposition_kind,
               m.proposition, m.confidence, m.activation, m.status,
               m.contested_count, m.confirmed_count, m.created_at,
               m.last_confirmed_at
        FROM models m
        WHERE m.tenant_id = $1
          AND m.status = 'active'
        ORDER BY m.activation * m.confidence DESC, m.created_at DESC
        """,
        tenant_id,
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        rec = dict(r)
        # Substrate band: re-derive from proposition_kind + natural so
        # we don't need to import the map_routes private fn.
        rec["band"] = _band_from_kind(
            kind=rec["proposition_kind"] or "",
            natural=rec["natural"] or "",
        )
        rec["category"] = _classify_category(
            band=rec["band"],
            natural=rec["natural"] or "",
            proposition_kind=rec["proposition_kind"] or "",
        )
        out.append(rec)
    return out


# Inlined version of map_routes._classify_band — keeping it
# duplicate-free is fine since the model_page module is the only other
# consumer and the wire surface is independent.
_PROPOSITION_KIND_BAND: dict[str, str] = {
    "recommendation": "goal",
    "state": "commitment",
    "relation": "commitment",
    "prediction": "decision",
    "hypothesis": "decision",
    "concern": "risk",
    "pattern": "risk",
    "pattern_instance": "risk",
    "environmental_trend": "risk",
    "capability_assessment": "risk",
    "market_assessment": "customer",
}


def _band_from_kind(*, kind: str, natural: str) -> str:
    nat = (natural or "").strip()
    if nat.startswith("Goal G-"):
        return "goal"
    if nat.startswith("Decision D-"):
        return "decision"
    if nat.startswith("Commitment "):
        return "commitment"
    if nat.startswith("Risk R-"):
        return "risk"
    return _PROPOSITION_KIND_BAND.get(kind, "commitment")


async def _fetch_edges(
    pool, tenant_id: UUID, model_ids: list[UUID]
) -> list[dict[str, Any]]:
    if not model_ids:
        return []
    rows = await pool.fetch(
        """
        SELECT source_model_id, target_model_id, edge_kind, weight, status
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND source_model_id = ANY($2::uuid[])
          AND target_model_id = ANY($2::uuid[])
        """,
        tenant_id, model_ids,
    )
    return [dict(r) for r in rows]


def _short_label(natural: str, n: int = 64) -> str:
    s = (natural or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "\u2026"


def _item_status(rec: dict[str, Any]) -> str:
    contested = int(rec.get("contested_count") or 0)
    confirmed = int(rec.get("confirmed_count") or 0)
    confidence = float(rec.get("confidence") or 0.0)
    activation = float(rec.get("activation") or 0.0)
    if contested > confirmed and contested > 0:
        return "contested"
    if confidence < 0.4:
        return "watch"
    if activation < 0.3:
        return "stale"
    if confidence >= 0.7:
        return "healthy"
    return "watch"


def _item_summary(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(rec["id"]),
        "categoryId": rec["category"],
        "assertion": rec["natural"] or "",
        "shortLabel": _short_label(rec["natural"] or "", 56),
        "status": _item_status(rec),
        "confidence": float(rec.get("confidence") or 0.0),
    }


async def _build_overview(
    *, pool, tenant_id: UUID, mode: str
) -> dict[str, Any]:
    models = await _fetch_models(pool, tenant_id)
    by_cat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in models:
        by_cat[r["category"]].append(r)

    # Categories — one card per locked category, even when empty.
    categories: list[dict[str, Any]] = [
        _build_category_card(cid, by_cat.get(cid, []))
        for cid in _CATEGORY_LABELS
    ]

    # Relationship bundles — aggregate edges by (src_cat, verb, tgt_cat).
    model_ids = [r["id"] for r in models]
    edges = await _fetch_edges(pool, tenant_id, model_ids)
    cat_by_id = {r["id"]: r["category"] for r in models}
    bundles_raw: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for e in edges:
        src_cat = cat_by_id.get(e["source_model_id"])
        tgt_cat = cat_by_id.get(e["target_model_id"])
        if not src_cat or not tgt_cat or src_cat == tgt_cat:
            continue
        verb = _verb_for(src_cat, e["edge_kind"])
        bundles_raw[(src_cat, verb, tgt_cat)].append(e)

    # Filter real bundles by mode (design fix spec §3 Problem 5).
    bundles: list[dict[str, Any]] = []
    for (src, verb, tgt), instances in bundles_raw.items():
        if not _is_bundle_for_mode(mode, src, verb, tgt):
            continue
        bundles.append(_make_bundle(src, verb, tgt, mode, len(instances)))

    # Synthesize per-mode bundles for any template pair that has
    # populated categories on both sides but no real edges. This is
    # what makes the mode bar feel real on sparse tenants: clicking
    # Ownership reveals "people owns N commitments" even when the
    # model_edges table has no rows.
    counts_by_cat = {cid: len(by_cat.get(cid, [])) for cid in _CATEGORY_LABELS}
    seen_pairs = {
        (b["sourceCategoryId"], b["targetCategoryId"]) for b in bundles
    }
    for (src, verb, tgt) in _MODE_BUNDLE_TEMPLATES.get(mode, []):
        if (src, tgt) in seen_pairs:
            continue
        # Use the smaller of the two category counts as a coarse
        # estimate of how many instances the bundle would have in a
        # well-edged graph (e.g. 5 goals served by ≤5 commitments).
        # Cap at a humane number so the label doesn't read 632.
        est = max(1, min(counts_by_cat.get(src, 0), counts_by_cat.get(tgt, 0), 8))
        if est <= 0:
            continue
        bundles.append(_make_bundle(src, verb, tgt, mode, est, synthesized=True))
        seen_pairs.add((src, tgt))

    # Top 7 by instance count (spec §6.4).
    bundles.sort(key=lambda b: (-b["instanceCount"], b["sourceCategoryId"]))
    bundles = bundles[:7]

    summary = {
        "activeItemCount": sum(c["itemCount"] for c in categories),
        "changedTodayCount": 0,
        "blockedCount": sum(
            b["instanceCount"] for b in bundles if b["verb"] == "blocks"
        ),
        "contestedCount": sum(c["contestedCount"] for c in categories),
        "exposureAtRisk": None,
        "lastUpdatedAt": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "summary": summary,
        "categories": categories,
        "relationshipBundles": bundles,
        "mode": mode,
        "layoutHints": {
            "categoryPositions": {
                cid: _CATEGORY_POSITIONS[cid] for cid in _CATEGORY_LABELS
            },
        },
    }


def _make_bundle(
    src: str, verb: str, tgt: str, mode: str, count: int,
    *, synthesized: bool = False,
) -> dict[str, Any]:
    return {
        "id": f"{src}__{verb}__{tgt}",
        "mode": mode,
        "sourceCategoryId": src,
        "targetCategoryId": tgt,
        "verb": verb,
        "label": f"{verb} {count} {tgt}",
        "instanceCount": count,
        "severity": _severity_for(count),
        "synthesized": synthesized,
        "visual": {
            "colorToken": _color_for_verb(verb),
            "strength": _strength_for(count),
            "direction": "source_to_target",
            "lineStyle": "dashed" if synthesized else "solid",
        },
    }


def _severity_for(count: int) -> str:
    if count >= 4:
        return "high"
    if count >= 2:
        return "medium"
    return "low"


def _strength_for(count: int) -> str:
    if count >= 4:
        return "high"
    if count >= 2:
        return "medium"
    return "low"


async def _build_category_focus(
    *, pool, tenant_id: UUID, category_id: str, mode: str,
) -> dict[str, Any]:
    models = await _fetch_models(pool, tenant_id)
    by_cat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in models:
        by_cat[r["category"]].append(r)

    own = by_cat.get(category_id, [])
    top_items = [_item_summary(r) for r in own[:8]]

    # Related categories: any other category that shares at least one
    # edge with this one. Computed below in the bundles loop.
    model_ids = [r["id"] for r in models]
    edges = await _fetch_edges(pool, tenant_id, model_ids)
    cat_by_id = {r["id"]: r["category"] for r in models}

    related_set: set[str] = set()
    bundles_raw: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for e in edges:
        src_cat = cat_by_id.get(e["source_model_id"])
        tgt_cat = cat_by_id.get(e["target_model_id"])
        if not src_cat or not tgt_cat or src_cat == tgt_cat:
            continue
        if src_cat != category_id and tgt_cat != category_id:
            continue
        verb = _verb_for(src_cat, e["edge_kind"])
        bundles_raw[(src_cat, verb, tgt_cat)].append(e)
        related_set.add(src_cat if src_cat != category_id else tgt_cat)

    bundles: list[dict[str, Any]] = []
    for (src, verb, tgt), insts in bundles_raw.items():
        if not _is_bundle_for_mode(mode, src, verb, tgt):
            continue
        bundles.append(_make_bundle(src, verb, tgt, mode, len(insts)))

    # Mode-aware synthesizer: for any template bundle touching this
    # category that isn't already present, fabricate it from category
    # populations. Same rationale as overview synthesizer — the mode
    # bar must feel real on sparse tenants.
    counts_by_cat = {cid: len(by_cat.get(cid, [])) for cid in _CATEGORY_LABELS}
    seen_pairs = {
        (b["sourceCategoryId"], b["targetCategoryId"]) for b in bundles
    }
    for (src, verb, tgt) in _MODE_BUNDLE_TEMPLATES.get(mode, []):
        if src != category_id and tgt != category_id:
            continue
        if (src, tgt) in seen_pairs:
            continue
        est = max(1, min(counts_by_cat.get(src, 0), counts_by_cat.get(tgt, 0), 8))
        if est <= 0:
            continue
        bundles.append(_make_bundle(src, verb, tgt, mode, est, synthesized=True))
        seen_pairs.add((src, tgt))
        related_set.add(src if src != category_id else tgt)

    bundles.sort(key=lambda b: (-b["instanceCount"], b["sourceCategoryId"]))

    related_categories = []
    for cid in _CATEGORY_LABELS:
        if cid == category_id:
            continue
        related = _build_category_card(cid, by_cat.get(cid, []))
        related["isRelated"] = cid in related_set
        related_categories.append(related)

    category = _build_category_card(category_id, own)

    return {
        "category": category,
        "relatedCategories": related_categories,
        "relationshipBundles": bundles,
        "topItems": top_items,
        "totalItems": len(own),
    }


def _build_category_card(cid: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a complete ModelCategory shape (matches ui types). Both
    /overview and /focus return this so the frontend can render
    CategoryModule consistently without partial-shape crashes."""
    dist: dict[str, int] = defaultdict(int)
    for r in items:
        dist[_item_status(r)] += 1
    beads = [
        {"status": s, "count": dist.get(s, 0)}
        for s in ("healthy", "watch", "at_risk", "blocked", "critical", "contested", "stale")
    ]
    return {
        "id": cid,
        "label": _CATEGORY_LABELS[cid],
        "description": _CATEGORY_DESCRIPTIONS[cid],
        "colorToken": _CATEGORY_COLORS[cid],
        "itemCount": len(items),
        "changedTodayCount": 0,
        "contestedCount": dist.get("contested", 0),
        "blockedCount": dist.get("blocked", 0),
        "atRiskCount": dist.get("at_risk", 0) + dist.get("critical", 0),
        "topItems": [_item_summary(r) for r in items[:4]],
        "statusDistribution": beads,
        "position": _CATEGORY_POSITIONS[cid],
    }


async def _build_relationship_focus(
    *, pool, tenant_id: UUID, src: str, verb: str, tgt: str,
) -> dict[str, Any]:
    models = await _fetch_models(pool, tenant_id)
    by_id = {r["id"]: r for r in models}
    cat_by_id = {r["id"]: r["category"] for r in models}
    model_ids = [r["id"] for r in models]
    edges = await _fetch_edges(pool, tenant_id, model_ids)

    instances: list[dict[str, Any]] = []
    for e in edges:
        src_cat = cat_by_id.get(e["source_model_id"])
        tgt_cat = cat_by_id.get(e["target_model_id"])
        if src_cat != src or tgt_cat != tgt:
            continue
        e_verb = _verb_for(src_cat, e["edge_kind"])
        if e_verb != verb:
            continue
        src_rec = by_id.get(e["source_model_id"])
        tgt_rec = by_id.get(e["target_model_id"])
        if not src_rec or not tgt_rec:
            continue
        instances.append({
            "id": f"{e['source_model_id']}__{e['target_model_id']}__{verb}",
            "sourceItem": _item_summary(src_rec),
            "targetItem": _item_summary(tgt_rec),
            "verb": verb,
            "explanation": _short_label(
                f"{src_rec['natural']} {verb} {tgt_rec['natural']}", 200
            ),
            "confidence": float(e.get("weight") or 0.0) or None,
        })

    bundle = {
        "id": f"{src}__{verb}__{tgt}",
        "sourceCategoryId": src,
        "targetCategoryId": tgt,
        "verb": verb,
        "instanceCount": len(instances),
        "severity": _severity_for(len(instances)),
        "visual": {
            "colorToken": _color_for_verb(verb),
            "strength": _strength_for(len(instances)),
            "direction": "source_to_target",
            "lineStyle": "solid",
        },
    }
    source_category = {
        "id": src,
        "label": _CATEGORY_LABELS[src],
        "colorToken": _CATEGORY_COLORS[src],
    }
    target_category = {
        "id": tgt,
        "label": _CATEGORY_LABELS[tgt],
        "colorToken": _CATEGORY_COLORS[tgt],
    }
    return {
        "bundle": bundle,
        "sourceCategory": source_category,
        "targetCategory": target_category,
        "instances": instances,
        "resolutionOpportunities": [],
    }


async def _build_item_detail(
    *, pool, tenant_id: UUID, item_id: UUID,
) -> dict[str, Any] | None:
    """Item detail with Node-neighborhood-shaped neighbors.

    Wire shape: `neighbors.{incoming,outgoing}` are `RelationshipInstance`
    objects (sourceItem / targetItem / verb / explanation) — NOT raw
    trace steps — because NodeNeighborhood renders relationship strands,
    not adjacency lists.

    When real model_edges adjacency is sparse (which is the common case
    on demo tenants — see design fix spec §3 Problem 1), we synthesize
    cross-category neighbors from sibling-category items so the
    Node Zoom canvas isn't a lonely card. Synthesized instances are
    flagged so the renderer can style them as soft connections.
    """
    row = await pool.fetchrow(
        """
        SELECT m.id, m."natural" AS natural, m.proposition_kind,
               m.proposition, m.confidence, m.activation, m.status,
               m.contested_count, m.confirmed_count, m.created_at,
               m.last_confirmed_at
        FROM models m
        WHERE m.id = $1 AND m.tenant_id = $2
        """,
        item_id, tenant_id,
    )
    if row is None:
        return None
    rec = dict(row)
    rec["band"] = _band_from_kind(
        kind=rec["proposition_kind"] or "",
        natural=rec["natural"] or "",
    )
    rec["category"] = _classify_category(
        band=rec["band"],
        natural=rec["natural"] or "",
        proposition_kind=rec["proposition_kind"] or "",
    )
    self_summary = _item_summary(rec)

    # Pull direct neighbors via model_edges JOIN models so we get
    # category classification on the other side too.
    edge_rows_out = await pool.fetch(
        """
        SELECT e.target_model_id AS other_id, e.edge_kind, e.weight,
               m2."natural" AS other_natural,
               m2.proposition_kind AS other_kind,
               m2.proposition AS other_proposition,
               m2.confidence AS other_confidence,
               m2.activation AS other_activation,
               m2.contested_count AS other_contested,
               m2.confirmed_count AS other_confirmed
        FROM model_edges e
        JOIN models m2 ON m2.id = e.target_model_id AND m2.tenant_id = e.tenant_id
        WHERE e.tenant_id = $1 AND e.source_model_id = $2
          AND e.status = 'active'
        LIMIT 12
        """,
        tenant_id, item_id,
    )
    edge_rows_in = await pool.fetch(
        """
        SELECT e.source_model_id AS other_id, e.edge_kind, e.weight,
               m2."natural" AS other_natural,
               m2.proposition_kind AS other_kind,
               m2.proposition AS other_proposition,
               m2.confidence AS other_confidence,
               m2.activation AS other_activation,
               m2.contested_count AS other_contested,
               m2.confirmed_count AS other_confirmed
        FROM model_edges e
        JOIN models m2 ON m2.id = e.source_model_id AND m2.tenant_id = e.tenant_id
        WHERE e.tenant_id = $1 AND e.target_model_id = $2
          AND e.status = 'active'
        LIMIT 12
        """,
        tenant_id, item_id,
    )

    def _other_summary(r: dict[str, Any]) -> dict[str, Any]:
        other = dict(r)
        other["band"] = _band_from_kind(
            kind=other.get("other_kind") or "",
            natural=other.get("other_natural") or "",
        )
        other["category"] = _classify_category(
            band=other["band"],
            natural=other.get("other_natural") or "",
            proposition_kind=other.get("other_kind") or "",
        )
        return {
            "id": str(other["other_id"]),
            "categoryId": other["category"],
            "assertion": other.get("other_natural") or "",
            "shortLabel": _short_label(other.get("other_natural") or "", 56),
            "status": _item_status({
                "confidence": other.get("other_confidence"),
                "activation": other.get("other_activation"),
                "contested_count": other.get("other_contested"),
                "confirmed_count": other.get("other_confirmed"),
            }),
            "confidence": float(other.get("other_confidence") or 0.0),
        }

    outgoing: list[dict[str, Any]] = []
    incoming: list[dict[str, Any]] = []
    for er in edge_rows_out:
        other = _other_summary(dict(er))
        verb = _verb_for(self_summary["categoryId"], er["edge_kind"])
        outgoing.append({
            "id": f"{self_summary['id']}__{other['id']}__{verb}",
            "sourceItem": self_summary,
            "targetItem": other,
            "verb": verb,
            "explanation": f"{self_summary['shortLabel']} {verb} {other['shortLabel']}.",
        })
    for er in edge_rows_in:
        other = _other_summary(dict(er))
        verb = _verb_for(other["categoryId"], er["edge_kind"])
        incoming.append({
            "id": f"{other['id']}__{self_summary['id']}__{verb}",
            "sourceItem": other,
            "targetItem": self_summary,
            "verb": verb,
            "explanation": f"{other['shortLabel']} {verb} {self_summary['shortLabel']}.",
        })

    # Synthesize cross-category neighbors when real adjacency is sparse
    # so Node Zoom never shows the central card alone (design fix §3
    # Problem 1). We pick representative items from each related
    # category using the mode-agnostic dependency templates as a guide.
    if len(outgoing) + len(incoming) < 4:
        synth_outgoing, synth_incoming = await _synth_neighbors(
            pool=pool,
            tenant_id=tenant_id,
            self_summary=self_summary,
            exclude_ids={item_id},
            want=4 - len(outgoing) - len(incoming),
        )
        outgoing.extend(synth_outgoing)
        incoming.extend(synth_incoming)

    item = {
        "id": str(rec["id"]),
        "categoryId": rec["category"],
        "assertion": rec["natural"] or "",
        "shortLabel": _short_label(rec["natural"] or "", 56),
        "status": _item_status(rec),
        "confidence": float(rec["confidence"] or 0.0),
        "activation": float(rec["activation"] or 0.0),
        "authority": "system_inference",
        "lifecycle": {
            "createdAt": rec["created_at"].isoformat() if rec["created_at"] else None,
            "lastConfirmedAt": (
                rec["last_confirmed_at"].isoformat()
                if rec["last_confirmed_at"] else None
            ),
        },
        "propositionKind": rec["proposition_kind"],
        # Relationship counts surfaced on the central Node card per
        # design fix spec §3 Problem 9: "Blocked by 2 · Serves 3
        # customers · Related decision 1".
        "relationshipCounts": _count_neighbors(outgoing, incoming),
    }
    return {
        "item": item,
        "neighbors": {
            "outgoing": outgoing,
            "incoming": incoming,
        },
        "evidence": [],
        "missingContext": [],
        "relatedDecisionDeltas": [],
        "relatedForecasts": [],
        "relatedLedgerEvents": [],
    }


def _count_neighbors(
    outgoing: list[dict[str, Any]], incoming: list[dict[str, Any]],
) -> dict[str, int]:
    """Bucket neighbors by verb / target-category for the central card
    summary line. Used by NodeZoom to render lines like:
      Blocked by 2 · Serves 3 customers · Related decision 1
    """
    counts: dict[str, int] = defaultdict(int)
    for n in outgoing:
        verb = n.get("verb") or ""
        tgt = (n.get("targetItem") or {}).get("categoryId") or ""
        counts[f"{verb}_{tgt}"] += 1
        counts[f"out_{verb}"] += 1
    for n in incoming:
        verb = n.get("verb") or ""
        src = (n.get("sourceItem") or {}).get("categoryId") or ""
        counts[f"in_{verb}"] += 1
        counts[f"in_{src}"] += 1
    return dict(counts)


async def _synth_neighbors(
    *,
    pool, tenant_id: UUID,
    self_summary: dict[str, Any],
    exclude_ids: set[UUID],
    want: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Synthesize neighbors from sibling categories using the
    canonical category↔category templates. Returns at most `want`
    instances total, split into (outgoing, incoming) buckets.

    The synthesizer picks the most-active item from each candidate
    related category (by activation × confidence). This makes Node
    Zoom feel substantial even when the substrate has no real edges
    for this Node — which is the common case for fresh demo tenants.
    """
    if want <= 0:
        return [], []
    cat = self_summary["categoryId"]
    # Pick the 4 most informative related categories for this node:
    # the dependency-mode template gives a good spread (goal/decision/
    # risk/customer/etc), and we always include People as the owner.
    template = _MODE_BUNDLE_TEMPLATES["dependencies"] + \
        _MODE_BUNDLE_TEMPLATES["impact"] + \
        _MODE_BUNDLE_TEMPLATES["ownership"]
    seen_cats: set[str] = set()
    pairs: list[tuple[str, str, str, str]] = []  # (src, verb, tgt, direction)
    for (src, verb, tgt) in template:
        if src == cat and tgt not in seen_cats:
            pairs.append((src, verb, tgt, "out"))
            seen_cats.add(tgt)
        elif tgt == cat and src not in seen_cats:
            pairs.append((src, verb, tgt, "in"))
            seen_cats.add(src)
        if len(pairs) >= 5:
            break
    if not pairs:
        return [], []

    other_cats = sorted({p[3] == "out" and p[2] or p[0] for p in pairs})
    # Pull top candidates from each related category — most-active
    # first. We use the same band/category classifier so the routing
    # is consistent with the rest of the API.
    rows = await pool.fetch(
        """
        SELECT m.id, m."natural" AS natural, m.proposition_kind,
               m.proposition, m.confidence, m.activation, m.status,
               m.contested_count, m.confirmed_count, m.created_at,
               m.last_confirmed_at
        FROM models m
        WHERE m.tenant_id = $1
          AND m.status = 'active'
        ORDER BY m.activation * m.confidence DESC, m.created_at DESC
        LIMIT 200
        """,
        tenant_id,
    )
    by_cat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        rec = dict(r)
        if rec["id"] in exclude_ids:
            continue
        rec["band"] = _band_from_kind(
            kind=rec["proposition_kind"] or "",
            natural=rec["natural"] or "",
        )
        rec["category"] = _classify_category(
            band=rec["band"],
            natural=rec["natural"] or "",
            proposition_kind=rec["proposition_kind"] or "",
        )
        if rec["category"] in other_cats:
            by_cat[rec["category"]].append(rec)

    out_instances: list[dict[str, Any]] = []
    in_instances: list[dict[str, Any]] = []
    for (src, verb, tgt, direction) in pairs:
        if len(out_instances) + len(in_instances) >= want:
            break
        other_cat = tgt if direction == "out" else src
        candidates = by_cat.get(other_cat) or []
        if not candidates:
            continue
        other_rec = candidates[0]
        # Use up to one item per category so the neighborhood is varied.
        by_cat[other_cat] = candidates[1:]
        other_summary = _item_summary(other_rec)
        if direction == "out":
            inst = {
                "id": f"{self_summary['id']}__{other_summary['id']}__{verb}",
                "sourceItem": self_summary,
                "targetItem": other_summary,
                "verb": verb,
                "explanation": (
                    f"{self_summary['shortLabel']} {verb} "
                    f"{other_summary['shortLabel']}."
                ),
                "synthesized": True,
            }
            out_instances.append(inst)
        else:
            inst = {
                "id": f"{other_summary['id']}__{self_summary['id']}__{verb}",
                "sourceItem": other_summary,
                "targetItem": self_summary,
                "verb": verb,
                "explanation": (
                    f"{other_summary['shortLabel']} {verb} "
                    f"{self_summary['shortLabel']}."
                ),
                "synthesized": True,
            }
            in_instances.append(inst)
    return out_instances, in_instances


async def _build_item_trace(
    *, pool, tenant_id: UUID, item_id: UUID,
    direction: str, depth: int,
) -> dict[str, Any]:
    """Wrap services.model_trace into the v2 trace response shape."""
    async with pool.acquire() as conn:
        if direction == "cause":
            chain = await trace_back(conn, tenant_id, item_id, depth)
        else:
            chain = await trace_forward(conn, tenant_id, item_id, depth)

    if not chain:
        return {
            "rootItemId": str(item_id),
            "direction": direction,
            "nodes": [],
            "edges": [],
        }

    nodes = []
    edges = []
    for i, step in enumerate(chain):
        nodes.append({
            "id": str(step.id),
            "assertion": step.summary or step.label,
            "shortLabel": _short_label(step.label or "", 56),
            "kind": step.kind,
            "ts": step.ts.isoformat() if step.ts is not None else None,
            "step": i,
        })
        if i > 0:
            prev = chain[i - 1]
            edges.append({
                "source": str(prev.id) if direction == "consequence" else str(step.id),
                "target": str(step.id) if direction == "consequence" else str(prev.id),
                "verb": step.via_edge_kind or "supports",
            })
    return {
        "rootItemId": str(item_id),
        "direction": direction,
        "nodes": nodes,
        "edges": edges,
    }


# =====================================================================
# Helpers
# =====================================================================


def _deps(request: Request):
    from services.gateway.main import _deps as _gw_deps
    return _gw_deps(request)


def _auth_or_none(request: Request) -> AuthContext | None:
    return getattr(request.state, "auth", None)


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


__all__ = ["register_model_page_routes"]
