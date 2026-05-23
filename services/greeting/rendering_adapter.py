"""services/greeting/rendering_adapter.py — adapter into services/rendering/.

Agent-RND owns the rendering HTTP API (CONTRACTS §2.1). Until Agent-RND
lands, we develop against a deterministic mock that returns valid HTML
shaped per CONTRACTS §1.1. Swap-in point: constructor injection. Real
adapter does HTTP; mock synthesises from the SubstrateSnapshot.

The adapter exposes a single method per render kind so the scheduler
doesn't need to know whether we're in mock or live mode.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

import httpx

from services.greeting.snapshot import (
    FounderContext,
    QueryGridSnapshot,
    SubstrateSnapshot,
)


# =====================================================================
# Public protocol — both real and mock implement this
# =====================================================================


@dataclass(frozen=True)
class RenderedGreeting:
    body_html: str
    signals_watched_count: int
    rendering_model_used: str
    cost_usd: float


@dataclass(frozen=True)
class RenderedCard:
    id: str
    kind: Literal["observation", "decision", "question"]
    tag_color: Literal["hot", "warm", "soft"]
    tag_label: str
    meta: str
    body_html: str
    reasoning_html: str
    evidence: list[dict[str, Any]]
    verbs: list[dict[str, Any]]
    rendering_model_used: str
    cost_usd: float


@dataclass(frozen=True)
class RenderedQueryGrid:
    queries: list[dict[str, Any]]
    rendering_model_used: str
    cost_usd: float


@dataclass(frozen=True)
class RenderedCloseLine:
    body: str
    signal_count: int
    external_moves: int
    calibration_pct: int
    rendering_model_used: str
    cost_usd: float


@dataclass(frozen=True)
class RenderedCardReasoning:
    """Gate 4b fix — LLM-composed reasoning_html + evidence[] payload."""
    reasoning_html: str
    evidence: list[dict[str, Any]]
    rendering_model_used: str
    cost_usd: float
    fallback: bool = False  # True when we fell back to placeholder synthesis


class RenderingAdapter(Protocol):
    async def render_greeting(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedGreeting: ...

    async def render_card(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
    ) -> RenderedCard: ...

    async def render_card_reasoning(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
        *,
        card_subject: str,
        card_body_context: str,
        supporting_evidence: list[dict[str, Any]],
    ) -> RenderedCardReasoning: ...

    async def render_query_grid(
        self,
        grid: QueryGridSnapshot,
        founder: FounderContext,
    ) -> RenderedQueryGrid: ...

    async def render_close_line(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedCloseLine: ...


# =====================================================================
# Mock
# =====================================================================


class MockRenderingAdapter:
    """Deterministic synthesiser used until Agent-RND's HTTP service is
    live. Output is valid HTML that passes the shape validators and
    reads sensibly so we can visually smoke-test the UI.

    All outputs carry inline spans matching the contract's `.serif`,
    `.hl`, `.n` classes so UI styling can be exercised end-to-end.
    """

    MODEL_NAME = "mock-rendering-adapter/1"

    async def render_greeting(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedGreeting:
        bucket = snapshot.time_of_day_bucket
        opener = _opener_for_bucket(bucket)
        anomaly_count = len(snapshot.anomalies)
        blocked = sum(
            1 for com in snapshot.active_commitments if com.state == "blocked"
        )
        unhealthy = sum(
            1 for r in snapshot.customer_resources
            if r.health in ("warning", "degraded", "critical")
        )
        signals = (
            len(snapshot.top_models)
            + len(snapshot.active_commitments)
            + len(snapshot.customer_resources)
            + len(snapshot.recent_state_changes)
            + len(snapshot.anomalies)
        )
        # Prose assembly — intentionally terse. Real rendering will
        # replace this with voice-compliant LLM output.
        if anomaly_count == 0 and blocked == 0 and unhealthy == 0:
            body = (
                f"{opener} Nothing consequential since yesterday; "
                f"<span class='serif'>the company is running at normal metabolism.</span>"
            )
        else:
            parts = [f"{opener} "]
            parts.append(
                f"<span class='n'>{signals}</span> signals crossed the "
                "watch threshold overnight. "
            )
            if anomaly_count:
                top = snapshot.anomalies[0]
                parts.append(
                    f"<span class='serif'>{top.kind.replace('_', ' ')}</span> "
                    f"flagged at significance <span class='n'>{top.significance:.2f}</span>. "
                )
            if blocked:
                parts.append(
                    f"<span class='hl'>{blocked} commitment(s) blocked</span>. "
                )
            if unhealthy:
                parts.append(
                    f"<span class='hl'>{unhealthy} customer(s)</span> off-health. "
                )
            body = "".join(parts).strip()
        return RenderedGreeting(
            body_html=body,
            signals_watched_count=signals,
            rendering_model_used=self.MODEL_NAME,
            cost_usd=0.0,
        )

    async def render_card(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
    ) -> RenderedCard:
        # Pull the pinned candidate from the snapshot's
        # conversation_context.recent_queries[0].card_candidate — the
        # composer places it there.
        candidate: dict[str, Any] = {}
        rq = snapshot.conversation_context.recent_queries
        if rq and isinstance(rq[0], dict):
            candidate = rq[0].get("card_candidate") or {}

        tag_color, tag_label, body_html, reasoning, evidence, verbs = (
            _render_card_body(card_kind, candidate, snapshot)
        )
        meta = _render_card_meta(card_kind, candidate, snapshot)
        card_id = candidate.get("id") or str(uuid4())
        return RenderedCard(
            id=f"{card_kind}:{card_id}",
            kind=card_kind,
            tag_color=tag_color,
            tag_label=tag_label,
            meta=meta,
            body_html=body_html,
            reasoning_html=reasoning,
            evidence=evidence,
            verbs=verbs,
            rendering_model_used=self.MODEL_NAME,
            cost_usd=0.0,
        )

    async def render_card_reasoning(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
        *,
        card_subject: str,
        card_body_context: str,
        supporting_evidence: list[dict[str, Any]],
    ) -> RenderedCardReasoning:
        """Deterministic placeholder — the pre-Gate-4b synthesis path.

        The live adapter calls the LLM-backed endpoint and falls back
        here on failure. This method also still serves mock-mode (the
        default when no RND URL is configured) so UI smoke tests keep
        working without LLM.
        """
        reasoning_html, evidence = _synthesize_placeholder_reasoning(
            card_kind, card_subject, card_body_context, supporting_evidence, snapshot,
        )
        return RenderedCardReasoning(
            reasoning_html=reasoning_html,
            evidence=evidence,
            rendering_model_used=self.MODEL_NAME,
            cost_usd=0.0,
            fallback=False,  # mock is not a fallback — it's the mock
        )

    async def render_query_grid(
        self,
        grid: QueryGridSnapshot,
        founder: FounderContext,
    ) -> RenderedQueryGrid:
        # Already structured; the grid composer emits rendering-ready
        # chip dicts. The rendering layer gets to edit labels for voice;
        # the mock passes them through verbatim.
        out: list[dict[str, Any]] = []
        for q in grid.situation_queries + grid.evergreen_queries:
            out.append(
                {
                    "id": q["id"],
                    "icon": q["icon"],
                    "label": q["label"],
                    "tag": q.get("tag"),
                    "hot": bool(q.get("hot", False)),
                }
            )
        return RenderedQueryGrid(
            queries=out,
            rendering_model_used=self.MODEL_NAME,
            cost_usd=0.0,
        )

    async def render_close_line(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedCloseLine:
        signal_count = (
            len(snapshot.recent_state_changes)
            + len(snapshot.anomalies)
        )
        external_moves = sum(
            1 for sc in snapshot.recent_state_changes
            if sc.entity_kind in ("resource", "commitment")
        )
        # Calibration stub until the calibration bridge is wired.
        calibration_pct = 74
        body = (
            f"{signal_count} signals tracked, "
            f"{external_moves} external moves, "
            f"calibration {calibration_pct}%."
        )
        return RenderedCloseLine(
            body=body,
            signal_count=signal_count,
            external_moves=external_moves,
            calibration_pct=calibration_pct,
            rendering_model_used=self.MODEL_NAME,
            cost_usd=0.0,
        )


# =====================================================================
# Real HTTP adapter (used after Agent-RND lands)
# =====================================================================


class HttpRenderingAdapter:
    """HTTP client pointing at services/rendering/api.py.

    Week-4 integration contract note: Agent-RND's wire is the
    *rendering* layer — it takes a SubstrateSnapshot + card_focus and
    returns `body_html` (plus cost/meta). GRT owns the structural card
    metadata (tag_color / tag_label / meta / expanded / verbs) because
    those fields are computed from the snapshot candidate, not
    LLM-composed. This adapter therefore:
      - adapts GRT's snapshot shape to RND's Pydantic input shape,
      - calls RND for prose (body_html, query labels, close-line text),
      - synthesises the structural card fields from the candidate
        attached to the snapshot's conversation_context.
    """

    def __init__(
        self,
        endpoint_base: str,
        *,
        timeout_s: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ):
        self._base = endpoint_base.rstrip("/")
        self._timeout = timeout_s
        self._client = client

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        client = self._client or httpx.AsyncClient(timeout=self._timeout)
        owned = self._client is None
        try:
            resp = await client.post(f"{self._base}{path}", json=payload)
            resp.raise_for_status()
            return resp.json()
        finally:
            if owned:
                await client.aclose()

    async def render_greeting(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedGreeting:
        resp = await self._post(
            "/rendering/greeting",
            {
                "tenant_id": str(snapshot.tenant_id),
                "timestamp": snapshot.captured_at.isoformat(),
                "substrate_state": _snapshot_to_rnd_wire(snapshot),
                "founder_context": _founder_to_rnd_wire(founder),
            },
        )
        meta = resp.get("meta") or {}
        return RenderedGreeting(
            body_html=resp["body_html"],
            signals_watched_count=int(meta.get("signals_watched_count", 0)),
            rendering_model_used=str(resp.get("rendering_model_used", "")),
            cost_usd=float(resp.get("cost_usd", 0.0)),
        )

    async def render_card(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
    ) -> RenderedCard:
        # Pull the candidate attached to the card-focus snapshot by the
        # composer. This is also what the mock adapter reads.
        candidate: dict[str, Any] = {}
        rq = snapshot.conversation_context.recent_queries
        if rq and isinstance(rq[0], dict):
            candidate = rq[0].get("card_candidate") or {}
        card_focus = _card_focus_from_candidate(card_kind, candidate, snapshot)

        resp = await self._post(
            "/rendering/card",
            {
                "tenant_id": str(snapshot.tenant_id),
                "timestamp": snapshot.captured_at.isoformat(),
                "kind": card_kind,
                "substrate_state": _snapshot_to_rnd_wire(snapshot),
                "card_focus": card_focus,
                "founder_context": _founder_to_rnd_wire(founder),
            },
        )
        body_html = resp.get("body_html", "")

        # GRT synthesises the structural fields from the candidate —
        # RND does not produce them. Reuses the mock's derivation so the
        # shape matches whether we're in mock or live mode.
        _tag_color, tag_label, _body_html_unused, reasoning, evidence, verbs = (
            _render_card_body(card_kind, candidate, snapshot)
        )
        meta = _render_card_meta(card_kind, candidate, snapshot)
        card_id = candidate.get("id") or str(uuid4())
        tag_color: Literal["hot", "warm", "soft"] = _tag_color  # narrow for dataclass
        return RenderedCard(
            id=f"{card_kind}:{card_id}",
            kind=card_kind,
            tag_color=tag_color,
            tag_label=tag_label,
            meta=meta,
            body_html=body_html,
            reasoning_html=reasoning,
            evidence=evidence,
            verbs=verbs,
            rendering_model_used=str(resp.get("rendering_model_used", "")),
            cost_usd=float(resp.get("cost_usd", 0.0)),
        )

    async def render_card_reasoning(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
        card_kind: Literal["observation", "decision", "question"],
        *,
        card_subject: str,
        card_body_context: str,
        supporting_evidence: list[dict[str, Any]],
    ) -> RenderedCardReasoning:
        """Gate 4b fix — call the live RND endpoint and on failure fall
        back to the placeholder synthesis. The fallback keeps
        `GET /view/ceo/home` robust even if the LLM is unreachable,
        circuit-broken, or timing out."""
        try:
            resp = await self._post(
                "/rendering/card-reasoning",
                {
                    "tenant_id": str(snapshot.tenant_id),
                    "timestamp": snapshot.captured_at.isoformat(),
                    "card_kind": card_kind,
                    "card_subject": card_subject,
                    "card_body_context": card_body_context,
                    "substrate_state": _snapshot_to_rnd_wire(snapshot),
                    "supporting_evidence": _evidence_to_rnd_wire(supporting_evidence),
                    "founder_context": _founder_to_rnd_wire(founder),
                },
            )
            evidence = resp.get("evidence") or []
            # Pass-through; RND guarantees `label`+`body_html` on each.
            return RenderedCardReasoning(
                reasoning_html=str(resp.get("reasoning_html", "")),
                evidence=[
                    {
                        "label": str(e.get("label", "")),
                        "body_html": str(e.get("body_html", "")),
                    }
                    for e in evidence
                    if isinstance(e, dict)
                ],
                rendering_model_used=str(resp.get("rendering_model_used", "")),
                cost_usd=float(resp.get("cost_usd", 0.0)),
                fallback=False,
            )
        except Exception as exc:
            import logging

            logging.getLogger(__name__).warning(
                "grt.render_card_reasoning_fallback",
                extra={
                    "card_kind": card_kind,
                    "card_subject": card_subject[:80],
                    "error": str(exc),
                },
            )
            reasoning_html, evidence = _synthesize_placeholder_reasoning(
                card_kind, card_subject, card_body_context,
                supporting_evidence, snapshot,
            )
            return RenderedCardReasoning(
                reasoning_html=reasoning_html,
                evidence=evidence,
                rendering_model_used="fallback-placeholder/1",
                cost_usd=0.0,
                fallback=True,
            )

    async def render_query_grid(
        self,
        grid: QueryGridSnapshot,
        founder: FounderContext,
    ) -> RenderedQueryGrid:
        # RND's query-grid route takes `specs: list[QueryGridItemSpec]`;
        # each spec carries id/icon/hot/tag/intent/query_template. The
        # LLM re-labels the chips (voice), everything else passes through.
        specs: list[dict[str, Any]] = []
        for q in list(grid.situation_queries) + list(grid.evergreen_queries):
            specs.append(
                {
                    "id": q["id"],
                    "icon": q["icon"],
                    "hot": bool(q.get("hot", False)),
                    "tag": q.get("tag"),
                    "intent": q.get("intent") or q.get("label") or "",
                    "query_template": q.get("query_template"),
                }
            )
        resp = await self._post(
            "/rendering/query-grid",
            {
                "tenant_id": str(grid.tenant_id),
                "timestamp": grid.captured_at.isoformat(),
                "substrate_state": _min_snapshot_wire_for_grid(grid),
                "specs": specs,
                "founder_context": _founder_to_rnd_wire(founder),
            },
        )
        queries = resp.get("queries") or []
        return RenderedQueryGrid(
            queries=[
                {
                    "id": q["id"],
                    "icon": q["icon"],
                    "label": q["label"],
                    "tag": q.get("tag"),
                    "hot": bool(q.get("hot", False)),
                }
                for q in queries
            ],
            rendering_model_used=str(resp.get("rendering_model_used", "")),
            cost_usd=float(resp.get("cost_usd", 0.0)),
        )

    async def render_close_line(
        self,
        snapshot: SubstrateSnapshot,
        founder: FounderContext,
    ) -> RenderedCloseLine:
        # Structural counts are GRT-owned (same derivation as the mock);
        # RND produces only the short prose body.
        signal_count = (
            len(snapshot.recent_state_changes) + len(snapshot.anomalies)
        )
        external_moves = sum(
            1 for sc in snapshot.recent_state_changes
            if sc.entity_kind in ("resource", "commitment")
        )
        calibration_pct = 74
        resp = await self._post(
            "/rendering/close-line",
            {
                "tenant_id": str(snapshot.tenant_id),
                "timestamp": snapshot.captured_at.isoformat(),
                "signals_watched_count": signal_count,
                "external_moves": external_moves,
                "calibration_pct": calibration_pct,
                "substrate_state": _snapshot_to_rnd_wire(snapshot),
            },
        )
        meta = resp.get("metadata") or {}
        return RenderedCloseLine(
            body=resp.get("body", ""),
            signal_count=int(meta.get("signal_count", signal_count)),
            external_moves=int(meta.get("external_moves", external_moves)),
            calibration_pct=int(meta.get("calibration_pct", calibration_pct)),
            rendering_model_used=str(resp.get("rendering_model_used", "")),
            cost_usd=float(resp.get("cost_usd", 0.0)),
        )


# =====================================================================
# helpers
# =====================================================================


def _serialise_founder(founder: FounderContext) -> dict[str, Any]:
    return {
        "tenant_id": str(founder.tenant_id),
        "role": founder.role,
        "display_name": founder.display_name,
        "timezone_name": founder.timezone_name,
        "observed_rhythms": founder.observed_rhythms,
    }


def _founder_to_rnd_wire(founder: FounderContext) -> dict[str, Any]:
    """Shape for services/rendering/api.py `FounderContextIn`."""
    rhythms = founder.observed_rhythms or {}
    rhythm_strs = [str(v) for v in (list(rhythms.values()) if isinstance(rhythms, dict) else rhythms)]
    return {
        "display_name": founder.display_name,
        "role": founder.role,
        "observed_rhythms": rhythm_strs,
        "recent_interactions": [],
    }


def _snapshot_to_rnd_wire(snapshot: SubstrateSnapshot) -> dict[str, Any]:
    """Adapt GRT's `SubstrateSnapshot` to the permissive
    `SubstrateSnapshotIn` shape in services/rendering/api.py.

    Field mapping:
      ModelRef.id        -> "m-<short>"
      ModelRef.natural   -> claim
      ModelRef.confidence, confidence_at_assertion -> prior
      ModelRef.last_state_change_at -> state_changed_at
      CommitmentRef.title -> label
      ResourceRef.identity -> name; utilization_state / health -> health
      StateChange.entity_id/kind -> subject_id/subject_kind
      AnomalyRef.kind/region -> kind/description
    """
    def _uid(u: Any) -> str:
        s = str(u)
        return s[:8] if len(s) > 12 else s

    def _iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        return dt.isoformat()

    top_models = [
        {
            "id": f"m-{_uid(m.id)}",
            "claim": m.natural,
            "confidence": float(m.confidence),
            "prior_confidence": float(m.confidence_at_assertion)
            if m.confidence_at_assertion is not None else None,
            "state_changed_at": _iso(m.last_state_change_at),
            "falsifier": None,
        }
        for m in snapshot.top_models
    ]
    active_commitments = [
        {
            "id": f"c-{_uid(c.id)}",
            "label": c.title,
            "owner_name": None,
            "state": c.state,
            "due_at": _iso(c.due_date),
            "pressure": "high" if c.is_critical_path else None,
        }
        for c in snapshot.active_commitments
    ]
    customer_resources = [
        {
            "id": f"r-{_uid(r.id)}",
            "kind": r.kind,
            "name": r.identity,
            "health": r.health or r.utilization_state or "healthy",
            "revenue_at_risk": (
                f"${r.revenue_at_risk_usd:,.0f}"
                if r.revenue_at_risk_usd is not None else None
            ),
        }
        for r in snapshot.customer_resources
    ]
    recent_state_changes = [
        {
            "subject_id": f"{sc.entity_kind or 'entity'}-{_uid(sc.entity_id)}",
            "subject_kind": sc.entity_kind or "unknown",
            "from_state": str((sc.metadata or {}).get("from_state", "")),
            "to_state": str((sc.metadata or {}).get("to_state", sc.kind)),
            "at": sc.occurred_at.isoformat(),
            "reason": sc.kind,
        }
        for sc in snapshot.recent_state_changes
    ]
    anomalies = [
        {
            "id": f"a-{_uid(a.id)}",
            "kind": a.kind,
            "description": json.dumps(a.region)[:120] if a.region else a.kind,
            "severity": (
                "high" if a.significance >= 0.8
                else "medium" if a.significance >= 0.5
                else "low"
            ),
        }
        for a in snapshot.anomalies
    ]
    conv = snapshot.conversation_context
    signals_watched_count = (
        len(top_models) + len(active_commitments) + len(customer_resources)
        + len(recent_state_changes) + len(anomalies)
    )
    return {
        "tenant_id": str(snapshot.tenant_id),
        "captured_at": snapshot.captured_at.isoformat(),
        "top_models": top_models,
        "active_commitments": active_commitments,
        "customer_resources": customer_resources,
        "recent_state_changes": recent_state_changes,
        "anomalies": anomalies,
        "conversation_context": {
            "was_here_recently": bool(conv.last_interaction_at is not None),
            "last_visit_at": _iso(conv.last_interaction_at),
            "last_queries": [
                str(q.get("label", q.get("query", "")))[:120]
                for q in (conv.recent_queries or [])
                if isinstance(q, dict)
            ],
        },
        "time_of_day_bucket": snapshot.time_of_day_bucket,
        "signals_watched_count": signals_watched_count,
    }


def _min_snapshot_wire_for_grid(grid: QueryGridSnapshot) -> dict[str, Any]:
    """The query-grid route still demands a SubstrateSnapshotIn payload
    (the prompt reads it for voice context). We synthesise a minimal
    shape from the grid snapshot — no tops, just tenant + time."""
    return {
        "tenant_id": str(grid.tenant_id),
        "captured_at": grid.captured_at.isoformat(),
        "top_models": [],
        "active_commitments": [],
        "customer_resources": [],
        "recent_state_changes": [],
        "anomalies": [],
        "conversation_context": {
            "was_here_recently": False,
            "last_visit_at": None,
            "last_queries": [],
        },
        "time_of_day_bucket": grid.time_of_day_bucket,
        "signals_watched_count": 0,
    }


def _card_focus_from_candidate(
    kind: str,
    candidate: dict[str, Any],
    snapshot: SubstrateSnapshot,
) -> dict[str, Any]:
    """Map GRT's card-candidate shape to RND's `card_focus` dict.

    Shape is dict[str, Any]; RND's prompts are permissive. For decision
    cards we pull deadline + at_stake (needed by the Rev-2 Change 3
    wrapper) from the candidate if present, otherwise fall back to
    sensible dogfood defaults.
    """
    focus: dict[str, Any] = {
        "kind": kind,
        "subject_kind": candidate.get("subject_kind"),
        "natural": candidate.get("natural"),
    }
    if kind == "decision":
        dd = candidate.get("days_to_due")
        focus["deadline"] = (
            candidate.get("deadline")
            or (f"in {dd} days" if dd is not None else "soon")
        )
        focus["at_stake"] = candidate.get("at_stake") or "unknown"
        focus["options"] = candidate.get("options") or ""
    if kind == "observation":
        focus["significance"] = candidate.get("significance")
    if kind == "question":
        focus["standing_days"] = candidate.get("standing_days")
    return focus


def _opener_for_bucket(bucket: str) -> str:
    return {
        "early_morning": "Good morning.",
        "morning": "Morning check-in.",
        "afternoon": "Midday check.",
        "evening": "Evening check-in.",
        "late": "Late check-in.",
    }.get(bucket, "Status.")


def _render_card_body(
    kind: str,
    candidate: dict[str, Any],
    snapshot: SubstrateSnapshot,
) -> tuple[str, str, str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    """Mock body generator. Returns
    (tag_color, tag_label, body_html, reasoning_html, evidence, verbs).
    """
    if kind == "observation":
        tag_color = "hot"
        tag_label = "Observation · " + (
            candidate.get("subject_kind", "recent movement")
        )
        subject = candidate.get("natural") or candidate.get("subject_kind", "the substrate")
        body_html = (
            f"<span class='serif'>{subject}</span>. "
            f"Significance <span class='n'>"
            f"{candidate.get('significance', 0.5):.2f}</span>."
        )
        reasoning_html = (
            "Pattern surfaced from recent substrate activity. "
            "See evidence for supporting events."
        )
    elif kind == "decision":
        tag_color = "warm"
        tag_label = "Decision · " + candidate.get("state", "pending")
        subject = candidate.get("state", "awaiting decision")
        dd = candidate.get("days_to_due")
        dd_str = f"<span class='n'>{dd}d</span>" if dd is not None else "soon"
        body_html = (
            f"<span class='serif'>{subject}</span> — due in {dd_str}."
        )
        reasoning_html = (
            "Commitment on the critical path has not advanced. "
            "Options laid out in evidence."
        )
    else:  # question
        tag_color = "soft"
        tag_label = "Question · unresolved"
        subject = candidate.get("natural") or "a drift in confidence"
        body_html = (
            f"<span class='serif'>{subject}</span> "
            "worth your attention."
        )
        reasoning_html = (
            "Confidence has drifted from its assertion value; "
            "no corroborating evidence yet."
        )

    evidence: list[dict[str, Any]] = []
    # Build evidence from the first few state_changes as placeholders.
    for i, sc in enumerate(snapshot.recent_state_changes[:3]):
        evidence.append(
            {
                "label": f"signal {i + 1}",
                "body_html": (
                    f"<span class='serif'>{sc.kind.replace('_', ' ')}</span> "
                    f"on {sc.entity_kind or 'entity'} "
                    f"({sc.occurred_at.isoformat()})"
                ),
            }
        )

    verbs = [
        {
            "id": "why",
            "label": "why",
            "primary": True,
            "query_template": f"Why was this {kind} surfaced?",
        },
        {
            "id": "timeline",
            "label": "timeline",
            "primary": False,
            "query_template": "Show me the timeline for this.",
        },
        {
            "id": "save",
            "label": "save",
            "primary": False,
            "query_template": "",
        },
    ]

    return tag_color, tag_label, body_html, reasoning_html, evidence, verbs


def _render_card_meta(
    kind: str,
    candidate: dict[str, Any],
    snapshot: SubstrateSnapshot,
) -> str:
    now = snapshot.captured_at
    # Find the candidate's timestamp if we can.
    ts: datetime | None = None
    try:
        from uuid import UUID as _UUID

        cand_id = _UUID(str(candidate.get("id")))
        if candidate.get("kind") == "model":
            for m in snapshot.top_models:
                if m.id == cand_id and m.last_state_change_at:
                    ts = m.last_state_change_at
                    break
        elif candidate.get("kind") == "commitment":
            for com in snapshot.active_commitments:
                if com.id == cand_id and com.last_state_change_at:
                    ts = com.last_state_change_at
                    break
        elif candidate.get("kind") == "anomaly":
            for a in snapshot.anomalies:
                if a.id == cand_id:
                    ts = a.published_at
                    break
    except (ValueError, TypeError, KeyError):
        ts = None
    if ts is None:
        return "filed recently"
    age_hours = max(0.0, (now - ts).total_seconds() / 3600.0)
    if age_hours < 1:
        age_s = f"{int(age_hours * 60)}m old"
    elif age_hours < 24:
        age_s = f"{int(age_hours)}h old"
    else:
        age_s = f"{int(age_hours / 24)}d old"
    stamp = ts.strftime("%a %H:%M")
    return f"filed {stamp} · {age_s}"


def _evidence_to_rnd_wire(
    evidence: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Normalise GRT's `supporting_evidence` dict shape to RND's
    `EvidenceRefIn` shape. Drops empty rows; serialises datetimes."""
    out: list[dict[str, Any]] = []
    for e in evidence or []:
        if not isinstance(e, dict):
            continue
        t = e.get("t")
        if isinstance(t, datetime):
            t_iso: str | None = t.isoformat()
        elif isinstance(t, str) and t:
            t_iso = t
        else:
            t_iso = None
        out.append(
            {
                "actor": e.get("actor"),
                "channel": e.get("channel"),
                "t": t_iso,
                "excerpt": str(e.get("excerpt") or ""),
                "cite_id": e.get("cite_id"),
                "kind": e.get("kind"),
            }
        )
    return out


def _synthesize_placeholder_reasoning(
    card_kind: str,
    card_subject: str,
    card_body_context: str,
    supporting_evidence: list[dict[str, Any]],
    snapshot: SubstrateSnapshot,
) -> tuple[str, list[dict[str, Any]]]:
    """Pre-Gate-4b synthesis — keeps the shape honest when the live
    endpoint is not reachable. The fallback path in HttpRenderingAdapter
    and the mock-mode default both call this.

    Produces valid HTML with `.serif`/`.cite`/`.note` spans so voice-rule
    spot checks still pass; the prose is generic-but-shape-correct, and
    is explicitly labelled (via the `fallback` flag on the caller) so
    observability can tell real LLM output apart from this path.
    """
    subject = card_subject or "this situation"
    reasoning_html = (
        f"<span class=\"serif\">{subject}</span> surfaced "
        "from recent substrate activity. "
        "<span class=\"note\">See evidence below for supporting events.</span>"
    )
    evidence: list[dict[str, Any]] = []
    for i, e in enumerate(supporting_evidence or [], 1):
        if not isinstance(e, dict):
            continue
        actor = str(e.get("actor") or "signal")
        t = e.get("t")
        ts = ""
        if isinstance(t, datetime):
            try:
                ts = t.strftime("%a %H:%M")
            except Exception:
                ts = ""
        elif isinstance(t, str) and t:
            ts = t[:16]
        label = f"{actor} \u2014 {ts}" if ts else f"signal {i}"
        excerpt = str(e.get("excerpt") or "").strip() or "(no excerpt)"
        cite_inner = label if ts else actor
        evidence.append(
            {
                "label": label,
                "body_html": (
                    f"{excerpt} <span class=\"cite\">{cite_inner}</span>."
                ),
            }
        )
    if not evidence:
        # Still need at least one .cite to satisfy §5 structural rules.
        evidence.append(
            {
                "label": "signal 1",
                "body_html": (
                    f"{card_body_context[:200].strip() or subject}. "
                    "<span class=\"cite\">substrate</span>"
                    "<span class=\"note\"> (no explicit evidence rows)</span>."
                ),
            }
        )
    return reasoning_html, evidence


def build_rendering_adapter() -> RenderingAdapter:
    """Factory: env-driven switch between mock and HTTP.

    - `GRT_RENDERING_BASE_URL` set  → HttpRenderingAdapter
    - unset                         → MockRenderingAdapter (default)
    """
    import os as _os
    base = _os.environ.get("GRT_RENDERING_BASE_URL")
    if not base:
        return MockRenderingAdapter()
    return HttpRenderingAdapter(base)


__all__ = [
    "RenderingAdapter",
    "MockRenderingAdapter",
    "HttpRenderingAdapter",
    "RenderedGreeting",
    "RenderedCard",
    "RenderedCardReasoning",
    "RenderedQueryGrid",
    "RenderedCloseLine",
    "build_rendering_adapter",
]
