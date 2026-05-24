"""services/rendering/api.py — FastAPI routes.

Phase 4: internal-only HTTP API over the RenderingService. Agent-GRT and
Agent-QRY call these endpoints; the UI does not call them directly.

Routes:
  POST /rendering/greeting
  POST /rendering/card
  POST /rendering/query-grid
  POST /rendering/conversation-turn
  POST /rendering/close-line

Request/response bodies mirror services/rendering/contracts.py with a
thin Pydantic shim so FastAPI can validate + serialise them. We do not
re-export the dataclass types on the wire because dataclasses + Decimal
+ datetime need adapter code anyway; Pydantic gives us that for free.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field, ConfigDict

from .contracts import (
    AnomalyRef,
    CommitmentRef,
    ConversationContext,
    ConversationTurn,
    EvidenceRef,
    FounderContext,
    ModelRef,
    QueryGridItemSpec,
    RenderCardReasoningRequest,
    RenderCardRequest,
    RenderCloseLineRequest,
    RenderConversationTurnRequest,
    RenderGreetingRequest,
    RenderQueryGridRequest,
    ResourceRef,
    StateChange,
    SubstrateSnapshot,
)
from .core import RenderingService


# ---------------------------------------------------------------------
# Pydantic wire types. Permissive on substrate fields so a GRT-side
# addition doesn't break the rendering wire.
# ---------------------------------------------------------------------


class _WireConfig(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)


class ModelRefIn(_WireConfig):
    id: str
    claim: str
    confidence: float
    prior_confidence: Optional[float] = None
    state_changed_at: Optional[datetime] = None
    falsifier: Optional[str] = None


class CommitmentRefIn(_WireConfig):
    id: str
    label: str
    owner_name: Optional[str] = None
    state: str = "Open"
    due_at: Optional[datetime] = None
    pressure: Optional[str] = None


class ResourceRefIn(_WireConfig):
    id: str
    kind: str
    name: str
    health: str = "healthy"
    revenue_at_risk: Optional[str] = None


class StateChangeIn(_WireConfig):
    subject_id: str
    subject_kind: str
    from_state: str
    to_state: str
    at: datetime
    reason: Optional[str] = None


class AnomalyRefIn(_WireConfig):
    id: str
    kind: str
    description: str
    severity: str = "medium"


class ConversationContextIn(_WireConfig):
    was_here_recently: bool = False
    last_visit_at: Optional[datetime] = None
    last_queries: list[str] = Field(default_factory=list)


class FounderContextIn(_WireConfig):
    display_name: str = "the founder"
    role: str = "ceo"
    observed_rhythms: list[str] = Field(default_factory=list)
    recent_interactions: list[str] = Field(default_factory=list)


class SubstrateSnapshotIn(_WireConfig):
    tenant_id: UUID
    captured_at: datetime
    top_models: list[ModelRefIn] = Field(default_factory=list)
    active_commitments: list[CommitmentRefIn] = Field(default_factory=list)
    customer_resources: list[ResourceRefIn] = Field(default_factory=list)
    recent_state_changes: list[StateChangeIn] = Field(default_factory=list)
    anomalies: list[AnomalyRefIn] = Field(default_factory=list)
    conversation_context: ConversationContextIn = Field(default_factory=ConversationContextIn)
    time_of_day_bucket: Literal[
        "early_morning", "morning", "afternoon", "evening", "late"
    ] = "morning"
    signals_watched_count: int = 0


def _to_snapshot(s: SubstrateSnapshotIn) -> SubstrateSnapshot:
    return SubstrateSnapshot(
        tenant_id=s.tenant_id,
        captured_at=s.captured_at,
        top_models=[
            ModelRef(
                id=m.id, claim=m.claim, confidence=m.confidence,
                prior_confidence=m.prior_confidence,
                state_changed_at=m.state_changed_at, falsifier=m.falsifier,
            )
            for m in s.top_models
        ],
        active_commitments=[
            CommitmentRef(
                id=c.id, label=c.label, owner_name=c.owner_name,
                state=c.state, due_at=c.due_at, pressure=c.pressure,
            )
            for c in s.active_commitments
        ],
        customer_resources=[
            ResourceRef(
                id=r.id, kind=r.kind, name=r.name, health=r.health,
                revenue_at_risk=r.revenue_at_risk,
            )
            for r in s.customer_resources
        ],
        recent_state_changes=[
            StateChange(
                subject_id=sc.subject_id, subject_kind=sc.subject_kind,
                from_state=sc.from_state, to_state=sc.to_state,
                at=sc.at, reason=sc.reason,
            )
            for sc in s.recent_state_changes
        ],
        anomalies=[
            AnomalyRef(
                id=a.id, kind=a.kind, description=a.description,
                severity=a.severity,
            )
            for a in s.anomalies
        ],
        conversation_context=ConversationContext(
            was_here_recently=s.conversation_context.was_here_recently,
            last_visit_at=s.conversation_context.last_visit_at,
            last_queries=list(s.conversation_context.last_queries),
        ),
        time_of_day_bucket=s.time_of_day_bucket,
        signals_watched_count=s.signals_watched_count,
    )


def _to_founder(f: FounderContextIn | None) -> FounderContext:
    if f is None:
        return FounderContext()
    return FounderContext(
        display_name=f.display_name,
        role=f.role,
        observed_rhythms=list(f.observed_rhythms),
        recent_interactions=list(f.recent_interactions),
    )


# ---------------------------------------------------------------------
# Request / response wire models
# ---------------------------------------------------------------------


class GreetingRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    substrate_state: SubstrateSnapshotIn
    founder_context: FounderContextIn | None = None


class CardRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    kind: Literal["observation", "decision", "question"]
    substrate_state: SubstrateSnapshotIn
    card_focus: dict[str, Any] = Field(default_factory=dict)
    founder_context: FounderContextIn | None = None


class QueryGridItemSpecIn(BaseModel):
    id: str
    icon: str
    hot: bool = False
    tag: Literal["urgent", "relevant", "2min", "evergreen"] | None = None
    intent: str
    query_template: str | None = None


class QueryGridRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    substrate_state: SubstrateSnapshotIn
    specs: list[QueryGridItemSpecIn]
    founder_context: FounderContextIn | None = None


class ConversationHistoryTurn(BaseModel):
    role: Literal["founder", "system"]
    text: str


class ConversationTurnRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    query: str
    retrieval_context: dict[str, Any] = Field(default_factory=dict)
    substrate_state: SubstrateSnapshotIn | None = None
    conversation_history: list[ConversationHistoryTurn] = Field(default_factory=list)
    founder_context: FounderContextIn | None = None


class CloseLineRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    signals_watched_count: int
    external_moves: int
    calibration_pct: int
    substrate_state: SubstrateSnapshotIn | None = None


class EvidenceRefIn(_WireConfig):
    actor: Optional[str] = None
    channel: Optional[str] = None
    t: Optional[datetime] = None
    excerpt: str = ""
    cite_id: Optional[str] = None
    kind: Optional[str] = None


class CardReasoningRequestBody(BaseModel):
    tenant_id: UUID
    timestamp: datetime
    card_kind: Literal["observation", "decision", "question"]
    card_subject: str
    card_body_context: str
    substrate_state: SubstrateSnapshotIn
    supporting_evidence: list[EvidenceRefIn] = Field(default_factory=list)
    founder_context: FounderContextIn | None = None


class RenderedEvidenceEntryOut(BaseModel):
    label: str
    body_html: str


class CardReasoningResponseOut(BaseModel):
    reasoning_html: str
    evidence: list[RenderedEvidenceEntryOut]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


# Response wire shapes — same fields as dataclasses; serialised via
# Pydantic so Decimals and datetimes marshal cleanly.


class GreetingResponseOut(BaseModel):
    body_html: str
    meta: dict[str, int]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


class CardResponseOut(BaseModel):
    body_html: str
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


class QueryChipOut(BaseModel):
    id: str
    icon: str
    label: str
    tag: str | None
    hot: bool


class QueryGridResponseOut(BaseModel):
    queries: list[QueryChipOut]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


class ConversationTurnResponseOut(BaseModel):
    response_html: str
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


class CloseLineResponseOut(BaseModel):
    body: str
    metadata: dict[str, int]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict]
    retried: bool
    flagged: bool
    latency_ms: int


# ---------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------


# Module-level service singleton (optional). Apps can either:
#   - construct the app via `create_app(service=...)`, which binds a
#     service to the Depends shim, OR
#   - allow the default Depends to call `RenderingService.from_env()`.
_service_singleton: RenderingService | None = None


def get_service() -> RenderingService:
    global _service_singleton
    if _service_singleton is None:
        _service_singleton = RenderingService.from_env()
    return _service_singleton


# ---------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------


router = APIRouter(prefix="/rendering", tags=["rendering"])


@router.post("/greeting", response_model=GreetingResponseOut)
async def route_greeting(
    body: GreetingRequestBody,
    service: RenderingService = Depends(get_service),
) -> GreetingResponseOut:
    req = RenderGreetingRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        substrate_state=_to_snapshot(body.substrate_state),
        founder_context=_to_founder(body.founder_context),
    )
    resp = await service.render_greeting(req)
    return GreetingResponseOut(
        body_html=resp.body_html,
        meta={"signals_watched_count": resp.meta.signals_watched_count},
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


@router.post("/card", response_model=CardResponseOut)
async def route_card(
    body: CardRequestBody,
    service: RenderingService = Depends(get_service),
) -> CardResponseOut:
    req = RenderCardRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        kind=body.kind,
        substrate_state=_to_snapshot(body.substrate_state),
        card_focus=body.card_focus,
        founder_context=_to_founder(body.founder_context),
    )
    if body.kind == "observation":
        resp = await service.render_card_observation(req)
    elif body.kind == "decision":
        resp = await service.render_card_decision(req)
    elif body.kind == "question":
        resp = await service.render_card_question(req)
    else:  # defensive; Pydantic validates already
        raise HTTPException(status_code=400, detail=f"unknown card kind: {body.kind}")
    return CardResponseOut(
        body_html=resp.body_html,
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


@router.post("/card-reasoning", response_model=CardReasoningResponseOut)
async def route_card_reasoning(
    body: CardReasoningRequestBody,
    service: RenderingService = Depends(get_service),
) -> CardReasoningResponseOut:
    """Gate 4b fix — render the expanded-card drawer's
    `reasoning_html` + `evidence[]` content via LLM.

    GRT (`services/greeting/`) calls this per card after it has the
    card body rendered, passing its structured evidence refs. The
    response entries plug straight into CONTRACTS §1.1
    `cards[].expanded.{reasoning_html, evidence}` without further
    transformation on the GRT side.
    """
    evs = [
        EvidenceRef(
            actor=e.actor,
            channel=e.channel,
            t=e.t,
            excerpt=e.excerpt,
            cite_id=e.cite_id,
            kind=e.kind,
        )
        for e in body.supporting_evidence
    ]
    req = RenderCardReasoningRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        card_kind=body.card_kind,
        card_subject=body.card_subject,
        card_body_context=body.card_body_context,
        substrate_state=_to_snapshot(body.substrate_state),
        supporting_evidence=evs,
        founder_context=_to_founder(body.founder_context),
    )
    resp = await service.render_card_reasoning(req)
    return CardReasoningResponseOut(
        reasoning_html=resp.reasoning_html,
        evidence=[
            RenderedEvidenceEntryOut(label=e.label, body_html=e.body_html)
            for e in resp.evidence
        ],
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


@router.post("/query-grid", response_model=QueryGridResponseOut)
async def route_query_grid(
    body: QueryGridRequestBody,
    service: RenderingService = Depends(get_service),
) -> QueryGridResponseOut:
    specs = [
        QueryGridItemSpec(
            id=s.id, icon=s.icon, hot=s.hot, tag=s.tag, intent=s.intent,
            query_template=s.query_template,
        )
        for s in body.specs
    ]
    req = RenderQueryGridRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        substrate_state=_to_snapshot(body.substrate_state),
        specs=specs,
        founder_context=_to_founder(body.founder_context),
    )
    resp = await service.render_query_grid(req)
    return QueryGridResponseOut(
        queries=[
            QueryChipOut(id=q.id, icon=q.icon, label=q.label, tag=q.tag, hot=q.hot)
            for q in resp.queries
        ],
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


@router.post("/conversation-turn", response_model=ConversationTurnResponseOut)
async def route_conversation_turn(
    body: ConversationTurnRequestBody,
    service: RenderingService = Depends(get_service),
) -> ConversationTurnResponseOut:
    req = RenderConversationTurnRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        query=body.query,
        retrieval_context=body.retrieval_context,
        substrate_state=_to_snapshot(body.substrate_state) if body.substrate_state else None,
        conversation_history=[
            ConversationTurn(role=t.role, text=t.text) for t in body.conversation_history
        ],
        founder_context=_to_founder(body.founder_context),
    )
    resp = await service.render_conversation_turn(req)
    return ConversationTurnResponseOut(
        response_html=resp.response_html,
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


@router.post("/close-line", response_model=CloseLineResponseOut)
async def route_close_line(
    body: CloseLineRequestBody,
    service: RenderingService = Depends(get_service),
) -> CloseLineResponseOut:
    req = RenderCloseLineRequest(
        tenant_id=body.tenant_id,
        timestamp=body.timestamp,
        signals_watched_count=body.signals_watched_count,
        external_moves=body.external_moves,
        calibration_pct=body.calibration_pct,
        substrate_state=_to_snapshot(body.substrate_state) if body.substrate_state else None,
    )
    resp = await service.render_close_line(req)
    return CloseLineResponseOut(
        body=resp.body,
        metadata=resp.metadata,
        rendering_model_used=resp.rendering_model_used,
        cost_usd=resp.cost_usd,
        violations=resp.violations,
        retried=resp.retried,
        flagged=resp.flagged,
        latency_ms=resp.latency_ms,
    )


def create_app(service: RenderingService | None = None) -> FastAPI:
    """Factory: build a FastAPI app with the rendering router attached.

    If `service` is provided, it is bound via a dependency override so
    tests and integration setups can inject a stub provider.
    """
    app = FastAPI(title="Company OS — Rendering Service", version="0.1.0")
    app.include_router(router)
    if service is not None:
        app.dependency_overrides[get_service] = lambda: service
    return app


__all__ = [
    "CardReasoningRequestBody",
    "CardReasoningResponseOut",
    "CardRequestBody",
    "CardResponseOut",
    "CloseLineRequestBody",
    "CloseLineResponseOut",
    "ConversationTurnRequestBody",
    "ConversationTurnResponseOut",
    "GreetingRequestBody",
    "GreetingResponseOut",
    "QueryGridRequestBody",
    "QueryGridResponseOut",
    "create_app",
    "get_service",
    "router",
]
