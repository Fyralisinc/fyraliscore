"""services/rendering/contracts.py — request/response types for the service.

Mirrors CONTRACTS.md §2.1 (RenderGreetingRequest/Response and siblings)
and §2.3 (SubstrateSnapshot shape). Agent-GRT owns the concrete
snapshot shape in `services/greeting/snapshot.py`; this module holds
the dataclasses the rendering layer consumes today so it can be tested
in isolation.

Design rule: we deliberately model SubstrateSnapshot as a permissive
dict-shaped dataclass. Agent-GRT may extend fields; the rendering layer
reads by key with safe defaults so contract evolution doesn't break
this service.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID


# ---------------------------------------------------------------------
# SubstrateSnapshot + supporting shapes (rendering-layer view).
# ---------------------------------------------------------------------


@dataclass
class ModelRef:
    """Shape the rendering layer reads from a Model row. Agent-GRT
    produces these from services/models/; we accept unknown keys in
    `extra` for forward-compatibility."""
    id: str                     # e.g. "m-2841"
    claim: str                  # natural-language statement of belief
    confidence: float           # current confidence in [0, 1]
    prior_confidence: float | None = None
    state_changed_at: datetime | None = None
    falsifier: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class CommitmentRef:
    id: str
    label: str                  # e.g. "ship rate-limiter for Acme"
    owner_name: str | None = None
    state: str = "Open"         # Open | InProgress | Blocked | DoneVerified | ...
    due_at: datetime | None = None
    pressure: str | None = None  # "high" | "medium" | "low" — GRT-computed
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceRef:
    id: str
    kind: str                   # "customer" | "goal" | "project" | ...
    name: str                   # e.g. "Acme"
    health: str = "healthy"     # healthy | warning | critical
    revenue_at_risk: str | None = None  # pre-formatted "$487K"
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class StateChange:
    subject_id: str             # entity whose state changed
    subject_kind: str           # "model" | "commitment" | "resource" | "goal"
    from_state: str
    to_state: str
    at: datetime
    reason: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyRef:
    id: str
    kind: str                   # "silence" | "re_estimation" | "slip" | ...
    description: str
    severity: str = "medium"    # low | medium | high
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConversationContext:
    was_here_recently: bool = False
    last_visit_at: datetime | None = None
    last_queries: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class FounderContext:
    """Who is reading, and how they read."""
    display_name: str = "the founder"
    role: str = "ceo"
    observed_rhythms: list[str] = field(default_factory=list)
    recent_interactions: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubstrateSnapshot:
    """What Agent-GRT hands to rendering. See CONTRACTS.md §2.3."""
    tenant_id: UUID
    captured_at: datetime
    top_models: list[ModelRef] = field(default_factory=list)
    active_commitments: list[CommitmentRef] = field(default_factory=list)
    customer_resources: list[ResourceRef] = field(default_factory=list)
    recent_state_changes: list[StateChange] = field(default_factory=list)
    anomalies: list[AnomalyRef] = field(default_factory=list)
    conversation_context: ConversationContext = field(default_factory=ConversationContext)
    time_of_day_bucket: Literal[
        "early_morning", "morning", "afternoon", "evening", "late"
    ] = "morning"
    signals_watched_count: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Request / response envelopes.
# ---------------------------------------------------------------------


@dataclass
class RenderMeta:
    signals_watched_count: int = 0


@dataclass
class RenderGreetingRequest:
    tenant_id: UUID
    timestamp: datetime
    substrate_state: SubstrateSnapshot
    founder_context: FounderContext = field(default_factory=FounderContext)


@dataclass
class RenderGreetingResponse:
    body_html: str
    meta: RenderMeta
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


# ---------- Card types ----------

CardKind = Literal["observation", "decision", "question"]


@dataclass
class RenderCardRequest:
    tenant_id: UUID
    timestamp: datetime
    kind: CardKind
    # A focused view of the substrate relevant to this card. The
    # rendering layer does not need the full snapshot to render a
    # single card body. Callers populate `substrate_state` for
    # convenience + full-context rendering; `card_focus` names the
    # specific model/resource/commitment this card is about.
    substrate_state: SubstrateSnapshot
    card_focus: dict[str, Any] = field(default_factory=dict)
    founder_context: FounderContext = field(default_factory=FounderContext)


@dataclass
class RenderCardResponse:
    body_html: str
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


# ---------- Query grid ----------


@dataclass
class QueryGridItemSpec:
    """Agent-GRT supplies the structural intent of each chip; rendering
    crafts the label. Icon + tag + hot are contract-mandated fields from
    CONTRACTS.md §1.1 and are NOT rendered — they're passed through."""
    id: str
    icon: str                   # from the fixed set (CONTRACTS.md §4)
    hot: bool
    tag: Literal["urgent", "relevant", "2min", "evergreen"] | None
    intent: str                 # structural description: what this query is about
    query_template: str | None = None  # executed when the chip is tapped


@dataclass
class RenderQueryGridRequest:
    tenant_id: UUID
    timestamp: datetime
    substrate_state: SubstrateSnapshot
    specs: list[QueryGridItemSpec]
    founder_context: FounderContext = field(default_factory=FounderContext)


@dataclass
class RenderedQueryChip:
    id: str
    icon: str
    label: str
    tag: str | None
    hot: bool


@dataclass
class RenderQueryGridResponse:
    queries: list[RenderedQueryChip]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


# ---------- Conversation turn ----------


@dataclass
class ConversationTurn:
    role: Literal["founder", "system"]
    text: str


@dataclass
class RenderConversationTurnRequest:
    tenant_id: UUID
    timestamp: datetime
    query: str
    # The retrieval output that should ground the response. Rendering
    # reads these structurally and cites them inline.
    retrieval_context: dict[str, Any] = field(default_factory=dict)
    substrate_state: SubstrateSnapshot | None = None
    conversation_history: list[ConversationTurn] = field(default_factory=list)
    founder_context: FounderContext = field(default_factory=FounderContext)


@dataclass
class RenderConversationTurnResponse:
    response_html: str
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


# ---------- Card reasoning (Gate 4b fix) ----------


@dataclass
class EvidenceRef:
    """Raw structured evidence row Agent-GRT gathers on the card-candidate
    path. RND reads these into the prompt and renders each as a
    `{label, body_html}` entry with `.cite`/`.note` spans.

    Fields are deliberately small and stringly-typed — GRT pulls them
    from state_changes / Observations / Commitments and passes through
    without a heavy shape contract. Excerpt is the short prose we want
    the rendered evidence body to quote or summarise.
    """
    actor: str | None = None              # "Alice", "linear webhook", ...
    channel: str | None = None            # "slack_eng", "linear", ...
    t: datetime | None = None             # when it happened (UTC)
    excerpt: str = ""                     # short quote / summary
    cite_id: str | None = None            # cite code (obs-88412, c-187, m-2841)
    kind: str | None = None               # state_change | slack | ... (GRT-shape)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class RenderCardReasoningRequest:
    tenant_id: UUID
    timestamp: datetime
    card_kind: Literal["observation", "decision", "question"]
    card_subject: str                      # short label — "Acme renewal"
    card_body_context: str                 # rendered body_html the LLM grounds against
    substrate_state: SubstrateSnapshot     # reused snapshot shape
    supporting_evidence: list[EvidenceRef] = field(default_factory=list)
    founder_context: FounderContext = field(default_factory=FounderContext)


@dataclass
class RenderedEvidenceEntry:
    """One entry in the rendered evidence list — matches CONTRACTS §1.1
    `cards[].expanded.evidence[]` shape (`label` + `body_html`)."""
    label: str
    body_html: str


@dataclass
class RenderCardReasoningResponse:
    reasoning_html: str                    # uses .serif / .hl / .n / .cite / .note per §5
    evidence: list[RenderedEvidenceEntry]  # rendered evidence entries
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


# ---------- Close line ----------


@dataclass
class RenderCloseLineRequest:
    tenant_id: UUID
    timestamp: datetime
    signals_watched_count: int
    external_moves: int
    calibration_pct: int
    substrate_state: SubstrateSnapshot | None = None


@dataclass
class RenderCloseLineResponse:
    body: str                   # plain text; no span classes in close line
    metadata: dict[str, int]
    rendering_model_used: str
    cost_usd: Decimal
    violations: list[dict] = field(default_factory=list)
    retried: bool = False
    flagged: bool = False
    latency_ms: int = 0


__all__ = [
    "AnomalyRef",
    "CardKind",
    "CommitmentRef",
    "ConversationContext",
    "ConversationTurn",
    "EvidenceRef",
    "FounderContext",
    "ModelRef",
    "QueryGridItemSpec",
    "RenderCardReasoningRequest",
    "RenderCardReasoningResponse",
    "RenderCardRequest",
    "RenderCardResponse",
    "RenderCloseLineRequest",
    "RenderCloseLineResponse",
    "RenderConversationTurnRequest",
    "RenderConversationTurnResponse",
    "RenderGreetingRequest",
    "RenderGreetingResponse",
    "RenderMeta",
    "RenderQueryGridRequest",
    "RenderQueryGridResponse",
    "RenderedEvidenceEntry",
    "RenderedQueryChip",
    "ResourceRef",
    "StateChange",
    "SubstrateSnapshot",
]
