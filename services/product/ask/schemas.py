"""Wire and domain schemas for Ask Fyralis."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


AskScopeType = Literal[
    "current_object",
    "current_page",
    "account",
    "deal",
    "goal",
    "team",
    "region",
    "role_view",
    "whole_company",
    "custom",
]

AskMode = Literal[
    "direct_synthesis_read",
    "quick_inquiry",
    "deep_inquiry",
    "background_review",
]

AskAction = Literal["accept", "reject", "delegate", "deep_review"]


class AskScope(BaseModel):
    type: AskScopeType = "current_page"
    label: str = "Current page"
    root_node_ids: list[UUID] = Field(default_factory=list)
    related_entity_ids: list[UUID] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    access_mode: Literal["full", "partial", "restricted"] = "full"


class AskSession(BaseModel):
    id: UUID
    tenant_id: UUID
    viewer_id: UUID
    initial_scope: AskScope
    current_scope: AskScope
    source_route: str | None = None
    source_object_id: UUID | None = None
    source_object_type: str | None = None
    mode: AskMode
    status: Literal["open", "closed", "failed"]
    created_at: datetime
    updated_at: datetime


class AskSessionCreateRequest(BaseModel):
    initial_scope: AskScope
    source_route: str | None = None
    source_object_id: UUID | None = None
    source_object_type: str | None = None


class AskSessionCreateResponse(BaseModel):
    session: AskSession


class AskTurnRequest(BaseModel):
    query: str
    scope: AskScope | None = None
    requested_mode: AskMode | None = None


class AskRelatedNode(BaseModel):
    id: UUID
    label: str
    confidence: float | None = None
    activation: float | None = None
    role: str = "supporting"


class AskEvidenceItem(BaseModel):
    id: UUID
    source_ref: UUID | None = None
    source_kind: str
    summary: str
    strength: Literal[
        "decisive", "supporting", "contextual", "weak", "counterevidence", "unknown"
    ] = "contextual"
    supports_answer: bool = False
    is_counterevidence: bool = False
    token_estimate: int | None = None
    omitted_reason: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class AskProposedStateChange(BaseModel):
    id: UUID
    answer_id: UUID
    proposed_op: dict[str, Any]
    status: Literal[
        "proposed", "accepted", "rejected", "delegated",
        "applied", "failed_validation",
    ]
    linked_trigger_id: UUID | None = None


class AskAnswerPayload(BaseModel):
    answer: str
    confidence: float
    premise_check: dict[str, Any] = Field(default_factory=dict)
    state_facts: list[dict[str, Any]] = Field(default_factory=list)
    why: list[str] = Field(default_factory=list)
    counterevidence: list[str] = Field(default_factory=list)
    impact: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    related_nodes: list[AskRelatedNode] = Field(default_factory=list)
    evidence: list[AskEvidenceItem] = Field(default_factory=list)
    omitted_evidence_count: int = 0
    possible_state_change: AskProposedStateChange | None = None


class AskTurnResponse(BaseModel):
    session: AskSession
    message_id: UUID
    answer_id: UUID
    retrieval_run_id: UUID
    mode: AskMode
    intent: str
    latency_ms: int
    payload: AskAnswerPayload


class EvidenceExpansionRequest(BaseModel):
    retrieval_run_id: UUID


class EvidenceExpansionResponse(BaseModel):
    retrieval_run_id: UUID
    evidence: list[AskEvidenceItem]
    omitted: list[AskEvidenceItem]


class ProposedStateChangeActionRequest(BaseModel):
    action: AskAction
    note: str | None = None
    delegate_to: str | None = None


class ProposedStateChangeActionResponse(BaseModel):
    change: AskProposedStateChange


class AskFeedbackRequest(BaseModel):
    session_id: UUID
    answer_id: UUID | None = None
    feedback_type: Literal[
        "helpful", "wrong", "missing_context", "too_verbose", "unsafe", "irrelevant"
    ]
    payload: dict[str, Any] = Field(default_factory=dict)


class AskFeedbackResponse(BaseModel):
    ok: bool
    feedback_id: UUID
