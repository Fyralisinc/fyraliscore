"""Public data shapes for adaptive inquiry execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from services.reasoning.retrieval.primary import RetrievalResult


SignalRoute = Literal[
    "IGNORE_OR_ARCHIVE",
    "DETERMINISTIC_UPDATE",
    "FAST_PATH",
    "DEEP_INQUIRY_PATH",
    "BACKGROUND_PATH",
    "HUMAN_VALIDATION_PATH",
]

InquiryStopStatus = Literal[
    "sufficient_for_reasoning",
    "insufficient_continue",
    "insufficient_defer",
    "human_validation_required",
    "no_update_needed",
    "budget_exhausted",
]

RetrievalActionPath = Literal[
    "structural",
    "focused_index",
    "semantic_terms",
    "semantic",
    "temporal",
    "pattern",
    "model_edge",
    "sage_reader",
]

MemoryDecisionOpFamily = Literal[
    "claim_insert",
    "claim_update",
    "edge_insert",
    "act_update",
    "prediction",
    "no_op",
]


@dataclass(frozen=True, slots=True)
class Hypothesis:
    id: str
    claim: str
    confidence: float
    impact_if_true: str
    delta_type: str | None = None
    target_model_ids: tuple[str, ...] = ()
    affected_entities: tuple[str, ...] = ()
    uncertainty_slots: tuple[str, ...] = ()
    evidence_needed: tuple[str, ...] = ()
    source: str = "deterministic"


@dataclass(frozen=True, slots=True)
class InquiryQuestion:
    question_id: str
    question: str
    primitive: str
    tests_hypotheses: tuple[str, ...]
    expected_value: float
    expected_cost: float
    retrieval_target: str
    stop_condition: str
    score: float
    round_index: int = 0


@dataclass(frozen=True, slots=True)
class RetrievalAction:
    question_id: str
    path: RetrievalActionPath
    target: str
    query: str | None = None
    filters: dict[str, Any] = field(default_factory=dict)
    budget: int = 25


@dataclass(frozen=True, slots=True)
class ReconstructionState:
    """Compact state used to steer the next memory read."""

    round_index: int
    summary: str
    active_cues: tuple[str, ...] = ()
    active_tags: tuple[str, ...] = ()
    unresolved_slots: tuple[str, ...] = ()
    known_model_ids: tuple[str, ...] = ()
    known_observation_ids: tuple[str, ...] = ()
    supporting_refs: tuple[str, ...] = ()
    counterevidence_refs: tuple[str, ...] = ()
    answered_questions: tuple[str, ...] = ()
    inconclusive_questions: tuple[str, ...] = ()
    operator_bias: tuple[str, ...] = ()
    hypothesis_status: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_evidence: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class MemoryDecisionCandidate:
    """Typed, advisory decision surface for final Think reasoning.

    Candidates are not writes. They summarize the plausible memory decisions
    the inquiry packet has narrowed to, plus the uncertainty still left for
    Think to adjudicate.
    """

    candidate_id: str
    op_family: MemoryDecisionOpFamily
    proposed_text: str
    target_model_ids: tuple[str, ...] = ()
    target_act_ids: tuple[str, ...] = ()
    source_observation_ids: tuple[str, ...] = ()
    member_observation_ids: tuple[str, ...] = ()
    semantic_scope: tuple[str, ...] = ()
    observation_evidence: tuple[dict[str, str], ...] = ()
    evidence_model_ids: tuple[str, ...] = ()
    supporting_evidence_ids: tuple[str, ...] = ()
    counterevidence_ids: tuple[str, ...] = ()
    uncertainty_slots: tuple[str, ...] = ()
    retrieval_targets: tuple[str, ...] = ()
    suggested_edge_kinds: tuple[str, ...] = ()
    write_preconditions: tuple[str, ...] = ()
    answer_summary: str = ""
    confidence: float = 0.0
    reason: str = ""


@dataclass(frozen=True, slots=True)
class LearnedRetrievalMotif:
    id: UUID
    signature: dict[str, Any]
    question_primitive: str
    plan: dict[str, Any]
    utility_score: float
    success_count: int
    match_score: float


@dataclass(frozen=True, slots=True)
class QuestionPolicySignal:
    signal_type: str
    question_primitive: str
    attempts: int
    successes: int
    utility_score: float
    total_credit: float
    total_cost: float


@dataclass(slots=True)
class EvidenceCard:
    evidence_id: UUID
    source_type: str
    source_ref: str
    source_ref_id: UUID | None
    summary: str
    trust_tier: str | None
    timestamp: datetime | None
    retrieval_paths: set[str] = field(default_factory=set)
    retrieved_for_questions: set[str] = field(default_factory=set)
    supports_hypotheses: set[str] = field(default_factory=set)
    weakens_hypotheses: set[str] = field(default_factory=set)
    contradicts_hypotheses: set[str] = field(default_factory=set)
    raw_content_ref: str | None = None
    token_estimate: int = 1
    access_scope: str = "tenant"
    sensitivity: str = "normal"
    score: float = 0.0

    def merge(
        self,
        *,
        path: str,
        question_id: str,
        supports: set[str] | None = None,
        weakens: set[str] | None = None,
        contradicts: set[str] | None = None,
        score: float = 0.0,
    ) -> None:
        self.retrieval_paths.add(path)
        self.retrieved_for_questions.add(question_id)
        self.supports_hypotheses.update(supports or set())
        self.weakens_hypotheses.update(weakens or set())
        self.contradicts_hypotheses.update(contradicts or set())
        self.score = max(self.score, float(score))


@dataclass(frozen=True, slots=True)
class ResidualDebtCard:
    """Compact non-canonical model-metabolism debt for packet sidecars."""

    residual_id: UUID | str | None
    residual_kind: str
    compact_summary: str
    reason: str = ""
    status: str = "open"
    source_observation_id: UUID | str | None = None
    model_id: UUID | str | None = None


@dataclass(frozen=True, slots=True)
class QuestionAnswer:
    question_id: str
    answer_status: str
    summary: str
    supporting_evidence: tuple[str, ...] = ()
    counterevidence: tuple[str, ...] = ()
    new_uncertainties: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SufficiencyVerdict:
    status: InquiryStopStatus
    reason: str
    evidence_count: int
    answered_questions: int
    remaining_unknowns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class InquiryResult:
    session_id: UUID
    route: SignalRoute
    retrieval_result: RetrievalResult
    hypotheses: tuple[Hypothesis, ...]
    questions: tuple[InquiryQuestion, ...]
    retrieval_actions: tuple[RetrievalAction, ...]
    question_answers: tuple[QuestionAnswer, ...]
    evidence_cards: tuple[EvidenceCard, ...]
    sufficiency: SufficiencyVerdict
    context_packet: dict[str, Any]
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelRelevance:
    model_id: UUID
    final_score: float
    base_score: float
    lexical_score: float
    scope_score: float
    path_score: float
    evidence_score: float
    provenance_score: float
    penalty: float
    reasons: tuple[str, ...]


__all__ = [
    "EvidenceCard",
    "Hypothesis",
    "InquiryQuestion",
    "InquiryResult",
    "InquiryStopStatus",
    "LearnedRetrievalMotif",
    "MemoryDecisionCandidate",
    "MemoryDecisionOpFamily",
    "ModelRelevance",
    "QuestionAnswer",
    "QuestionPolicySignal",
    "ReconstructionState",
    "ResidualDebtCard",
    "RetrievalAction",
    "RetrievalActionPath",
    "SignalRoute",
    "SufficiencyVerdict",
]
