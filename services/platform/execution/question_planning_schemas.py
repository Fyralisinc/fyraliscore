"""Structured LLM output schemas for inquiry question planning."""

from __future__ import annotations

from pydantic import BaseModel, Field


class LLMInquiryQuestionSpec(BaseModel):
    primitive: str = Field(
        description=(
            "One of DEPENDENCY, COMMITMENT, CONSTRAINT, COUNTEREVIDENCE, "
            "OWNERSHIP, GOAL_IMPACT, RECURRENCE."
        )
    )
    question: str = Field(
        min_length=8,
        max_length=240,
        description="The concrete retrieval question to ask next.",
    )
    retrieval_target: str | None = Field(
        default=None,
        max_length=120,
        description="Compact target such as active_commitments or pattern+model_edges.",
    )
    expected_value: float = Field(ge=0.0, le=1.0)
    expected_cost: float = Field(ge=0.0, le=1.0)
    tests_hypotheses: list[str] = Field(default_factory=list, max_length=4)
    stop_condition: str | None = Field(default=None, max_length=180)


class LLMBeliefDeltaSpec(BaseModel):
    delta_id: str | None = Field(default=None, max_length=96)
    claim_atom: str = Field(
        min_length=8,
        max_length=240,
        description="Atomic belief candidate implied by the signal.",
    )
    delta_type: str = Field(
        default="update",
        max_length=32,
        description=("One of create, update, weaken, split, merge, supersede, no_op."),
    )
    target_model_ids: list[str] = Field(default_factory=list, max_length=5)
    affected_entities: list[str] = Field(default_factory=list, max_length=8)
    uncertainty_slots: list[str] = Field(default_factory=list, max_length=8)
    evidence_needed: list[str] = Field(default_factory=list, max_length=8)
    impact_if_true: str = Field(default="medium", max_length=16)
    confidence: float = Field(default=0.45, ge=0.0, le=1.0)


class LLMInquiryQuestionPlan(BaseModel):
    rationale: str | None = Field(default=None, max_length=500)
    belief_deltas: list[LLMBeliefDeltaSpec] = Field(
        default_factory=list,
        max_length=5,
    )
    questions: list[LLMInquiryQuestionSpec] = Field(default_factory=list, max_length=6)


class LLMCompactQuestionSpec(BaseModel):
    p: str = Field(max_length=32)
    q: str = Field(min_length=8, max_length=180)
    v: float = Field(default=0.74, ge=0.0, le=1.0)
    c: float = Field(default=0.24, ge=0.0, le=1.0)


class LLMCompactBeliefDeltaSpec(BaseModel):
    i: str | None = Field(default=None, max_length=96)
    claim: str = Field(min_length=8, max_length=220)
    type: str = Field(default="update", max_length=32)
    entities: list[str] = Field(default_factory=list, max_length=6)
    slots: list[str] = Field(default_factory=list, max_length=5)
    evidence: list[str] = Field(default_factory=list, max_length=5)
    impact: str = Field(default="medium", max_length=16)
    conf: float = Field(default=0.45, ge=0.0, le=1.0)


class LLMCompactQuestionPlan(BaseModel):
    r: str | None = Field(default=None, max_length=300)
    d: list[LLMCompactBeliefDeltaSpec] = Field(default_factory=list, max_length=4)
    q: list[LLMCompactQuestionSpec] = Field(default_factory=list, max_length=3)


__all__ = [
    "LLMBeliefDeltaSpec",
    "LLMCompactBeliefDeltaSpec",
    "LLMCompactQuestionPlan",
    "LLMCompactQuestionSpec",
    "LLMInquiryQuestionPlan",
    "LLMInquiryQuestionSpec",
]
