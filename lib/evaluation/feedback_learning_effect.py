"""Objective matched-effect evidence for non-authoritative feedback learning.

This evaluator deliberately credits only the behavior directly observed by the
active-surfaces experiment: settled source outcomes alter SAGE retrieval
salience in a matched adaptive arm while a frozen arm retains baseline policy.
It does not turn a policy-weight delta into a claim about selected evidence or
terminal answer quality.
"""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SourceSalienceObservation,
    validate_active_learning_surfaces_artifact,
)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class MatchedSalienceEffect(_Model):
    case_id: str = Field(min_length=1)
    expected_effect: Literal["increase", "nonincrease", "none"]
    frozen_salience: float
    adaptive_salience: float
    adaptive_minus_frozen: float
    direction_correct: bool
    truth_immutable: bool


class FeedbackLearningEffectReport(_Model):
    schema_version: Literal["feedback-learning-effect-report-v1"] = (
        "feedback-learning-effect-report-v1"
    )
    status: Literal["observed", "contradicted"]
    matched_pair_count: int = Field(ge=0)
    useful_pair_count: int = Field(ge=0)
    safety_pair_count: int = Field(ge=0)
    direction_correct_rate: float = Field(ge=0.0, le=1.0)
    truth_immutability_rate: float = Field(ge=0.0, le=1.0)
    useful_adaptive_minus_frozen: float
    safety_absolute_effect_mean: float = Field(ge=0.0)
    continuous_score: float = Field(ge=0.0, le=1.0)
    effects: tuple[MatchedSalienceEffect, ...]
    causal_claim: str
    excluded_claims: tuple[str, ...]
    source_evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def exact_accounting(self) -> Self:
        if self.matched_pair_count != len(self.effects):
            raise ValueError("matched pair count must equal effect rows")
        if self.useful_pair_count + self.safety_pair_count != self.matched_pair_count:
            raise ValueError("useful and safety populations must partition pairs")
        expected = "observed" if all(
            effect.direction_correct and effect.truth_immutable
            for effect in self.effects
        ) else "contradicted"
        if self.status != expected:
            raise ValueError("status must follow noncompensatory pair evidence")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class FeedbackLearningEffectEvidence(_Model):
    schema_version: Literal["objective-feedback-learning-evidence-v1"] = (
        "objective-feedback-learning-evidence-v1"
    )
    source_artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report: FeedbackLearningEffectReport

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {**self.model_dump(mode="json"), "evidence_digest": self.digest}


def compose_feedback_learning_effect(
    *,
    source_payload: dict[str, Any],
    source_artifact_sha256: str,
) -> FeedbackLearningEffectEvidence:
    source = validate_active_learning_surfaces_artifact(source_payload)
    effects = tuple(_effect(row) for row in source.salience_observations)
    useful = tuple(row for row in effects if row.expected_effect == "increase")
    safety = tuple(row for row in effects if row.expected_effect != "increase")
    direction_rate = _rate(row.direction_correct for row in effects)
    immutable_rate = _rate(row.truth_immutable for row in effects)
    useful_delta = (
        sum(row.adaptive_minus_frozen for row in useful) / len(useful)
        if useful else 0.0
    )
    safety_effect = (
        sum(abs(row.adaptive_minus_frozen) for row in safety) / len(safety)
        if safety else 0.0
    )
    # Score observed policy adaptation and safety, without rewarding effect size:
    # scale is policy-specific and is not terminal utility.
    score = (direction_rate + immutable_rate) / 2.0
    report = FeedbackLearningEffectReport(
        status=(
            "observed"
            if all(row.direction_correct and row.truth_immutable for row in effects)
            else "contradicted"
        ),
        matched_pair_count=len(effects),
        useful_pair_count=len(useful),
        safety_pair_count=len(safety),
        direction_correct_rate=direction_rate,
        truth_immutability_rate=immutable_rate,
        useful_adaptive_minus_frozen=useful_delta,
        safety_absolute_effect_mean=safety_effect,
        continuous_score=score,
        effects=effects,
        causal_claim=(
            "On these sealed matched cases, settled feedback caused the adaptive "
            "SAGE policy to change source salience in the intended direction "
            "relative to the frozen policy without mutating canonical truth."
        ),
        excluded_claims=(
            "selected-evidence quality improved",
            "terminal answer or company-model quality improved",
            "late retrieval became Model-first",
            "the effect generalizes beyond the sealed source cases",
        ),
        source_evidence_digest=source.digest,
    )
    return FeedbackLearningEffectEvidence(
        source_artifact_sha256=source_artifact_sha256,
        report=report,
    )


def validate_feedback_learning_effect_artifact(
    payload: dict[str, Any],
) -> FeedbackLearningEffectEvidence:
    supplied = str(payload.get("evidence_digest") or "")
    evidence = FeedbackLearningEffectEvidence.model_validate(
        {key: value for key, value in payload.items() if key != "evidence_digest"}
    )
    if supplied != evidence.digest:
        raise ValueError("feedback-learning evidence digest mismatch")
    return evidence


def _effect(row: SourceSalienceObservation) -> MatchedSalienceEffect:
    if row.execution_status != "observed":
        raise ValueError(f"feedback effect requires observed case: {row.case_id}")
    assert row.baseline_salience is not None
    assert row.learned_salience is not None
    expected: Literal["increase", "nonincrease", "none"]
    if row.case_id == "settled_useful":
        expected = "increase"
    elif row.case_id == "corrected":
        expected = "nonincrease"
    else:
        expected = "none"
    delta = row.learned_salience - row.baseline_salience
    correct = (
        delta > 0.0 if expected == "increase"
        else delta <= 0.0 if expected == "nonincrease"
        else delta == 0.0
    )
    return MatchedSalienceEffect(
        case_id=row.case_id,
        expected_effect=expected,
        frozen_salience=row.baseline_salience,
        adaptive_salience=row.learned_salience,
        adaptive_minus_frozen=delta,
        direction_correct=correct,
        truth_immutable=row.immutable,
    )


def _rate(values: Any) -> float:
    rows = tuple(bool(value) for value in values)
    return sum(rows) / len(rows) if rows else 0.0


__all__ = [
    "FeedbackLearningEffectEvidence",
    "FeedbackLearningEffectReport",
    "MatchedSalienceEffect",
    "compose_feedback_learning_effect",
    "validate_feedback_learning_effect_artifact",
]
