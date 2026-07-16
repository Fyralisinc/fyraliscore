"""Build-time architecture hypothesis and commitment-class contracts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ArchitectureCommitmentClass(StrEnum):
    CONSTITUTIONAL_INVARIANT = "constitutional_invariant"
    STABLE_SEMANTIC_CONTRACT = "stable_semantic_contract"
    GOVERNED_POLICY = "governed_policy"
    EMPIRICAL_HYPOTHESIS = "empirical_hypothesis"
    REBUILDABLE_MECHANISM = "rebuildable_mechanism"


class ArchitectureHypothesisStatus(StrEnum):
    PROPOSED = "proposed"
    RUNNING = "running"
    MEASURED = "measured"
    CONTRADICTED = "contradicted"
    SUPPORTED_WITH_SCOPE = "supported_with_scope"
    ADOPTED = "adopted"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"


class ArchitectureDecision(StrEnum):
    RETAIN = "retain"
    REVISE = "revise"
    NARROW = "narrow"
    REJECT = "reject"
    DEFER = "defer"


class ArchitectureHypothesis(_FrozenContract):
    hypothesis_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    statement: str = Field(min_length=1)
    commitment_class: ArchitectureCommitmentClass = (
        ArchitectureCommitmentClass.EMPIRICAL_HYPOTHESIS
    )
    predicted_metric_changes: dict[str, float] = Field(min_length=1)
    credible_alternatives: tuple[str, ...] = Field(min_length=1)
    population: str = Field(min_length=1)
    operating_region: str = Field(min_length=1)
    budget: str = Field(min_length=1)
    minimum_effect: float
    safety_noninferiority: tuple[str, ...] = Field(min_length=1)
    rollback_plan: str = Field(min_length=1)
    registered_at: datetime
    expires_at: datetime
    status: ArchitectureHypothesisStatus = ArchitectureHypothesisStatus.PROPOSED

    @model_validator(mode="after")
    def hypothesis_is_empirical_and_forward_looking(self) -> Self:
        if self.commitment_class is not ArchitectureCommitmentClass.EMPIRICAL_HYPOTHESIS:
            raise ValueError("ArchitectureHypothesis must be an empirical hypothesis")
        if self.expires_at <= self.registered_at:
            raise ValueError("hypothesis expiry must follow registration")
        return self


class ArchitectureDecisionRecord(_FrozenContract):
    decision_id: str = Field(min_length=1)
    hypothesis_id: str = Field(min_length=1)
    hypothesis_version: str = Field(min_length=1)
    decision: ArchitectureDecision
    evidence_refs: tuple[str, ...]
    operating_region: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    residual_uncertainty: tuple[str, ...] = ()
    compatibility_impact: str = Field(min_length=1)
    follow_up_obligations: tuple[str, ...] = ()
    decided_by: str = Field(min_length=1)
    decided_at: datetime

    @model_validator(mode="after")
    def substantive_decisions_require_evidence(self) -> Self:
        if self.decision is not ArchitectureDecision.DEFER and not self.evidence_refs:
            raise ValueError("architecture decisions require evidence references")
        if self.decision is ArchitectureDecision.DEFER and not (
            self.follow_up_obligations
        ):
            raise ValueError("deferred architecture decisions require follow-up work")
        return self
