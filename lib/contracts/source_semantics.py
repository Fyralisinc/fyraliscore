"""Contracts for the first grounded source-semantics to belief vertical.

The source interpretation objects themselves are defined in
``lib.contracts.perception``.  This module only binds those existing objects to
one grounding trace and describes the narrow belief proposal admitted by the
initial vertical slice.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    GroundingContinuity,
    SemanticFrameCandidate,
    SourceAssertion,
    SpeechActCandidate,
)


class _SourceSemanticContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


class SourceSemanticAdmissionDisposition(StrEnum):
    BELIEF_APPLIED = "belief_applied"
    NO_ADMISSION = "no_admission"


class GroundedSourceSemanticBundle(_SourceSemanticContract):
    """One independently versioned interpretation over a grounding trace."""

    tenant_id: UUID
    grounding_trace_id: UUID
    source_assertion: SourceAssertion
    semantic_frame: SemanticFrameCandidate
    speech_act: SpeechActCandidate

    @model_validator(mode="after")
    def semantic_objects_share_one_assertion(self) -> Self:
        assertion_id = self.source_assertion.assertion_id
        if self.semantic_frame.source_assertion_id != assertion_id:
            raise ValueError("semantic frame must reference the source assertion")
        if self.speech_act.source_assertion_id != assertion_id:
            raise ValueError("speech act must reference the source assertion")
        return self

    @property
    def bundle_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ProposedBeliefAssertion(_SourceSemanticContract):
    """Temporary proposal accepted only by the narrow EpistemicApplier lane."""

    proposal_id: UUID
    proposal_version: int = Field(default=1, ge=1)
    proposed_model_id: UUID
    tenant_id: UUID
    interpretation_id: UUID
    source_assertion_id: str = Field(min_length=1)
    semantic_frame_id: str = Field(min_length=1)
    speech_act_id: str = Field(min_length=1)
    grounding_continuity: GroundingContinuity
    natural: str = Field(min_length=1)
    proposition: dict[str, Any]
    confidence: float = Field(ge=0.05, le=0.69)

    @model_validator(mode="after")
    def proposal_is_one_grounded_belief(self) -> Self:
        if self.proposition.get("kind") != "belief":
            raise ValueError("grounded belief proposal must use the belief stance")
        if self.grounding_continuity.downstream_object_ref != (
            f"model:{self.proposed_model_id}"
        ):
            raise ValueError("grounding continuity must name the proposed Model")
        return self

    @property
    def proposal_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class GroundedBeliefApplyResult(_SourceSemanticContract):
    interpretation_id: UUID
    admission_decision_id: UUID
    disposition: SourceSemanticAdmissionDisposition
    reason_codes: tuple[str, ...] = Field(min_length=1)
    model_id: UUID | None = None
    duplicate: bool = False

    @model_validator(mode="after")
    def model_exists_only_for_applied_belief(self) -> Self:
        applied = self.disposition is SourceSemanticAdmissionDisposition.BELIEF_APPLIED
        if applied != (self.model_id is not None):
            raise ValueError("only an applied belief admission may carry a Model ID")
        return self


__all__ = [
    "GroundedBeliefApplyResult",
    "GroundedSourceSemanticBundle",
    "ProposedBeliefAssertion",
    "SourceSemanticAdmissionDisposition",
]
