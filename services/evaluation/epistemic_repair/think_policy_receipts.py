"""TI4-min immutable policy identities, evaluation receipts, and replay."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256

from .think_semantic_scorer import FailureClass, SemanticScorerResult


class PolicyIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    prompt_policy_version: str = Field(min_length=1)
    provider_schema_version: str = Field(min_length=1)
    compiler_version: str = Field(min_length=1)
    routing_policy_version: str = Field(min_length=1)
    model: str = Field(min_length=1)
    effort: Literal["none", "low", "medium", "high", "xhigh"]

    @property
    def content_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PolicyCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sequence: int = Field(ge=1)
    policy: PolicyIdentity
    promoted: bool
    evaluation_result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class EvaluationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    receipt_version: Literal["think-semantic-evaluation-receipt-v1"]
    attempt_id: str = Field(min_length=1)
    case_id: str = Field(min_length=1)
    dossier_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_identity: PolicyIdentity
    policy_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_decision_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    compiler_receipt_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    scorer_case_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    scorer_version: Literal["think-semantic-result-v1"]
    scorer_result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluated_at: datetime
    failure_class: FailureClass | None = None
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def verify_identity_and_digest(self) -> "EvaluationReceipt":
        if self.policy_digest != self.policy_identity.content_digest:
            raise ValueError("policy identity digest mismatch")
        body = self.model_dump(mode="json", exclude={"content_digest"})
        if self.content_digest != canonical_sha256(body):
            raise ValueError("evaluation receipt digest mismatch")
        return self


def build_evaluation_receipt(
    *, attempt_id: str, dossier_digest: str, policy: PolicyIdentity,
    raw_decision_digest: str, scorer_result: SemanticScorerResult,
    compiler_receipt_digest: str | None = None,
    evaluated_at: datetime | None = None,
) -> EvaluationReceipt:
    body = {
        "receipt_version": "think-semantic-evaluation-receipt-v1",
        "attempt_id": attempt_id, "case_id": scorer_result.case_id,
        "dossier_digest": dossier_digest, "policy_identity": policy.model_dump(mode="json"),
        "policy_digest": policy.content_digest, "raw_decision_digest": raw_decision_digest,
        "compiler_receipt_digest": compiler_receipt_digest,
        "scorer_case_digest": scorer_result.case_digest,
        "scorer_version": scorer_result.schema_version,
        "scorer_result_digest": scorer_result.content_digest,
        "evaluated_at": (evaluated_at or datetime.now(timezone.utc)).isoformat().replace(
            "+00:00", "Z"
        ),
        "failure_class": scorer_result.failure_class,
    }
    return EvaluationReceipt(**body, content_digest=canonical_sha256(body))


def replay_evaluation_receipt(
    receipt: EvaluationReceipt,
    *, raw_decision: Mapping[str, Any], scorer_result: SemanticScorerResult,
    compiler_receipt: Mapping[str, Any] | None = None,
) -> bool:
    """Provider-free digest replay for immutable captured artifacts."""
    if canonical_sha256(dict(raw_decision)) != receipt.raw_decision_digest:
        return False
    if scorer_result.content_digest != receipt.scorer_result_digest:
        return False
    if scorer_result.case_digest != receipt.scorer_case_digest:
        return False
    if compiler_receipt is None:
        return receipt.compiler_receipt_digest is None
    return canonical_sha256(dict(compiler_receipt)) == receipt.compiler_receipt_digest


def deterministic_rollback(
    checkpoints: Sequence[PolicyCheckpoint], *, current_policy_digest: str,
) -> PolicyIdentity:
    """Select the latest earlier promoted immutable policy, never mutate live text."""
    ordered = sorted(checkpoints, key=lambda row: row.sequence)
    positions = [index for index, row in enumerate(ordered)
                 if row.policy.content_digest == current_policy_digest]
    if len(positions) != 1:
        raise ValueError("current policy checkpoint is missing or ambiguous")
    earlier = [row for row in ordered[:positions[0]] if row.promoted]
    if not earlier:
        raise ValueError("no promoted rollback checkpoint exists")
    return earlier[-1].policy


__all__ = [
    "EvaluationReceipt", "PolicyCheckpoint", "PolicyIdentity",
    "build_evaluation_receipt", "deterministic_rollback", "replay_evaluation_receipt",
]
