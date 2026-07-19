"""Independent TI3 semantic scorer for frozen synthesis decisions.

Gold exists only in this evaluation package.  The scorer consumes frozen raw
decision artifacts and compiler/apply receipts; runtime reasoning code never
imports these contracts.
"""
from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256


Digest = str
FailureClass = Literal[
    "context_dossier", "semantic_model", "schema_binding", "compiler",
    "validator_applier", "evaluator", "infrastructure",
]


class ConfidenceBand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    minimum: float = Field(ge=0, le=1)
    maximum: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def ordered(self) -> "ConfidenceBand":
        if self.minimum > self.maximum:
            raise ValueError("confidence band is reversed")
        return self


class PositiveGold(BaseModel):
    model_config = ConfigDict(extra="forbid")
    required_thesis_facets: list[list[str]] = Field(min_length=1)
    required_mechanism_facets: list[list[str]] = Field(min_length=1)
    allowed_relation_kinds: list[str] = Field(min_length=1)
    expected_direction: Literal["source_to_synthesis"]
    allowed_cause_handle_sets: list[list[str]] = Field(min_length=1)
    required_support_handles: list[str] = Field(default_factory=list)
    required_counterevidence_handles: list[str] = Field(default_factory=list)
    required_alternative_facets: list[list[str]] = Field(default_factory=list)
    expected_novelty: Literal["novel", "extends", "confirms", "duplicates"]
    forbidden_handles: list[str] = Field(default_factory=list)
    confidence_band: ConfidenceBand


class NullGold(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allowed_decisions: list[Literal["abstain"]] = Field(min_length=1)
    allowed_reason_codes: list[str] = Field(min_length=1)
    required_missing_evidence_facets: list[list[str]] = Field(min_length=1)
    forbidden_handles: list[str] = Field(default_factory=list)
    maximum_synthesis_confidence: float = Field(ge=0, le=1)


class SemanticScorerCase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["think-semantic-case-v1"]
    case_id: str = Field(min_length=1, max_length=120)
    dossier_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    case_kind: Literal["positive", "null"]
    positive_gold: PositiveGold | None = None
    null_gold: NullGold | None = None

    @model_validator(mode="after")
    def exact_gold_variant(self) -> "SemanticScorerCase":
        if (self.case_kind == "positive") != (self.positive_gold is not None):
            raise ValueError("case kind and positive gold disagree")
        if (self.case_kind == "null") != (self.null_gold is not None):
            raise ValueError("case kind and null gold disagree")
        return self

    @property
    def content_digest(self) -> Digest:
        return canonical_sha256(self.model_dump(mode="json"))


class ExecutionEvidence(BaseModel):
    """Outcome facts supplied by the independent artifact reader."""
    model_config = ConfigDict(extra="forbid")
    schema_valid: bool
    handles_resolved: bool
    evidence_complete: bool
    scope_clean: bool
    compiler_accepted: bool
    unsupported_canonical_relation_count: int = Field(ge=0)
    partial_write_count: int = Field(ge=0)
    validator_applier_failure_count: int = Field(ge=0)
    compiler_receipt_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    tokens: int = Field(default=0, ge=0)
    latency_ms: float = Field(default=0, ge=0)
    cost_usd: float = Field(default=0, ge=0)
    consistency: float = Field(default=1, ge=0, le=1)


class HardGates(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_valid: bool
    handles_resolved: bool
    evidence_complete: bool
    scope_clean: bool
    relation_supported: bool
    compiler_accepted: bool
    correct_mechanism_and_direction: bool
    correct_abstention: bool
    unsupported_canonical_relations_zero: bool
    partial_writes_zero: bool
    validator_applier_failures_zero: bool


class ContinuousMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scope_precision: float = Field(ge=0, le=1)
    mechanism_correctness: float = Field(ge=0, le=1)
    thesis_completeness: float = Field(ge=0, le=1)
    causal_direction_correctness: float = Field(ge=0, le=1)
    evidence_precision: float = Field(ge=0, le=1)
    evidence_coverage: float = Field(ge=0, le=1)
    counterevidence_recognition: float = Field(ge=0, le=1)
    alternative_quality: float = Field(ge=0, le=1)
    novelty_judgment: float = Field(ge=0, le=1)
    confidence_calibration: float = Field(ge=0, le=1)
    abstention_appropriateness: float = Field(ge=0, le=1)
    schema_acceptance: float = Field(ge=0, le=1)
    compiler_acceptance: float = Field(ge=0, le=1)
    cross_scope_contamination: float = Field(ge=0, le=1)
    consistency: float = Field(ge=0, le=1)
    tokens: int = Field(ge=0)
    latency_ms: float = Field(ge=0)
    cost_usd: float = Field(ge=0)
    semantic_value_per_thousand_tokens: float = Field(ge=0)


class SemanticScorerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["think-semantic-result-v1"]
    case_id: str
    case_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    compiler_receipt_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    hard_gates: HardGates
    continuous_metrics: ContinuousMetrics
    failure_class: FailureClass | None = None
    verdict: Literal["green", "red"]
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def verify_digest(self) -> "SemanticScorerResult":
        body = self.model_dump(mode="json", exclude={"content_digest"})
        if self.content_digest != canonical_sha256(body):
            raise ValueError("semantic scorer result digest mismatch")
        return self


def score_semantic_decision(
    case: SemanticScorerCase,
    decision_artifact: Mapping[str, Any],
    *,
    decision_artifact_digest: str,
    execution: ExecutionEvidence,
) -> SemanticScorerResult:
    """Recompute one frozen decision score; fail closed on artifact tampering."""
    artifact = dict(decision_artifact)
    if canonical_sha256(artifact) != decision_artifact_digest:
        raise ValueError("decision artifact digest mismatch")
    decision = _decision(artifact)
    kind = str(decision.get("kind") or "")
    used_handles = _used_handles(decision)
    if case.case_kind == "positive":
        assert case.positive_gold is not None
        gold = case.positive_gold
        thesis = _facet_score(str(decision.get("thesis") or ""), gold.required_thesis_facets)
        mechanism = _facet_score(str(decision.get("mechanism") or ""), gold.required_mechanism_facets)
        relation = _mapping(decision.get("relation"))
        causes = set(map(str, decision.get("cause_condition_handles") or ()))
        cause_ok = any(causes == set(group) for group in gold.allowed_cause_handle_sets)
        direction = relation.get("direction") == "source_to_target"
        relation_ok = relation.get("relation_kind") in gold.allowed_relation_kinds and cause_ok
        support = set(map(str, decision.get("supporting_evidence_handles") or ()))
        counter = {str(_mapping(row).get("handle")) for row in decision.get("counterevidence") or ()}
        evidence_coverage = _set_recall(set(gold.required_support_handles), support)
        counter_score = _set_recall(set(gold.required_counterevidence_handles), counter)
        alternative = _mapping(decision.get("strongest_alternative"))
        alternative_score = _facet_score(
            " ".join(map(str, alternative.values())), gold.required_alternative_facets,
        )
        novelty_score = float(_mapping(decision.get("novelty")).get("classification") == gold.expected_novelty)
        confidence = float(decision.get("confidence") or 0)
        confidence_score = float(gold.confidence_band.minimum <= confidence <= gold.confidence_band.maximum)
        forbidden_clean = not (used_handles & set(gold.forbidden_handles))
        correct_semantics = kind == "synthesis" and thesis == mechanism == 1 and relation_ok and direction
        correct_abstention = True
        abstention_score = 1.0
    else:
        assert case.null_gold is not None
        gold = case.null_gold
        missing_text = " ".join(map(str, decision.get("missing_evidence") or ()))
        missing_score = _facet_score(missing_text, gold.required_missing_evidence_facets)
        confidence = float(decision.get("confidence") or 0)
        correct_abstention = (
            kind in gold.allowed_decisions
            and decision.get("reason_code") in gold.allowed_reason_codes
            and missing_score == 1
            and not (used_handles & set(gold.forbidden_handles))
        )
        thesis = mechanism = relation_ok = direction = evidence_coverage = counter_score = 1.0
        alternative_score = novelty_score = confidence_score = float(correct_abstention)
        forbidden_clean = not (used_handles & set(gold.forbidden_handles))
        correct_semantics = True
        abstention_score = float(correct_abstention)

    evidence_precision = float(forbidden_clean)
    hard = HardGates(
        schema_valid=execution.schema_valid, handles_resolved=execution.handles_resolved,
        evidence_complete=(execution.evidence_complete and evidence_coverage == 1
                           and counter_score == 1),
        scope_clean=execution.scope_clean and forbidden_clean,
        relation_supported=bool(relation_ok), compiler_accepted=(
            execution.compiler_accepted if kind == "synthesis" else True
        ), correct_mechanism_and_direction=bool(correct_semantics),
        correct_abstention=bool(correct_abstention),
        unsupported_canonical_relations_zero=execution.unsupported_canonical_relation_count == 0,
        partial_writes_zero=execution.partial_write_count == 0,
        validator_applier_failures_zero=execution.validator_applier_failure_count == 0,
    )
    hard_values = hard.model_dump().values()
    semantic_mean = sum(map(float, (
        thesis, mechanism, float(direction), evidence_precision, evidence_coverage,
        counter_score, alternative_score, novelty_score, confidence_score, abstention_score,
    ))) / 10
    value_per_k = semantic_mean / max(execution.tokens / 1000, 1)
    continuous = ContinuousMetrics(
        scope_precision=float(execution.scope_clean), mechanism_correctness=float(mechanism),
        thesis_completeness=float(thesis), causal_direction_correctness=float(direction),
        evidence_precision=evidence_precision, evidence_coverage=float(evidence_coverage),
        counterevidence_recognition=float(counter_score), alternative_quality=float(alternative_score),
        novelty_judgment=float(novelty_score), confidence_calibration=float(confidence_score),
        abstention_appropriateness=abstention_score,
        schema_acceptance=float(execution.schema_valid),
        compiler_acceptance=float(execution.compiler_accepted),
        cross_scope_contamination=float(not execution.scope_clean), consistency=execution.consistency,
        tokens=execution.tokens, latency_ms=execution.latency_ms, cost_usd=execution.cost_usd,
        semantic_value_per_thousand_tokens=value_per_k,
    )
    verdict: Literal["green", "red"] = "green" if all(hard_values) else "red"
    failure = None if verdict == "green" else _failure_class(hard)
    body = {
        "schema_version": "think-semantic-result-v1", "case_id": case.case_id,
        "case_digest": case.content_digest, "decision_artifact_digest": decision_artifact_digest,
        "compiler_receipt_digest": execution.compiler_receipt_digest,
        "hard_gates": hard.model_dump(mode="json"),
        "continuous_metrics": continuous.model_dump(mode="json"),
        "failure_class": failure, "verdict": verdict,
    }
    return SemanticScorerResult(**body, content_digest=canonical_sha256(body))


def _decision(artifact: Mapping[str, Any]) -> dict[str, Any]:
    # Supports the TI2 envelope and observational Arm A's already-frozen raw
    # decision wrapper without rewriting either artifact.
    value = artifact.get("decision", artifact)
    return _mapping(value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _used_handles(decision: Mapping[str, Any]) -> set[str]:
    values: list[Any] = []
    for key in ("cause_condition_handles", "effect_handles", "supporting_evidence_handles",
                "relevant_handles"):
        values.extend(decision.get(key) or ())
    values.extend(_mapping(row).get("handle") for row in decision.get("counterevidence") or ())
    values.extend(_mapping(decision.get("relation")).get("source_handles") or ())
    return {str(value) for value in values if value}


def _facet_score(text: str, facets: list[list[str]]) -> float:
    if not facets:
        return 1.0
    normalized = " ".join(text.casefold().split())
    return sum(any(term.casefold() in normalized for term in group) for group in facets) / len(facets)


def _set_recall(required: set[str], observed: set[str]) -> float:
    return 1.0 if not required else len(required & observed) / len(required)


def _failure_class(gates: HardGates) -> FailureClass:
    if not gates.schema_valid or not gates.handles_resolved:
        return "schema_binding"
    if not gates.evidence_complete or not gates.scope_clean:
        return "context_dossier"
    if not gates.compiler_accepted:
        return "compiler"
    if not gates.partial_writes_zero or not gates.validator_applier_failures_zero:
        return "validator_applier"
    return "semantic_model"


__all__ = [
    "ExecutionEvidence", "SemanticScorerCase", "SemanticScorerResult",
    "score_semantic_decision",
]
