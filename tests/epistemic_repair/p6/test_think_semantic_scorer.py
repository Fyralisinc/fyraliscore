from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from services.evaluation.epistemic_repair.think_policy_receipts import (
    EvaluationReceipt,
    PolicyCheckpoint,
    PolicyIdentity,
    build_evaluation_receipt,
    deterministic_rollback,
    replay_evaluation_receipt,
)
from services.evaluation.epistemic_repair.think_semantic_scorer import (
    ExecutionEvidence,
    SemanticScorerCase,
    SemanticScorerResult,
    score_semantic_decision,
)


DIGEST = "a" * 64


def _positive_case() -> SemanticScorerCase:
    return SemanticScorerCase.model_validate({
        "schema_version": "think-semantic-case-v1", "case_id": "generic-positive",
        "dossier_digest": DIGEST, "case_kind": "positive", "positive_gold": {
            "required_thesis_facets": [["ownership"], ["rollout", "release"]],
            "required_mechanism_facets": [["certificate"], ["delay", "blocked"]],
            "allowed_relation_kinds": ["causes"], "expected_direction": "source_to_synthesis",
            "allowed_cause_handle_sets": [["M1"]], "required_support_handles": ["O1"],
            "required_counterevidence_handles": ["O3"],
            "required_alternative_facets": [["capacity"]], "expected_novelty": "novel",
            "forbidden_handles": ["O9"], "confidence_band": {"minimum": .7, "maximum": .9},
        },
    })


def _positive_decision() -> dict:
    return {"schema_version": "think-synthesis-decision-v1", "dossier_digest": DIGEST,
        "decision": {"kind": "synthesis",
            "thesis": "Certificate ownership affected rollout timing.",
            "mechanism": "Missing certificate ownership delayed the release gate.",
            "cause_condition_handles": ["M1"], "effect_handles": ["M2"],
            "supporting_evidence_handles": ["O1"],
            "counterevidence": [{"handle": "O3", "bearing": "weakens"}],
            "strongest_alternative": {"thesis": "Capacity delayed rollout.",
                                      "why_weaker": "No evidence."},
            "novelty": {"classification": "novel"}, "confidence": .8,
            "relation": {"relation_kind": "causes", "source_handles": ["M1"],
                         "target": "synthesis_output", "direction": "source_to_target"}}}


def _execution(**changes) -> ExecutionEvidence:
    values = {"schema_valid": True, "handles_resolved": True, "evidence_complete": True,
              "scope_clean": True, "compiler_accepted": True,
              "unsupported_canonical_relation_count": 0, "partial_write_count": 0,
              "validator_applier_failure_count": 0, "compiler_receipt_digest": "b" * 64,
              "tokens": 2000, "latency_ms": 300, "cost_usd": .01, "consistency": .9}
    return ExecutionEvidence(**{**values, **changes})


def test_positive_case_scores_all_hard_and_continuous_metrics() -> None:
    artifact = _positive_decision()
    result = score_semantic_decision(
        _positive_case(), artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(),
    )
    assert result.verdict == "green"
    assert all(result.hard_gates.model_dump().values())
    assert result.continuous_metrics.mechanism_correctness == 1
    assert result.continuous_metrics.semantic_value_per_thousand_tokens > 0
    assert result.failure_class is None


def test_null_case_requires_abstention_and_missing_evidence() -> None:
    case = SemanticScorerCase.model_validate({
        "schema_version": "think-semantic-case-v1", "case_id": "generic-null",
        "dossier_digest": DIGEST, "case_kind": "null", "null_gold": {
            "allowed_decisions": ["abstain"], "allowed_reason_codes": ["insufficient_evidence"],
            "required_missing_evidence_facets": [["independent"], ["timeline"]],
            "forbidden_handles": ["O9"], "maximum_synthesis_confidence": .4}})
    artifact = {"kind": "abstain", "reason_code": "insufficient_evidence",
                "missing_evidence": ["Independent ownership timeline"],
                "relevant_handles": ["O1"], "confidence": .8}
    result = score_semantic_decision(
        case, artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(compiler_accepted=False, compiler_receipt_digest=None),
    )
    assert result.verdict == "green"
    assert result.hard_gates.correct_abstention is True


def test_semantic_failure_is_noncompensatory_and_classified() -> None:
    artifact = _positive_decision()
    artifact["decision"]["relation"]["direction"] = "target_to_source"
    result = score_semantic_decision(
        _positive_case(), artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(),
    )
    assert result.verdict == "red"
    assert result.failure_class == "semantic_model"
    assert result.hard_gates.correct_mechanism_and_direction is False


def test_decision_and_result_tampering_fail_closed() -> None:
    artifact = _positive_decision()
    with pytest.raises(ValueError, match="artifact digest mismatch"):
        score_semantic_decision(
            _positive_case(), artifact, decision_artifact_digest="0" * 64,
            execution=_execution(),
        )
    result = score_semantic_decision(
        _positive_case(), artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(),
    )
    body = result.model_dump(mode="json")
    body["verdict"] = "red"
    with pytest.raises(ValidationError, match="result digest mismatch"):
        SemanticScorerResult.model_validate(body)


def test_arm_a_wrapper_is_ingested_without_rewriting_artifact() -> None:
    artifact = _positive_decision()
    frozen = dict(artifact)
    score_semantic_decision(
        _positive_case(), artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(),
    )
    assert artifact == frozen


def test_receipt_replay_tamper_detection_and_deterministic_rollback() -> None:
    artifact = _positive_decision()
    result = score_semantic_decision(
        _positive_case(), artifact, decision_artifact_digest=canonical_sha256(artifact),
        execution=_execution(),
    )
    current = PolicyIdentity(prompt_policy_version="p2", provider_schema_version="s1",
        compiler_version="c1", routing_policy_version="r1", model="model-a", effort="high")
    previous = current.model_copy(update={"prompt_policy_version": "p1", "effort": "medium"})
    compiler = {"accepted": True}
    receipt = build_evaluation_receipt(
        attempt_id="attempt-1", dossier_digest=DIGEST, policy=current,
        raw_decision_digest=canonical_sha256(artifact), scorer_result=result,
        compiler_receipt_digest=canonical_sha256(compiler),
        evaluated_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
    )
    assert replay_evaluation_receipt(
        receipt, raw_decision=artifact, scorer_result=result, compiler_receipt=compiler,
    )
    assert not replay_evaluation_receipt(
        receipt, raw_decision={**artifact, "tampered": True},
        scorer_result=result, compiler_receipt=compiler,
    )
    body = receipt.model_dump(mode="json")
    body["policy_digest"] = "0" * 64
    with pytest.raises(ValidationError, match="policy identity digest mismatch"):
        EvaluationReceipt.model_validate(body)
    checkpoints = [
        PolicyCheckpoint(sequence=1, policy=previous, promoted=True,
                         evaluation_result_digest="1" * 64),
        PolicyCheckpoint(sequence=2, policy=current, promoted=True,
                         evaluation_result_digest=result.content_digest),
    ]
    assert deterministic_rollback(
        checkpoints, current_policy_digest=current.content_digest,
    ) == previous
