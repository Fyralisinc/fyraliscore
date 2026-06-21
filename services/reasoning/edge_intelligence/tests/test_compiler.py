from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence import (
    EdgeCompilerConfig,
    ModelPairEvidence,
    compile_pair_evidence_candidate,
    confidence_from_pair_evidence,
)


def _pair(**overrides) -> ModelPairEvidence:
    model_a_id = overrides.pop("model_a_id", uuid7())
    model_b_id = overrides.pop("model_b_id", uuid7())
    primitive = overrides.pop("primitive", "DEPENDENCY")
    return ModelPairEvidence(
        id=uuid7(),
        tenant_id=uuid7(),
        model_a_id=model_a_id,
        model_b_id=model_b_id,
        primitive=primitive,
        **overrides,
    )


def test_retrieval_only_pair_does_not_promote() -> None:
    evidence = _pair(
        co_retrieved_count=12,
        direction_votes={"a_to_b": 8},
        edge_kind_votes={"blocks": 7},
    )

    assert confidence_from_pair_evidence(evidence) > 0.0
    assert compile_pair_evidence_candidate(evidence) is None


def test_explicit_relation_and_valid_use_promotes_directed_candidate() -> None:
    model_a_id = uuid7()
    model_b_id = uuid7()
    evidence = _pair(
        model_a_id=model_a_id,
        model_b_id=model_b_id,
        co_retrieved_count=2,
        co_used_valid_diff_count=1,
        explicit_relation_count=1,
        direction_votes={"a_to_b": 2},
        edge_kind_votes={"blocks": 2},
    )

    candidate = compile_pair_evidence_candidate(evidence)

    assert candidate is not None
    assert candidate.source_model_id == model_a_id
    assert candidate.target_model_id == model_b_id
    assert candidate.edge_kind == "blocks"
    assert candidate.source == "edge_intelligence_kernel"
    assert candidate.metadata["edge_intelligence"]["primitive"] == "DEPENDENCY"


def test_t4_rejection_suppresses_without_acceptance() -> None:
    evidence = _pair(
        explicit_relation_count=2,
        co_used_valid_diff_count=2,
        t4_reject_count=1,
        direction_votes={"a_to_b": 2},
        edge_kind_votes={"blocks": 2},
    )

    assert compile_pair_evidence_candidate(evidence) is None


def test_t4_acceptance_can_overcome_prior_rejection() -> None:
    evidence = _pair(
        explicit_relation_count=1,
        co_used_valid_diff_count=1,
        t4_accept_count=1,
        t4_reject_count=1,
        direction_votes={"a_to_b": 2},
        edge_kind_votes={"blocks": 2},
    )

    candidate = compile_pair_evidence_candidate(
        evidence,
        config=EdgeCompilerConfig(min_confidence=0.5),
    )

    assert candidate is not None
    assert candidate.basis == "causal_confirmed"
    assert "mechanism_summary" in candidate.metadata["causal"]


def test_directed_kind_requires_direction_vote() -> None:
    evidence = _pair(
        explicit_relation_count=2,
        co_used_valid_diff_count=1,
        edge_kind_votes={"blocks": 2},
    )

    assert compile_pair_evidence_candidate(evidence) is None


def test_symmetric_kind_does_not_require_direction_vote() -> None:
    evidence = _pair(
        primitive="RECURRENCE",
        explicit_relation_count=2,
        co_used_valid_diff_count=1,
        edge_kind_votes={"same_issue_as": 2},
    )

    candidate = compile_pair_evidence_candidate(evidence)

    assert candidate is not None
    assert candidate.edge_kind == "same_issue_as"
