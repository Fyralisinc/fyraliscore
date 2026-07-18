from __future__ import annotations

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from services.evaluation.epistemic_repair.cf2_decisions import (
    compiled_batch_memory_decisions,
)
from services.evaluation.epistemic_repair.cf2_provider import CF2StructuredRequest
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    _batch_candidate_lines,
    _situation_member_ids,
    _supported_synthesis_relation_result,
)


def _request(*candidates: dict) -> CF2StructuredRequest:
    lines = ["<memory_decision_candidates>"]
    for candidate in candidates:
        lines.append("  <candidate>")
        for key, value in candidate.items():
            if key == "endpoint_model_cards":
                lines.append("    endpoint_model_cards:")
                lines.extend(f"      - {json.dumps(card)}" for card in value)
            else:
                lines.append(f"    {key}: {json.dumps(value)}")
        lines.append("  </candidate>")
    lines.append("</memory_decision_candidates>")
    return CF2StructuredRequest(
        schema_name="BatchMemoryDecisionSet",
        system="compiled closed-world task",
        user="\n".join(lines),
        schema=BatchMemoryDecisionSet.model_json_schema(),
    )


def _validated(*candidates: dict) -> BatchMemoryDecisionSet:
    return BatchMemoryDecisionSet.model_validate(
        compiled_batch_memory_decisions(_request(*candidates))
    )


def test_accepts_only_grounded_closed_resolved_atomics() -> None:
    observation_id = uuid4()
    grounded = {
        "candidate_id": "atomic-grounded",
        "candidate_kind": "atomic",
        "allowed_operations": ["claim", "no_op"],
        "entailed_claim_text": "The renewal approval is complete.",
        "canonical_scope_ref": "commitment:renewal",
        "source_observation_ids": [str(observation_id)],
    }
    ungrounded = {
        **grounded,
        "candidate_id": "atomic-no-scope",
        "canonical_scope_ref": "",
    }

    result = _validated(grounded, ungrounded)

    assert result.decisions[0].decision == "accept"
    assert result.decisions[0].operation == "claim"
    assert result.decisions[0].claim_text == grounded["entailed_claim_text"]
    assert result.decisions[0].claim_local_evidence_event_ids == [observation_id]
    assert result.decisions[1].decision == "reject"
    assert result.decisions[1].operation == "no_op"


def test_exact_closed_atomic_confirms_single_bound_target() -> None:
    target_id, observation_id = uuid4(), uuid4()
    result = _validated({
        "candidate_id": "atomic-confirm",
        "candidate_kind": "atomic",
        "allowed_operations": ["memory_lifecycle"],
        "entailed_claim_text": "The renewal approval is complete.",
        "canonical_scope_ref": "commitment:renewal",
        "source_observation_ids": [str(observation_id)],
        "target_model_ids": [str(target_id)],
    })

    decision = result.decisions[0]
    assert decision.operation == "memory_lifecycle"
    assert decision.lifecycle_action == "confirm"
    assert decision.model_id == target_id


def test_admits_at_most_one_exact_evidenced_mechanistic_synthesis() -> None:
    model_ids = [uuid4(), uuid4()]
    version_ids = [uuid4(), uuid4()]
    observation_ids = [uuid4(), uuid4()]
    candidate = {
        "candidate_id": "synthesis-one",
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation", "situation_and_edge", "no_op"],
        "confidence": 0.82,
        "proposed_text": "Missing ownership blocks renewal approval.",
        "canonical_scope_ref": "commitment:renewal",
        "member_observation_ids": [str(value) for value in observation_ids],
        "evidence_model_ids": [str(value) for value in model_ids],
        "endpoint_model_cards": [
            {
                "id": str(model_id),
                "version_id": str(version_id),
                "natural": natural,
            }
            for model_id, version_id, natural in zip(
                model_ids,
                version_ids,
                (
                    "Certificate renewal remains incomplete.",
                    "The rollout window moved after the delay.",
                ),
                strict=True,
            )
        ],
    }

    result = _validated(candidate, {**candidate, "candidate_id": "synthesis-two"})

    accepted, rejected = result.decisions
    assert accepted.decision == "accept"
    assert accepted.operation == "situation_and_edge"
    assert set(accepted.situation_member_model_ids) == set(model_ids)
    assert accepted.source_model_id == model_ids[0]
    assert accepted.target_model_id == model_ids[1]
    assert rejected.decision == "reject"


def test_synthesis_membership_is_normalized_to_include_relation_endpoints() -> None:
    model_ids = [uuid4() for _ in range(4)]
    decision = BatchMemoryCandidateDecision(
        candidate_id="normalized-synthesis",
        decision="accept",
        operation="situation_and_edge",
        confidence=0.9,
        situation_member_model_ids=model_ids[:2],
        source_model_id=model_ids[2],
        target_model_id=model_ids[3],
        reason="The selected endpoints express the causal mechanism.",
    )
    candidate = {
        "candidate_kind": "synthesis",
        "evidence_model_ids": [str(value) for value in model_ids],
    }

    assert _situation_member_ids(candidate, decision) == model_ids


@pytest.mark.parametrize("endpoint_mode", ["missing", "identical"])
def test_coupled_synthesis_requires_distinct_relation_endpoints(
    endpoint_mode: str,
) -> None:
    endpoint = uuid4()
    values = {
        "source_model_id": endpoint,
        "target_model_id": endpoint if endpoint_mode == "identical" else None,
    }

    with pytest.raises(ValidationError):
        BatchMemoryCandidateDecision(
            candidate_id="invalid-coupled-synthesis",
            decision="accept",
            operation="situation_and_edge",
            confidence=0.9,
            reason="Invalid endpoint binding.",
            **values,
        )


def test_synthesis_relation_reports_exact_closed_set_failure() -> None:
    source_id, target_id, outside_id = uuid4(), uuid4(), uuid4()
    source_version_id, target_version_id = uuid4(), uuid4()
    evidence_id = uuid4()
    decision = BatchMemoryCandidateDecision(
        candidate_id="diagnostic-synthesis",
        decision="accept",
        operation="situation_and_edge",
        confidence=0.9,
        source_model_id=source_id,
        target_model_id=outside_id,
        reason="A causal relation was selected.",
    )
    candidate = {
        "candidate_kind": "synthesis",
        "evidence_model_ids": [str(source_id), str(target_id)],
        "endpoint_model_versions": {
            str(source_id): str(source_version_id),
            str(target_id): str(target_version_id),
        },
        "relation_evidence_observation_ids": [str(evidence_id)],
        "explicit_relation_obligation": {
            "edge_kind": "causes",
            "evidence_event_ids": [str(evidence_id)],
            "evidence_model_ids": [str(source_id), str(target_id)],
        },
    }

    relation, failure = _supported_synthesis_relation_result(candidate, decision)

    assert relation is None
    assert failure == "relation endpoints are outside the closed model set"


def test_compiled_parser_accepts_long_production_shaped_endpoint_cards() -> None:
    model_ids = [uuid4(), uuid4()]
    version_ids = [uuid4(), uuid4()]
    observation_ids = [uuid4(), uuid4()]
    candidate = {
        "candidate_id": "synthesis-long-endpoints",
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation", "situation_and_edge", "no_op"],
        "confidence": 0.84,
        "proposed_text": "Incomplete certificate renewal blocks the rollout gate.",
        "canonical_scope_ref": "workstream:harbor-release",
        "member_observation_ids": [str(value) for value in observation_ids],
        "evidence_model_ids": [str(value) for value in model_ids],
        "endpoint_model_cards": [
            {
                "id": str(model_ids[0]),
                "version_id": str(version_ids[0]),
                "natural": "Certificate renewal remains incomplete and is the prerequisite.",
                "proposition": {
                    "kind": "belief",
                    "assertion": "Certificate renewal remains incomplete and open.",
                    "production_metadata": "x" * 4000,
                },
                "canonical_scope": {
                    "label": "Harbor release",
                    "ref": "workstream:harbor-release",
                },
            },
            {
                "id": str(model_ids[1]),
                "version_id": str(version_ids[1]),
                "natural": "The rollout gate is blocked and the launch window is delayed.",
                "proposition": {
                    "kind": "belief",
                    "assertion": "The rollout gate is blocked and launch is delayed.",
                    "production_metadata": "y" * 4000,
                },
                "canonical_scope": {
                    "label": "Harbor release",
                    "ref": "workstream:harbor-release",
                },
            },
        ],
    }
    user = "\n".join([
        "<memory_decision_candidates>",
        *_batch_candidate_lines(candidate),
        "</memory_decision_candidates>",
    ])
    request = CF2StructuredRequest(
        schema_name="BatchMemoryDecisionSet",
        system="compiled closed-world task",
        user=user,
        schema=BatchMemoryDecisionSet.model_json_schema(),
    )

    result = BatchMemoryDecisionSet.model_validate(
        compiled_batch_memory_decisions(request)
    )

    decision = result.decisions[0]
    assert decision.decision == "accept"
    assert decision.operation == "situation_and_edge"
    assert decision.source_model_id == model_ids[0]
    assert decision.target_model_id == model_ids[1]


def test_synthesis_fails_closed_without_exact_heads_evidence_or_mechanism() -> None:
    model_ids = [uuid4(), uuid4()]
    common = {
        "candidate_kind": "synthesis",
        "allowed_operations": ["situation_and_edge", "no_op"],
        "canonical_scope_ref": "project:beacon",
        "evidence_model_ids": [str(value) for value in model_ids],
        "endpoint_model_cards": [
            {
                "id": str(value), "version_id": str(uuid4()),
                "natural": "Certificate renewal remains incomplete.",
            }
            for value in model_ids
        ],
        "member_observation_ids": [str(uuid4())],
    }
    result = _validated(
        {**common, "candidate_id": "no-mechanism", "proposed_text": "Status is mixed."},
        {
            **common,
            "candidate_id": "missing-head",
            "proposed_text": "Missing ownership blocks launch.",
            "endpoint_model_cards": common["endpoint_model_cards"][:1],
        },
        {
            **common,
            "candidate_id": "missing-evidence",
            "proposed_text": "Missing ownership blocks launch.",
            "member_observation_ids": [],
        },
    )

    assert {decision.decision for decision in result.decisions} == {"reject"}


def test_explicit_higher_authority_contradiction_supersedes_bound_head() -> None:
    model_id, observation_id = uuid4(), uuid4()
    result = _validated({
        "candidate_id": "authority-correction",
        "candidate_kind": "reconciliation",
        "allowed_operations": ["memory_lifecycle", "no_op"],
        "target_model_ids": [str(model_id)],
        "source_observation_ids": [str(observation_id)],
        "counterevidence_ids": ["counter-1"],
        "observation_evidence": {
            "text": "The official system of record contradicts and supersedes the prior status."
        },
    })

    decision = result.decisions[0]
    assert decision.decision == "accept"
    assert decision.operation == "memory_lifecycle"
    assert decision.lifecycle_action == "supersede"
    assert decision.model_id == model_id
    assert decision.claim_local_evidence_event_ids == [observation_id]


def test_generic_contradiction_without_explicit_authority_fails_closed() -> None:
    result = _validated({
        "candidate_id": "weak-correction",
        "candidate_kind": "reconciliation",
        "allowed_operations": ["memory_lifecycle", "no_op"],
        "target_model_ids": [str(uuid4())],
        "source_observation_ids": [str(uuid4())],
        "counterevidence_ids": ["counter-1"],
        "observation_evidence": {"text": "A message contradicts the prior status."},
    })

    assert result.decisions[0].decision == "reject"
    assert result.decisions[0].operation == "no_op"


def test_authoritative_reconciliation_revises_exact_active_situation() -> None:
    model_id, observation_id = uuid4(), uuid4()
    member_ids = [uuid4(), uuid4()]
    result = _validated({
        "candidate_id": "authority-revision",
        "candidate_kind": "reconciliation",
        "allowed_operations": ["memory_lifecycle", "no_op"],
        "proposed_text": (
            "Harbor release is no longer blocked after certificate renewal completed."
        ),
        "target_model_ids": [str(model_id)],
        "evidence_model_ids": [str(value) for value in member_ids],
        "member_observation_ids": [str(observation_id)],
        "counterevidence_ids": [str(observation_id)],
        "observation_evidence": [{
            "observation_id": str(observation_id),
            "trust_tier": "authoritative",
            "body": "The authoritative record contradicts the prior blocked state.",
        }],
        "reason": (
            "Authoritative scope-level evidence contradicts the prior active "
            "situation and requires revision."
        ),
    })

    decision = result.decisions[0]
    assert decision.decision == "accept"
    assert decision.operation == "memory_lifecycle"
    assert decision.lifecycle_action == "revise"
    assert decision.model_id == model_id
    assert decision.claim_role == "situation"
    assert decision.situation_member_model_ids == member_ids
    assert decision.claim_local_evidence_event_ids == [observation_id]
