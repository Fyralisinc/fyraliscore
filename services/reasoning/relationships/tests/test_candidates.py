from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.reasoning.relationships.candidates import (
    JudgmentScores,
    ModelSignal,
    candidate_lifecycle_metadata,
    candidate_rules,
    generate_scope_overlap_candidates,
    make_edge_candidate,
    make_edge_type_candidate,
    make_situation_candidate,
    make_topology_candidate_metadata,
    rank_candidates,
)


# ---------------------------------------------------------------------
# Constructor invariants (unchanged contract).
# ---------------------------------------------------------------------


def test_situation_candidate_carries_first_class_proposition() -> None:
    tenant_id = uuid7()
    m1 = uuid7()
    m2 = uuid7()

    candidate = make_situation_candidate(
        tenant_id=tenant_id,
        situation="Beacon renewal risk is becoming cross-functional",
        summary="Delivery delay and pricing pressure are compounding.",
        relationship_summary="The delay increases the impact of pricing concern.",
        member_model_ids=(m1, m2, m1),
        basis="inferred",
        scores=JudgmentScores(
            impact=0.9,
            uncertainty=0.6,
            urgency=0.7,
            authority_required=0.8,
            actionability=0.6,
            novelty=0.5,
            confidence=0.7,
        ),
    )

    assert candidate.candidate_kind == "situation"
    assert candidate.member_model_ids == (m1, m2)
    assert candidate.proposed_proposition is not None
    assert candidate.proposed_proposition["kind"] == "belief"
    assert candidate.proposed_proposition["claim_role"] == "situation"
    assert candidate.proposed_proposition["member_model_ids"] == [
        str(m1),
        str(m2),
    ]
    assert candidate.proposed_proposition["pressure_type"] == "execution"
    assert candidate.proposed_proposition["shared_mechanism"] == (
        "The delay increases the impact of pricing concern."
    )
    assert candidate.proposed_proposition["judgment_change"]
    assert candidate.proposed_proposition["open_falsifier"]
    assert candidate.judgment_leverage_score > 0.6


def test_edge_candidate_rejects_self_edge() -> None:
    model_id = uuid7()

    with pytest.raises(ValueError):
        make_edge_candidate(
            tenant_id=uuid7(),
            source_model_id=model_id,
            target_model_id=model_id,
            edge_kind="supports",
            basis="inferred",
            explanation="bad self-edge",
            scores=JudgmentScores(),
        )


def test_edge_type_candidate_carries_ontology_gap_payload() -> None:
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    candidate = make_edge_type_candidate(
        tenant_id=tenant_id,
        proposed_edge_kind="gated_by_decision",
        description="Progress depends on an explicit decision gate.",
        relationship_summary=(
            "A model cannot progress until a specific decision is made."
        ),
        parent_kind="blocks",
        nearest_existing_kind="blocks",
        directionality="directed",
        dropped_dimensions=(
            "authority surface",
            "decision dependency",
            "approval state",
        ),
        example_source_model_id=source_model_id,
        example_target_model_id=target_model_id,
        scores=JudgmentScores(
            impact=0.9,
            uncertainty=0.7,
            urgency=0.8,
            authority_required=0.9,
            actionability=0.8,
            novelty=0.8,
            confidence=0.6,
        ),
    )

    assert candidate.candidate_kind == "edge_type"
    assert candidate.basis == "ontology_gap"
    assert candidate.source_model_id is None
    assert candidate.target_model_id is None
    assert candidate.edge_kind is None
    assert candidate.member_model_ids == (source_model_id, target_model_id)
    assert candidate.proposed_proposition is not None
    assert candidate.proposed_proposition["kind"] == "ontology_gap"
    assert candidate.proposed_proposition["proposed_edge_kind"] == "gated_by_decision"
    assert candidate.proposed_proposition["parent_kind"] == "blocks"
    assert candidate.metadata["ontology_gap"]["retrieval_fallback_kind"] == "blocks"
    assert candidate.review_status == "needs_review"


def test_candidate_record_carries_single_lifecycle_metadata() -> None:
    candidate = make_edge_candidate(
        tenant_id=uuid7(),
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="supports",
        basis="topology_suggested",
        explanation="Topology surfaced a possible support relation.",
        scores=JudgmentScores(impact=0.7, confidence=0.7),
        source="latent_topology",
        metadata=make_topology_candidate_metadata(
            proposal_kind="edge",
            pattern_kind="pair",
            selection_sources=("latent", "surface"),
            score_components={"total": 0.72},
            impact_signatures=(),
        ),
    )

    metadata = candidate.to_record()["metadata"]

    assert metadata["candidate_lifecycle"] == {
        "stage": "memory_proposal",
        "origin": "latent_topology",
        "origin_stage": "pattern_discovery",
        "proposal_kind": "edge",
        "discovery_pattern_kind": "pair",
    }
    assert metadata["topology"]["object_type"] == "pair_candidate"


def test_candidate_lifecycle_metadata_defaults_direct_proposals() -> None:
    metadata = candidate_lifecycle_metadata(
        None,
        candidate_kind="situation",
        source="relationship_candidate_service",
    )

    assert metadata["candidate_lifecycle"] == {
        "stage": "memory_proposal",
        "proposal_kind": "situation",
        "origin": "relationship_candidate_service",
        "origin_stage": "direct_proposal",
    }


def test_edge_type_candidate_rejects_partial_example_pair() -> None:
    with pytest.raises(ValueError):
        make_edge_type_candidate(
            tenant_id=uuid7(),
            proposed_edge_kind="accountable_for",
            description="Accountability proposal.",
            relationship_summary="Only one endpoint was supplied.",
            example_source_model_id=uuid7(),
            scores=JudgmentScores(),
        )


def test_causal_candidate_requires_mechanism() -> None:
    with pytest.raises(ValueError):
        make_edge_candidate(
            tenant_id=uuid7(),
            source_model_id=uuid7(),
            target_model_id=uuid7(),
            edge_kind="blocks",
            basis="causal_hypothesis",
            explanation="causal but underspecified",
            scores=JudgmentScores(),
        )


def test_rank_candidates_orders_by_judgment_leverage() -> None:
    tenant_id = uuid7()
    low = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="co_occurs_with",
        basis="inferred",
        explanation="low leverage",
        scores=JudgmentScores(impact=0.2, confidence=0.9),
    )
    high = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        basis="inferred",
        explanation="high leverage",
        scores=JudgmentScores(
            impact=0.9,
            urgency=0.8,
            actionability=0.8,
            authority_required=0.7,
            uncertainty=0.6,
            confidence=0.6,
        ),
    )

    assert rank_candidates([low, high]) == [high, low]


# ---------------------------------------------------------------------
# Per-edge-kind rule registry. One positive + one negative case each.
# ---------------------------------------------------------------------


def _emb(seed: float) -> tuple[float, ...]:
    return tuple([seed] + [0.0] * 7)


def _aligned(strength: float = 1.0) -> tuple[float, ...]:
    return tuple([strength] + [0.0] * 7)


def _scope(*entries: tuple[str, object]) -> tuple[tuple[str, object], ...]:
    return tuple(entries)


def _scope_meta() -> dict:
    return {"scope_type": "test", "scope_id": "fixed"}


def _rule(kind: str):
    return candidate_rules()[kind]


def test_same_issue_as_fires_on_high_cosine_same_entity_same_workstream() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    e = _aligned()
    left = ModelSignal(
        id=uuid7(),
        natural="Pricing on Beacon renewal is being renegotiated",
        proposition_kind="concern",
        embedding=e,
        workstream="renewal",
        scope_entities=_scope(("customer", customer)),
        confidence=0.7,
        activation=0.6,
    )
    right = ModelSignal(
        id=uuid7(),
        natural="Beacon renewal pricing renegotiation is in flight",
        proposition_kind="concern",
        embedding=e,
        workstream="renewal",
        scope_entities=_scope(("customer", customer)),
        confidence=0.7,
        activation=0.5,
    )
    result = _rule("same_issue_as")(tenant_id, _scope_meta(), left, right)
    assert result is not None
    assert result.edge_kind == "same_issue_as"


def test_same_issue_as_rejects_when_workstream_differs() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    e = _aligned()
    left = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="concern",
        embedding=e,
        workstream="renewal",
        scope_entities=_scope(("customer", customer)),
    )
    right = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="concern",
        embedding=e,
        workstream="delivery",
        scope_entities=_scope(("customer", customer)),
    )
    result = _rule("same_issue_as")(tenant_id, _scope_meta(), left, right)
    assert result is None


def test_supports_fires_on_shared_evidence_event() -> None:
    tenant_id = uuid7()
    ev = uuid7()
    left = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="state",
        confidence=0.6,
        activation=0.4,
        evidence_event_ids=(ev,),
    )
    right = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="state",
        confidence=0.6,
        activation=0.5,
        evidence_event_ids=(ev,),
    )
    result = _rule("supports")(tenant_id, _scope_meta(), left, right)
    assert result is not None
    assert result.edge_kind == "supports"
    assert result.evidence_event_ids == (ev,)


def test_supports_rejects_without_evidence_or_activation_pattern() -> None:
    tenant_id = uuid7()
    left = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="state",
        confidence=0.4,
        activation=0.3,
    )
    right = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="state",
        confidence=0.4,
        activation=0.3,
    )
    result = _rule("supports")(tenant_id, _scope_meta(), left, right)
    assert result is None


def test_analogous_to_fires_on_high_cosine_different_scope() -> None:
    tenant_id = uuid7()
    e = _aligned()
    left = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="pattern",
        embedding=e,
        workstream="alpha",
        scope_entities=_scope(("customer", uuid7())),
    )
    right = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="pattern",
        embedding=e,
        workstream="bravo",
        scope_entities=_scope(("customer", uuid7())),
    )
    result = _rule("analogous_to")(tenant_id, _scope_meta(), left, right)
    assert result is not None
    assert result.edge_kind == "analogous_to"


def test_analogous_to_rejects_when_scope_is_shared() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    e = _aligned()
    left = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="pattern",
        embedding=e,
        workstream="alpha",
        scope_entities=_scope(("customer", customer)),
    )
    right = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="pattern",
        embedding=e,
        workstream="alpha",
        scope_entities=_scope(("customer", customer)),
    )
    result = _rule("analogous_to")(tenant_id, _scope_meta(), left, right)
    assert result is None


def test_blocks_fires_on_explicit_blocker_target() -> None:
    tenant_id = uuid7()
    target_id = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="Launch is blocked by security review",
        proposition_kind="concern",
        confidence=0.8,
        activation=0.8,
        blocker_targets=(target_id,),
    )
    target = ModelSignal(
        id=target_id,
        natural="Security review for the launch",
        proposition_kind="state",
        confidence=0.7,
        activation=0.5,
    )
    result = _rule("blocks")(tenant_id, _scope_meta(), source, target)
    assert result is not None
    assert result.edge_kind == "blocks"
    assert result.source_model_id == source.id
    assert result.target_model_id == target.id
    assert result.metadata.get("mechanism")
    assert result.metadata.get("dependency_basis")


def test_blocks_rejects_pure_pressure_overlap() -> None:
    tenant_id = uuid7()
    left = ModelSignal(
        id=uuid7(),
        natural="The team is overloaded right now",
        proposition_kind="concern",
        confidence=0.7,
        activation=0.8,
    )
    right = ModelSignal(
        id=uuid7(),
        natural="Bandwidth is constrained across squads",
        proposition_kind="concern",
        confidence=0.7,
        activation=0.7,
    )
    result = _rule("blocks")(tenant_id, _scope_meta(), left, right)
    assert result is None


def test_early_warning_for_fires_on_leading_indicator_with_history() -> None:
    tenant_id = uuid7()
    target_id = uuid7()
    leading = ModelSignal(
        id=uuid7(),
        natural="Early-stage usage drop",
        proposition_kind="prediction",
        confidence=0.7,
        activation=0.7,
        time_shape="leading",
        historical_cooccurrence_with=(target_id,),
    )
    target = ModelSignal(
        id=target_id,
        natural="Churn outcome",
        proposition_kind="prediction",
        confidence=0.6,
        activation=0.6,
        time_shape="bounded",
    )
    result = _rule("early_warning_for")(tenant_id, _scope_meta(), leading, target)
    assert result is not None
    assert result.edge_kind == "early_warning_for"
    assert result.metadata.get("lead_time_evidence")


def test_early_warning_for_rejects_without_leading_time_shape() -> None:
    tenant_id = uuid7()
    a = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="state",
        time_shape="unspecified",
    )
    b = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="state",
        time_shape="unspecified",
    )
    result = _rule("early_warning_for")(tenant_id, _scope_meta(), a, b)
    assert result is None


def test_contradicts_fires_on_opposing_polarity_same_scope() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    a = ModelSignal(
        id=uuid7(),
        natural="Renewal is on track",
        proposition_kind="state",
        polarity="positive",
        scope_entities=_scope(("customer", customer)),
    )
    b = ModelSignal(
        id=uuid7(),
        natural="Renewal is at risk",
        proposition_kind="state",
        polarity="negative",
        scope_entities=_scope(("customer", customer)),
    )
    result = _rule("contradicts")(tenant_id, _scope_meta(), a, b)
    assert result is not None
    assert result.edge_kind == "contradicts"


def test_contradicts_rejects_when_polarities_match() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    a = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="state",
        polarity="positive",
        scope_entities=_scope(("customer", customer)),
    )
    b = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="state",
        polarity="positive",
        scope_entities=_scope(("customer", customer)),
    )
    result = _rule("contradicts")(tenant_id, _scope_meta(), a, b)
    assert result is None


def test_weakens_fires_on_partial_counterevidence_same_scope() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="New sponsor evidence weakens the renewal-on-track claim",
        proposition_kind="belief",
        polarity="negative",
        confidence=0.82,
        scope_entities=_scope(("customer", customer)),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Renewal is on track",
        proposition_kind="belief",
        polarity="positive",
        confidence=0.62,
        scope_entities=_scope(("customer", customer)),
    )

    result = _rule("weakens")(tenant_id, _scope_meta(), source, target)

    assert result is not None
    assert result.edge_kind == "weakens"
    assert result.source_model_id == source.id
    assert result.target_model_id == target.id


def test_explains_fires_on_explanatory_phrasing_same_scope() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="The implementation slipped because security review ownership changed",
        proposition_kind="belief",
        confidence=0.76,
        scope_entities=_scope(("customer", customer)),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Implementation slipped this week",
        proposition_kind="state",
        confidence=0.7,
        scope_entities=_scope(("customer", customer)),
    )

    result = _rule("explains")(tenant_id, _scope_meta(), source, target)

    assert result is not None
    assert result.edge_kind == "explains"
    assert result.metadata.get("mechanism")


def test_causes_fires_on_causal_phrasing_same_scope() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="The missing data residency signoff causes procurement delay",
        proposition_kind="belief",
        confidence=0.78,
        scope_entities=_scope(("customer", customer)),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Procurement delay is active",
        proposition_kind="state",
        confidence=0.7,
        scope_entities=_scope(("customer", customer)),
    )

    result = _rule("causes")(tenant_id, _scope_meta(), source, target)

    assert result is not None
    assert result.edge_kind == "causes"
    assert result.metadata.get("mechanism")


def test_contributes_to_resolution_fires_on_resolution_evidence() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="Audit evidence is now available and unblocked the exception",
        proposition_kind="state",
        confidence=0.8,
        scope_entities=_scope(("customer", customer)),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Procurement is blocked waiting on the audit exception",
        proposition_kind="concern",
        confidence=0.7,
        scope_entities=_scope(("customer", customer)),
    )

    result = _rule("contributes_to_resolution")(tenant_id, _scope_meta(), source, target)

    assert result is not None
    assert result.edge_kind == "contributes_to_resolution"
    assert result.source_model_id == source.id
    assert result.target_model_id == target.id


def test_predicts_fires_from_prediction_claim_to_outcome_scope() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="Prediction: the renewal will slip by Friday",
        proposition_kind="prediction",
        confidence=0.68,
        time_shape="future",
        proposition={"kind": "prediction", "claim_role": "prediction"},
        scope_entities=_scope(("customer", customer)),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Renewal slipped on Friday",
        proposition_kind="state",
        confidence=0.74,
        scope_entities=_scope(("customer", customer)),
    )

    result = _rule("predicts")(tenant_id, _scope_meta(), source, target)

    assert result is not None
    assert result.edge_kind == "predicts"
    assert result.source_model_id == source.id
    assert result.target_model_id == target.id


def test_enables_fires_on_capability_surface_and_capability_assessment() -> None:
    tenant_id = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="New onboarding playbook is live",
        proposition_kind="state",
        capability_surface="onboarding_playbook",
    )
    target = ModelSignal(
        id=uuid7(),
        natural="Customer team can run onboarding without engineering",
        proposition_kind="capability_assessment",
    )
    result = _rule("enables")(tenant_id, _scope_meta(), source, target)
    assert result is not None
    assert result.edge_kind == "enables"
    assert result.source_model_id == source.id
    assert result.target_model_id == target.id
    assert result.metadata.get("mechanism")


def test_enables_rejects_without_capability_assessment_target() -> None:
    tenant_id = uuid7()
    source = ModelSignal(
        id=uuid7(),
        natural="A",
        proposition_kind="state",
        capability_surface="onboarding_playbook",
    )
    target = ModelSignal(
        id=uuid7(),
        natural="B",
        proposition_kind="concern",
    )
    result = _rule("enables")(tenant_id, _scope_meta(), source, target)
    assert result is None


# ---------------------------------------------------------------------
# Loop-level: pairs that fail every rule produce NO candidates.
# ---------------------------------------------------------------------


def test_generate_scope_overlap_yields_no_candidates_without_signals() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    a = ModelSignal(
        id=uuid7(),
        natural="Pricing is being reviewed",
        proposition_kind="concern",
        scope_entities=_scope(("customer", customer)),
    )
    b = ModelSignal(
        id=uuid7(),
        natural="Roadmap meeting moved to next week",
        proposition_kind="state",
        scope_entities=_scope(("customer", customer)),
    )

    out = generate_scope_overlap_candidates(tenant_id=tenant_id, models=[a, b])
    assert out == []


def test_generate_scope_overlap_runs_blocks_rule_in_pipeline() -> None:
    tenant_id = uuid7()
    commitment = uuid7()
    target_id = uuid7()
    blocker = ModelSignal(
        id=uuid7(),
        natural="Launch is blocked on security review",
        proposition_kind="concern",
        confidence=0.8,
        activation=0.9,
        scope_entities=_scope(("commitment", commitment)),
        blocker_targets=(target_id,),
    )
    target = ModelSignal(
        id=target_id,
        natural="Security review for the launch",
        proposition_kind="state",
        confidence=0.7,
        activation=0.5,
        scope_entities=_scope(("commitment", commitment)),
    )

    out = generate_scope_overlap_candidates(
        tenant_id=tenant_id, models=[blocker, target]
    )

    kinds = {c.edge_kind for c in out}
    assert "blocks" in kinds
    blocks_candidate = next(c for c in out if c.edge_kind == "blocks")
    assert blocks_candidate.metadata.get("dependency_basis")
    assert blocks_candidate.metadata.get("mechanism")
    assert blocks_candidate.metadata["scope"] == {
        "type": "commitment",
        "id": str(commitment),
    }


def test_generate_scope_overlap_runs_precise_resolution_rule_in_pipeline() -> None:
    tenant_id = uuid7()
    customer = uuid7()
    resolution = ModelSignal(
        id=uuid7(),
        natural="Compliance artifact approved and unblocked the exception",
        proposition_kind="state",
        confidence=0.82,
        activation=0.8,
        scope_entities=_scope(("customer", customer)),
    )
    pressure = ModelSignal(
        id=uuid7(),
        natural="Renewal is blocked by the compliance exception",
        proposition_kind="concern",
        confidence=0.72,
        activation=0.7,
        scope_entities=_scope(("customer", customer)),
    )

    out = generate_scope_overlap_candidates(
        tenant_id=tenant_id,
        models=[resolution, pressure],
    )

    assert any(c.edge_kind == "contributes_to_resolution" for c in out)
