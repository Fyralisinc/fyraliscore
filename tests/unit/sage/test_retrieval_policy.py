from __future__ import annotations

from uuid import uuid4

from services.platform.execution.types import RetrievalAction
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.retrieval_policy import (
    SageRouteOutcome,
    SageRouteUtility,
    adapt_inquiry_actions,
    build_signal_signature,
    plan_primary_retrieval,
    route_utilities_from_outcomes,
    signature_hash,
)


def test_primary_policy_bounds_dense_semantic_when_graph_and_sparse_are_strong():
    model_id = uuid4()
    trigger = TriggerContext(
        kind="T2",
        tenant_id=uuid4(),
        model_id=model_id,
        seed_natural_text="Enterprise SSO audit_export renewal blocker",
    )

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights={"A": 0.16, "B": 0.15, "L": 0.12, "D": 0.12, "G": 0.45},
        effective_seed_entities=[{"type": "customer", "id": "Alpen"}],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
    )

    dense = policy.decision_for("B")
    assert dense is not None
    assert dense.mode == "probe"
    assert dense.stage == 2
    assert dense.budget == 6
    assert dense.weight_multiplier == 1.0
    assert policy.allows("B") is True
    assert "B" in policy.apply_primary_weights({"B": 0.5, "G": 0.5})


def test_primary_policy_keeps_dense_semantic_early_for_vague_language():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text="Something seems weird with this customer issue",
    )

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights={"A": 0.3, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16},
        effective_seed_entities=[],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
    )

    dense = policy.decision_for("B")
    assert dense is not None
    assert dense.mode == "probe"
    assert dense.stage == 2
    assert dense.budget == 10
    assert "B" in policy.stages[-1].paths


def test_primary_policy_bounds_dense_semantic_for_vague_signal_with_cheap_anchors():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text="Something seems weird with this customer issue",
    )

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights={"A": 0.3, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16},
        effective_seed_entities=[{"type": "customer", "label": "AcmeAtlas"}],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
    )

    dense = policy.decision_for("B")
    assert dense is not None
    assert dense.mode == "probe"
    assert dense.weight_multiplier == 1.0
    assert policy.allows("B") is True
    assert dense.reason == "cheap_anchors_bound_dense_semantic_for_vague_signal"


def test_adapt_inquiry_actions_marks_stages_and_downshifts_structural_semantic_probe():
    actions = [
        RetrievalAction("Q1", "focused_index", "question_answerability_scope", budget=48),
        RetrievalAction("Q1", "structural", "goal_resource_bridge", budget=25),
        RetrievalAction("Q1", "semantic", "constraint_evidence", budget=30),
    ]

    adapted, notes = adapt_inquiry_actions(
        question_primitive="CONSTRAINT",
        actions=actions,
        semantic_budget_floor=8,
    )

    assert [action.filters["_sage_policy_stage"] for action in adapted] == [1, 1, 2]
    assert adapted[2].filters["_sage_policy_mode"] == "probe"
    assert adapted[2].budget == 18
    assert notes[2]["reason"] == "semantic_probe_after_structural_first_actions"


def test_adapt_inquiry_actions_keeps_counterevidence_temporal_in_first_stage():
    actions = [
        RetrievalAction("Q1", "semantic", "counterevidence", budget=30),
        RetrievalAction("Q1", "temporal", "recent_counterevidence", budget=30),
    ]

    adapted, _notes = adapt_inquiry_actions(
        question_primitive="COUNTEREVIDENCE",
        actions=actions,
    )

    assert adapted[0].filters["_sage_policy_stage"] == 2
    assert adapted[1].filters["_sage_policy_stage"] == 1


def test_route_outcomes_compress_into_high_utility_memory():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text="Audit export renewal blocker",
    )
    signature = build_signal_signature(
        trigger=trigger,
        question_primitive="DEPENDENCY",
        projection_enabled=False,
    )

    utilities = route_utilities_from_outcomes(
        signature,
        [
            SageRouteOutcome(
                path="semantic",
                elapsed_ms=35,
                returned_models=12,
                selected_evidence=2,
                budget=18,
                quality_credit=1.4,
                cost_units=0.3,
            )
            for _ in range(5)
        ],
    )

    semantic = utilities[0]
    assert semantic.signature_hash == signature_hash(signature)
    assert semantic.path == "semantic"
    assert semantic.attempts == 5
    assert semantic.wins == 5
    assert semantic.utility_score > 0.55
    assert semantic.confidence > 0.5


def test_primary_policy_uses_positive_route_utility_to_promote_dense_semantic():
    trigger = TriggerContext(
        kind="T2",
        tenant_id=uuid4(),
        model_id=uuid4(),
        seed_natural_text="Enterprise SSO audit_export renewal blocker",
    )
    signature = build_signal_signature(
        trigger=trigger,
        effective_seed_entities=[{"type": "customer", "id": "Alpen"}],
        effective_scope_actors=[],
        projection_enabled=True,
    )
    utility = SageRouteUtility(
        signature_hash=signature_hash(signature),
        path="B",
        signal_type="T2",
        attempts=9,
        wins=7,
        returned_models=90,
        selected_evidence=10,
        elapsed_ms_total=420,
        latency_ms_p95=70,
        budget_total=90,
        total_quality_credit=8.0,
        utility_score=0.72,
        confidence=0.82,
    )

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights={"A": 0.16, "B": 0.15, "L": 0.12, "D": 0.12, "G": 0.45},
        effective_seed_entities=[{"type": "customer", "id": "Alpen"}],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
        route_utilities=(utility,),
    )

    dense = policy.decision_for("B")
    assert dense is not None
    assert dense.mode == "preferred"
    assert dense.weight_multiplier >= 1.0
    assert policy.allows("B") is True
    assert "B" in policy.apply_primary_weights({"B": 0.5, "G": 0.5})
    assert "positive_route_utility_promoted_path" in policy.reasons


def test_primary_policy_uses_negative_route_utility_to_skip_costly_temporal():
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text="Launch dependency status",
    )
    signature = build_signal_signature(trigger=trigger, projection_enabled=True)
    utility = SageRouteUtility(
        signature_hash=signature_hash(signature),
        path="C",
        signal_type="T1",
        attempts=6,
        wins=0,
        returned_models=0,
        returned_observations=0,
        elapsed_ms_total=7200,
        latency_ms_p95=1300,
        budget_total=120,
        total_cost=5.0,
        total_quality_credit=-0.6,
        utility_score=-0.74,
        confidence=0.55,
    )

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights={"A": 0.3, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16},
        effective_seed_entities=[],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
        route_utilities=(utility,),
    )

    temporal = policy.decision_for("C")
    assert temporal is not None
    assert temporal.mode == "skip"
    assert policy.allows("C") is False
    assert "negative_route_utility_suppressed_path" in policy.reasons


def test_adapt_inquiry_actions_uses_negative_route_utility_for_admission_skip():
    signature = build_signal_signature(
        trigger=TriggerContext(
            kind="T1",
            tenant_id=uuid4(),
            seed_natural_text="Launch dependency status",
        ),
        question_primitive="DEPENDENCY",
        projection_enabled=False,
    )
    utility = SageRouteUtility(
        signature_hash=signature_hash(signature),
        path="semantic",
        signal_type="T1",
        question_primitive="DEPENDENCY",
        attempts=5,
        wins=0,
        elapsed_ms_total=5000,
        latency_ms_p95=1200,
        budget_total=150,
        total_cost=4.0,
        total_quality_credit=-0.5,
        utility_score=-0.62,
        confidence=0.5,
    )

    adapted, notes = adapt_inquiry_actions(
        question_primitive="DEPENDENCY",
        signal_type="T1",
        route_utilities=(utility,),
        actions=[
            RetrievalAction("Q1", "structural", "goal_resource_bridge", budget=25),
            RetrievalAction("Q1", "semantic", "constraint_evidence", budget=30),
        ],
    )

    assert adapted[1].filters["_sage_policy_mode"] == "skip"
    assert adapted[1].filters["_sage_route_utility_skip"] is True
    assert adapted[1].budget == 6
    assert notes[1]["route_utility"]["utility_score"] == -0.62
