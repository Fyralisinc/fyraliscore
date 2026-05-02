"""Validator tests against a tiny hand-crafted bundle."""
from __future__ import annotations

from demo.generation.schemas import (
    EntityMention, GeneratedActor, GeneratedBundle, GeneratedCommitment,
    GeneratedCustomer, GeneratedDecision, GeneratedGoal,
    GeneratedRecommendation, GeneratedSignal, TargetActRef,
)
from demo.generation.validate import validate_bundle


def _tiny_bundle() -> GeneratedBundle:
    return GeneratedBundle(
        company_id="test", ceo_actor_id="a-ceo",
        actors=[
            GeneratedActor(id="a-ceo", name="CEO", role="founder"),
            GeneratedActor(id="a-eng", name="Eng", role="engineer", manager_id="a-ceo"),
        ],
        customers=[
            GeneratedCustomer(id="c1", company_name="X", arr_usd=100.0,
                              segment="enterprise", current_health="healthy"),
        ],
        goals=[
            GeneratedGoal(id="g1", title="G1", owner_id="a-ceo", altitude="strategic"),
            GeneratedGoal(id="g2", title="G2", owner_id="a-eng",
                          parent_goal_id="g1", altitude="operational"),
        ],
        decisions=[
            GeneratedDecision(id="d1", title="D1", decision_text="…",
                              rationale="r", revisit_triggers=["t1"]),
        ],
        commitments=[
            GeneratedCommitment(id="cm1", title="CM1", owner_id="a-eng",
                                contributes_to_goal_id="g2"),
            GeneratedCommitment(id="cm2", title="CM2", owner_id="a-eng",
                                depends_on=["cm1"],
                                constrained_by_decision_ids=["d1"]),
        ],
        signals=[
            GeneratedSignal(id="s1", source_channel="slack", source_ref="ts1",
                            author_id="a-eng", occurred_at="2026-04-01T00:00:00Z",
                            content_text="hi",
                            entities_mentioned=[EntityMention(type="commitment", id="cm1")]),
        ],
        recommendations=[
            GeneratedRecommendation(
                id="r1", proposition_text="reroute",
                target_act_ref=TargetActRef(type="commitment", id="cm1"),
                expected_impact_usd=1000.0,
                supporting_observation_ids=["s1"],
                target_actor_id="a-ceo",
            ),
        ],
    )


def test_valid_bundle_has_no_errors():
    assert validate_bundle(_tiny_bundle()) == []


def test_unresolved_actor_manager_flagged():
    b = _tiny_bundle()
    b.actors[1].manager_id = "ghost"
    errs = validate_bundle(b)
    assert any("manager_id ghost unresolved" in e for e in errs)


def test_actor_cycle_flagged():
    b = _tiny_bundle()
    b.actors[0].manager_id = "a-eng"
    b.actors[1].manager_id = "a-ceo"
    errs = validate_bundle(b)
    assert any("cycle" in e for e in errs)


def test_commitment_dep_cycle_flagged():
    b = _tiny_bundle()
    b.commitments[0].depends_on = ["cm2"]    # mutual dep -> cycle
    errs = validate_bundle(b)
    assert any("depends_on graph has a cycle" in e for e in errs)


def test_recommendation_dangling_signal_flagged():
    b = _tiny_bundle()
    b.recommendations[0].supporting_observation_ids = ["nope"]
    errs = validate_bundle(b)
    assert any("supporting_observation nope unresolved" in e for e in errs)


def test_count_mismatch_flagged():
    b = _tiny_bundle()
    spec = {"actor_count": 100, "customer_count": 1, "goal_count": 2,
            "decision_count": 1, "commitment_count": 2, "recommendation_count": 1}
    errs = validate_bundle(b, spec)
    assert any("count actors" in e for e in errs)
