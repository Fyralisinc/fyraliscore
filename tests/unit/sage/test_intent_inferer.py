"""Unit tests for services.sage.intent_inferer (Phase 3 v1).

Pure Python — no DB, no LLM. Uses an inline synthetic StructuredCues
factory so this file does not depend on services.sage.cue_extractor
being implemented yet.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from services.sage.intent_inferer import (
    RetrievalIntent,
    RetrievalIntentInferer,
)


# ---------------------------------------------------------------------
# Inline synthetic StructuredCues — mirrors the doc §7.2 field list.
# ---------------------------------------------------------------------


@dataclass
class _Cues:
    explicit_entities: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    actor_mentions: list[str] = field(default_factory=list)
    team_mentions: list[str] = field(default_factory=list)
    customer_mentions: list[str] = field(default_factory=list)
    system_mentions: list[str] = field(default_factory=list)
    goal_mentions: list[str] = field(default_factory=list)
    commitment_mentions: list[str] = field(default_factory=list)
    relationship_clues: list[str] = field(default_factory=list)
    time_constraints: dict[str, Any] = field(default_factory=dict)
    status_constraints: list[str] = field(default_factory=list)
    source_constraints: list[str] = field(default_factory=list)
    access_constraints: list[str] = field(default_factory=list)
    expected_synthesis_decision_type: list[str] = field(default_factory=list)


def _cues(**overrides: Any) -> _Cues:
    return _Cues(**overrides)


@pytest.fixture
def inferer() -> RetrievalIntentInferer:
    return RetrievalIntentInferer()


# ---------------------------------------------------------------------
# Per-primitive happy paths
# ---------------------------------------------------------------------


def test_dependency_for_sso_blocker(inferer: RetrievalIntentInferer) -> None:
    cues = _cues(
        explicit_entities=["Acme", "SSO"],
        relationship_clues=["depends_on", "blocks", "critical_path"],
        time_constraints={"recent_window_days": 30},
        expected_synthesis_decision_type=["update_commitment_risk"],
    )

    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q1",
        question_text="Is SSO on the critical path for the Acme launch?",
    )

    kinds = {i.intent for i in intents}
    assert "test_dependency" in kinds
    dep = next(i for i in intents if i.intent == "test_dependency")
    assert dep.paths == ("structural", "temporal", "semantic")
    assert dep.question_id == "Q1"
    assert "Acme" in dep.target or "SSO" in dep.target
    assert dep.constraints["time_window"] == {"recent_window_days": 30}
    assert "Acme" in dep.constraints["scope_entities"]


def test_counterevidence_when_contradicts_or_but(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        explicit_entities=["pricing model"],
        relationship_clues=["contradicts"],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q2",
        question_text=(
            "Acme says pricing is fine but Globex contradicts that — "
            "however, look closer."
        ),
    )
    counter = [i for i in intents if i.intent == "find_counterevidence"]
    assert len(counter) == 1
    c = counter[0]
    assert "counterevidence" in c.paths
    assert "recent_observations" in c.paths
    assert c.success_condition.startswith("≥2 counterevidence")


def test_find_owner_when_assigned_to_or_who_owns(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        explicit_entities=["billing pipeline"],
        relationship_clues=["assigned_to"],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q3",
        question_text="Who owns the billing pipeline right now?",
    )
    owner = [i for i in intents if i.intent == "find_owner"]
    assert len(owner) == 1
    o = owner[0]
    assert o.paths == ("exact", "structural", "actor_team_graph")
    assert o.success_condition.startswith("owner identified")


def test_pattern_recurrence_across_customers(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        relationship_clues=["across_customers", "recurring"],
        expected_synthesis_decision_type=["create_emerging_bottleneck_model"],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q4",
        question_text=(
            "Is this onboarding friction recurring across customers?"
        ),
    )
    pat = [i for i in intents if i.intent == "find_pattern_recurrence"]
    assert len(pat) == 1
    p = pat[0]
    assert p.paths == ("semantic", "pattern", "model_edge")


def test_find_action_candidates_for_action_question(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        explicit_entities=["SSO blocker"],
        expected_synthesis_decision_type=["select_next_action"],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q5",
        question_text="What should we do about the SSO blocker next?",
    )
    act = [i for i in intents if i.intent == "find_action_candidates"]
    assert len(act) == 1
    a = act[0]
    assert a.paths == ("model_edge", "actor_team_graph", "structural")


# ---------------------------------------------------------------------
# Multi-faceted question -> multiple intents
# ---------------------------------------------------------------------


def test_multifaceted_question_produces_multiple_intents(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        explicit_entities=["SSO blocker", "Acme"],
        relationship_clues=["blocks", "assigned_to"],
        expected_synthesis_decision_type=[
            "update_commitment_risk",
            "select_next_action",
        ],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state=None,
        question_id="Q6",
        question_text=(
            "Who owns the SSO blocker for Acme and what should we do "
            "next about it?"
        ),
    )
    kinds = {i.intent for i in intents}
    # At minimum we want the dependency + owner + action triple.
    assert {"test_dependency", "find_owner", "find_action_candidates"} <= kinds
    assert len(intents) >= 3
    # Every intent is tied back to the same question.
    assert all(i.question_id == "Q6" for i in intents)


# ---------------------------------------------------------------------
# Empty cues -> single fallback test_dependency
# ---------------------------------------------------------------------


def test_empty_cues_yield_single_fallback_dependency(
    inferer: RetrievalIntentInferer,
) -> None:
    intents = inferer.infer(
        cues=_cues(),
        evidence_state=None,
        question_id="Q7",
        question_text="",
    )
    assert len(intents) == 1
    fallback = intents[0]
    assert fallback.intent == "test_dependency"
    assert fallback.paths
    assert fallback.success_condition
    assert fallback.target  # always non-empty (literal "(unspecified)" ok)


# ---------------------------------------------------------------------
# Structural invariants on every emitted intent
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "cues_kwargs,question_text",
    [
        (
            dict(relationship_clues=["depends_on"], explicit_entities=["X"]),
            "Does X block launch?",
        ),
        (
            dict(relationship_clues=["assigned_to"], explicit_entities=["Y"]),
            "Who owns Y?",
        ),
        (
            dict(relationship_clues=["contradicts"], explicit_entities=["Z"]),
            "Anything contradicting Z?",
        ),
        (
            dict(relationship_clues=["recurring"], explicit_entities=["W"]),
            "Is W recurring across customers?",
        ),
        (
            dict(
                expected_synthesis_decision_type=["select_next_action"],
                explicit_entities=["V"],
            ),
            "Next action for V?",
        ),
        (dict(), "totally generic question"),
    ],
)
def test_every_intent_has_paths_budget_and_success_condition(
    inferer: RetrievalIntentInferer,
    cues_kwargs: dict[str, Any],
    question_text: str,
) -> None:
    intents = inferer.infer(
        cues=_cues(**cues_kwargs),
        evidence_state=None,
        question_id="Q-invariants",
        question_text=question_text,
    )
    assert intents, "Inferer must always produce at least one intent."
    for i in intents:
        assert isinstance(i, RetrievalIntent)
        # non-empty ordered pathway tuple
        assert isinstance(i.paths, tuple) and len(i.paths) >= 1
        # populated budget with both required knobs
        assert i.budget.get("max_nodes", 0) > 0
        assert i.budget.get("max_evidence", 0) > 0
        # non-empty success condition string
        assert isinstance(i.success_condition, str)
        assert i.success_condition.strip()
        # priors in [0, 1]
        assert 0.0 <= i.expected_value <= 1.0
        assert 0.0 <= i.expected_cost <= 1.0
        # carries the question id through
        assert i.question_id == "Q-invariants"


# ---------------------------------------------------------------------
# Bonus: evidence_state.known_model_ids flows into constraints
# ---------------------------------------------------------------------


def test_evidence_state_known_ids_exposed_in_constraints(
    inferer: RetrievalIntentInferer,
) -> None:
    cues = _cues(
        explicit_entities=["Acme"],
        relationship_clues=["depends_on"],
    )
    intents = inferer.infer(
        cues=cues,
        evidence_state={"known_model_ids": ["m1", "m2"]},
        question_id="Q8",
        question_text="Does SSO block Acme launch?",
    )
    assert intents
    for i in intents:
        assert i.constraints["exclude_known_model_ids"] == ["m1", "m2"]


# ---------------------------------------------------------------------
# Bonus: custom default_budget is respected
# ---------------------------------------------------------------------


def test_custom_default_budget_is_applied() -> None:
    inferer = RetrievalIntentInferer(
        default_budget={"max_nodes": 5, "max_evidence": 3}
    )
    intents = inferer.infer(
        cues=_cues(relationship_clues=["depends_on"]),
        evidence_state=None,
        question_id="Q9",
        question_text="x depends on y?",
    )
    assert intents
    for i in intents:
        assert i.budget == {"max_nodes": 5, "max_evidence": 3}
