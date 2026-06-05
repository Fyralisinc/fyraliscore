"""Unit tests for services.reasoning.sage.affordances.policy.derive_default_profile_from_model.

These tests exercise the heuristic mapping from a Model's stance +
memory-grammar shape to a v1 retrieval affordance profile. No DB.

Coverage:
  * Each of the four proposition kinds (observation, belief, prediction,
    norm) yields its stance-baseline primitives.
  * `claim_role` overlay adds the expected primitives.
  * `modality` nudges (observed / normative / expected) fire.
  * `polarity == "negative"` injects COUNTEREVIDENCE.
  * Edges (`supporting_model_ids`, `contributing_models`, `scope_actors`)
    add DEPENDENCY / GOAL_IMPACT / OWNERSHIP.
  * `abstraction_level` overlays (pattern, composite).
  * Both dict-style and attribute-style rows are accepted.
  * Missing id/tenant raises ValueError.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.sage.affordances.policy import derive_default_profile_from_model


_TENANT = uuid4()


def _row(**overrides):
    """Build a minimal dict-shaped Model row, override per test."""
    base = {
        "id": uuid4(),
        "tenant_id": _TENANT,
        "proposition": {"kind": "belief"},
        "proposition_kind": None,
        "claim_role": None,
        "abstraction_level": None,
        "modality": None,
        "polarity": None,
        "scope_actors": [],
        "supporting_model_ids": [],
        "contributing_models": [],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------
# Stance baselines (the four proposition kinds)
# ---------------------------------------------------------------------


def test_observation_baseline_primitives():
    profile = derive_default_profile_from_model(
        _row(proposition={"kind": "observation"}, proposition_kind="observation"),
    )
    assert "CAUSE" in profile.answers_question_primitives
    assert "COUNTEREVIDENCE" in profile.answers_question_primitives


def test_belief_baseline_primitives():
    profile = derive_default_profile_from_model(
        _row(proposition={"kind": "belief"}, proposition_kind="belief"),
    )
    assert "DEPENDENCY" in profile.answers_question_primitives
    assert "CONSTRAINT" in profile.answers_question_primitives


def test_prediction_baseline_primitives():
    profile = derive_default_profile_from_model(
        _row(proposition={"kind": "prediction"}, proposition_kind="prediction"),
    )
    assert "GOAL_IMPACT" in profile.answers_question_primitives
    assert "RECURRENCE" in profile.answers_question_primitives


def test_norm_baseline_primitives():
    profile = derive_default_profile_from_model(
        _row(proposition={"kind": "norm"}, proposition_kind="norm"),
    )
    assert "ACTION" in profile.answers_question_primitives
    assert "OWNERSHIP" in profile.answers_question_primitives


def test_kind_falls_back_to_proposition_when_column_missing():
    # proposition_kind generated column may not be hydrated on fresh
    # ModelCreate shapes — we should still pick up 'observation' from
    # proposition['kind'].
    profile = derive_default_profile_from_model(
        _row(proposition={"kind": "observation"}, proposition_kind=None),
    )
    assert "CAUSE" in profile.answers_question_primitives


# ---------------------------------------------------------------------
# claim_role overlay
# ---------------------------------------------------------------------


def test_claim_role_concern_adds_constraint_and_counterevidence():
    profile = derive_default_profile_from_model(_row(claim_role="concern"))
    assert "CONSTRAINT" in profile.answers_question_primitives
    assert "COUNTEREVIDENCE" in profile.answers_question_primitives


def test_claim_role_pattern_adds_pattern_and_recurrence():
    profile = derive_default_profile_from_model(_row(claim_role="pattern"))
    assert "PATTERN" in profile.answers_question_primitives
    assert "RECURRENCE" in profile.answers_question_primitives


def test_claim_role_recommendation_adds_action_and_ownership():
    profile = derive_default_profile_from_model(
        _row(claim_role="recommendation"),
    )
    assert "ACTION" in profile.answers_question_primitives
    assert "OWNERSHIP" in profile.answers_question_primitives


# ---------------------------------------------------------------------
# Modality nudges
# ---------------------------------------------------------------------


def test_modality_observed_adds_cause():
    profile = derive_default_profile_from_model(_row(modality="observed"))
    assert "CAUSE" in profile.answers_question_primitives


def test_modality_normative_adds_action_and_ownership():
    profile = derive_default_profile_from_model(_row(modality="normative"))
    assert "ACTION" in profile.answers_question_primitives
    assert "OWNERSHIP" in profile.answers_question_primitives


def test_modality_expected_adds_goal_impact():
    profile = derive_default_profile_from_model(_row(modality="expected"))
    assert "GOAL_IMPACT" in profile.answers_question_primitives


# ---------------------------------------------------------------------
# Polarity
# ---------------------------------------------------------------------


def test_negative_polarity_injects_counterevidence():
    profile = derive_default_profile_from_model(_row(polarity="negative"))
    assert "COUNTEREVIDENCE" in profile.answers_question_primitives


def test_positive_polarity_does_not_add_counterevidence_unless_else():
    # Plain belief with no other signal — DEPENDENCY/CONSTRAINT come
    # from the stance baseline, NOT COUNTEREVIDENCE.
    profile = derive_default_profile_from_model(
        _row(
            proposition={"kind": "belief"},
            proposition_kind="belief",
            polarity="positive",
        ),
    )
    assert "COUNTEREVIDENCE" not in profile.answers_question_primitives


# ---------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------


def test_supporting_model_ids_adds_dependency():
    profile = derive_default_profile_from_model(
        _row(supporting_model_ids=[uuid4()]),
    )
    assert "DEPENDENCY" in profile.answers_question_primitives


def test_contributing_models_adds_goal_impact():
    profile = derive_default_profile_from_model(
        _row(contributing_models=[uuid4()]),
    )
    assert "GOAL_IMPACT" in profile.answers_question_primitives


def test_scope_actors_adds_ownership():
    profile = derive_default_profile_from_model(
        _row(scope_actors=[uuid4()]),
    )
    assert "OWNERSHIP" in profile.answers_question_primitives


# ---------------------------------------------------------------------
# Abstraction level
# ---------------------------------------------------------------------


def test_abstraction_pattern_adds_pattern_and_recurrence():
    profile = derive_default_profile_from_model(
        _row(abstraction_level="pattern"),
    )
    assert "PATTERN" in profile.answers_question_primitives
    assert "RECURRENCE" in profile.answers_question_primitives


def test_abstraction_composite_adds_constraint():
    profile = derive_default_profile_from_model(
        _row(abstraction_level="composite"),
    )
    assert "CONSTRAINT" in profile.answers_question_primitives


# ---------------------------------------------------------------------
# Shape acceptance + invariants
# ---------------------------------------------------------------------


def test_attribute_style_row_accepted():
    class _AttrRow:
        def __init__(self):
            self.id = uuid4()
            self.tenant_id = _TENANT
            self.proposition = {"kind": "norm"}
            self.proposition_kind = "norm"
            self.claim_role = "recommendation"
            self.modality = "normative"
            self.polarity = "mixed"
            self.abstraction_level = "atomic"
            self.scope_actors = []
            self.supporting_model_ids = []
            self.contributing_models = []

    profile = derive_default_profile_from_model(_AttrRow())
    assert "ACTION" in profile.answers_question_primitives
    assert "OWNERSHIP" in profile.answers_question_primitives


def test_primitives_are_sorted_and_unique():
    profile = derive_default_profile_from_model(
        _row(
            proposition={"kind": "norm"},
            proposition_kind="norm",
            modality="normative",
            claim_role="recommendation",
        ),
    )
    primitives = profile.answers_question_primitives
    assert primitives == sorted(set(primitives))


def test_utility_score_starts_at_zero():
    profile = derive_default_profile_from_model(_row())
    assert profile.utility_score == 0.0


def test_missing_id_raises():
    with pytest.raises(ValueError):
        derive_default_profile_from_model(_row(id=None))


def test_missing_tenant_raises():
    with pytest.raises(ValueError):
        derive_default_profile_from_model(_row(tenant_id=None))
