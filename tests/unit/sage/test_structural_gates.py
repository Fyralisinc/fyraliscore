"""Unit tests for services.reasoning.sage.structural_gates (Phase 6 v1).

Pure-Python, deterministic. No DB, no LLM. Uses lightweight test
doubles (SimpleNamespace) for the structural-feature inputs so this
file does not depend on pydantic or the structural_features package
beyond what the module's duck-typed access already supports.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from services.reasoning.sage.structural_gates import (
    GateInputs,
    GateScore,
    StructuralGateScorer,
)


# ---------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------


_NOW = datetime(2026, 5, 31, 12, 0, 0, tzinfo=timezone.utc)


def _features(
    *,
    hub_score: float | None = None,
    bridge_score: float | None = None,
) -> SimpleNamespace:
    """Lightweight ModelStructuralFeatures stand-in."""
    return SimpleNamespace(hub_score=hub_score, bridge_score=bridge_score)


def _edge_features(*, bridge_likelihood: float | None = None) -> SimpleNamespace:
    """Lightweight EdgeStructuralFeatures stand-in."""
    return SimpleNamespace(bridge_likelihood=bridge_likelihood)


def _inputs(
    *,
    edge_type: str = "blocks",
    edge_confidence: float = 0.8,
    age_days: float = 14.0,
    source_features: Any = None,
    target_features: Any = None,
    edge_features: Any = None,
    source_trust_tier: str | None = "evidenced",
    access_allowed: bool = True,
) -> GateInputs:
    return GateInputs(
        edge_type=edge_type,
        edge_confidence=edge_confidence,
        edge_updated_at=_NOW - timedelta(days=age_days),
        source_features=source_features,
        target_features=target_features,
        edge_features=edge_features,
        source_trust_tier=source_trust_tier,
        access_allowed=access_allowed,
    )


@pytest.fixture
def scorer() -> StructuralGateScorer:
    return StructuralGateScorer()


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_gatescore_components_contains_all_named_factors(
    scorer: StructuralGateScorer,
) -> None:
    res = scorer.score(
        gate_inputs=_inputs(),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    expected = {
        "relation_type_weight",
        "trust_weight",
        "freshness_weight",
        "role_compatibility",
        "bridge_bonus",
        "hub_penalty",
        "access_allowed",
    }
    assert set(res.components.keys()) == expected
    # Every factor is a finite float.
    for k, v in res.components.items():
        assert isinstance(v, float), k
        assert v == v, k  # NaN guard


def test_hub_dampened_for_dependency_but_not_constraint(
    scorer: StructuralGateScorer,
) -> None:
    hub_feats = _features(hub_score=0.9)
    deps = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            source_features=hub_feats,
            target_features=_features(),
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    cons = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            source_features=hub_feats,
            target_features=_features(),
        ),
        question_primitive="CONSTRAINT",
        now=_NOW,
    )
    assert deps.components["hub_penalty"] < 1.0
    assert cons.components["hub_penalty"] == 1.0
    assert cons.score > deps.score
    # BOTTLENECK alias should behave like CONSTRAINT.
    bn = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            source_features=hub_feats,
            target_features=_features(),
        ),
        question_primitive="BOTTLENECK",
        now=_NOW,
    )
    assert bn.components["hub_penalty"] == 1.0


def test_bridge_edge_gets_a_bonus(scorer: StructuralGateScorer) -> None:
    base = scorer.score(
        gate_inputs=_inputs(edge_type="blocks"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    bridged = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            edge_features=_edge_features(bridge_likelihood=0.8),
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert bridged.components["bridge_bonus"] > 1.0
    # Bonus pulls the final score up (subject to the [0,1] clamp).
    assert bridged.score >= base.score
    # Also works when only endpoint features carry the bridge signal.
    bridged_via_node = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            source_features=_features(bridge_score=0.9),
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert bridged_via_node.components["bridge_bonus"] > 1.0


def test_stale_edge_drops_freshness_to_floor(scorer: StructuralGateScorer) -> None:
    fresh = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", age_days=3),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    stale = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", age_days=400),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert fresh.components["freshness_weight"] == pytest.approx(1.1)
    assert stale.components["freshness_weight"] == pytest.approx(0.4)
    assert stale.score < fresh.score


def test_access_denied_kills_score(scorer: StructuralGateScorer) -> None:
    res = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            access_allowed=False,
            source_features=_features(bridge_score=1.0),
            edge_features=_edge_features(bridge_likelihood=1.0),
            source_trust_tier="authoritative",
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert res.score == 0.0
    assert res.components["access_allowed"] == 0.0
    assert "access denied" in res.reason


def test_role_compatible_beats_role_incompatible(
    scorer: StructuralGateScorer,
) -> None:
    compat = scorer.score(
        gate_inputs=_inputs(edge_type="blocks"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    incompat = scorer.score(
        gate_inputs=_inputs(edge_type="co_occurs_with"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert compat.components["role_compatibility"] == 1.0
    assert incompat.components["role_compatibility"] < 1.0
    assert compat.score > incompat.score


def test_trust_tier_ordering(scorer: StructuralGateScorer) -> None:
    auth = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", source_trust_tier="authoritative"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    unv = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", source_trust_tier="unverified"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    none = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", source_trust_tier=None),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert auth.components["trust_weight"] > unv.components["trust_weight"]
    assert auth.score > unv.score
    # None falls back to the documented default (0.7), which sits
    # between unverified (0.6) and asserted (0.85).
    assert none.components["trust_weight"] == pytest.approx(0.7)


def test_final_score_always_in_unit_interval(
    scorer: StructuralGateScorer,
) -> None:
    # Construct a maximally-boosted case: high-value relation, fresh,
    # high bridge, authoritative trust, role-compatible. The raw
    # product exceeds 1.0; the clamp must pin it at 1.0.
    boosted = scorer.score(
        gate_inputs=_inputs(
            edge_type="contradicts",
            age_days=1,
            source_trust_tier="authoritative",
            source_features=_features(bridge_score=1.0),
            edge_features=_edge_features(bridge_likelihood=1.0),
        ),
        question_primitive="CONTRADICTION",
        now=_NOW,
    )
    assert 0.0 <= boosted.score <= 1.0
    assert boosted.score == 1.0  # clamp engaged

    # And a maximally-suppressed case.
    suppressed = scorer.score(
        gate_inputs=_inputs(
            edge_type="contradicts",
            age_days=400,
            source_trust_tier="unverified",
            source_features=_features(hub_score=1.0),
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert 0.0 <= suppressed.score <= 1.0


def test_reason_mentions_dominant_downgrade(scorer: StructuralGateScorer) -> None:
    stale = scorer.score(
        gate_inputs=_inputs(edge_type="blocks", age_days=400),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert "freshness_weight" in stale.reason

    incompat = scorer.score(
        gate_inputs=_inputs(edge_type="co_occurs_with"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert "role_compatibility" in incompat.reason


def test_reason_mentions_dominant_bonus(scorer: StructuralGateScorer) -> None:
    bridged = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            edge_features=_edge_features(bridge_likelihood=0.9),
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert "bridge_bonus" in bridged.reason or "relation_type_weight" in bridged.reason


def test_custom_weights_override_changes_score() -> None:
    base = StructuralGateScorer()
    custom = StructuralGateScorer(
        weights={
            "relation_type_weight": {"DEPENDENCY:blocks": 0.5},
            "bridge_coefficient": 0.0,
            "trust_weight": {"evidenced": 0.5},
        }
    )
    inp = _inputs(
        edge_type="blocks",
        source_features=_features(bridge_score=1.0),
    )
    base_score = base.score(
        gate_inputs=inp, question_primitive="DEPENDENCY", now=_NOW,
    )
    custom_score = custom.score(
        gate_inputs=inp, question_primitive="DEPENDENCY", now=_NOW,
    )
    assert custom_score.score < base_score.score
    assert custom_score.components["relation_type_weight"] == pytest.approx(0.5)
    assert custom_score.components["bridge_bonus"] == pytest.approx(1.0)
    assert custom_score.components["trust_weight"] == pytest.approx(0.5)


def test_intent_primitive_alias_resolves_to_coarse_label(
    scorer: StructuralGateScorer,
) -> None:
    # IntentInferer emits "test_dependency"; gate must treat it as
    # DEPENDENCY when picking the relation-type weight.
    via_alias = scorer.score(
        gate_inputs=_inputs(edge_type="blocks"),
        question_primitive="test_dependency",
        now=_NOW,
    )
    via_coarse = scorer.score(
        gate_inputs=_inputs(edge_type="blocks"),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert via_alias.components["relation_type_weight"] == pytest.approx(
        via_coarse.components["relation_type_weight"]
    )
    assert via_alias.score == pytest.approx(via_coarse.score)


def test_intent_kind_overrides_question_primitive(
    scorer: StructuralGateScorer,
) -> None:
    res = scorer.score(
        gate_inputs=_inputs(edge_type="contradicts"),
        question_primitive="DEPENDENCY",
        intent_kind="CONTRADICTION",
        now=_NOW,
    )
    # CONTRADICTION+contradicts is boosted (1.5); DEPENDENCY+contradicts
    # is heavily downweighted (0.3). If intent_kind wins, we expect
    # the boost not the downweight.
    assert res.components["relation_type_weight"] >= 1.0


def test_missing_features_degrade_to_neutral(
    scorer: StructuralGateScorer,
) -> None:
    res = scorer.score(
        gate_inputs=_inputs(
            edge_type="blocks",
            source_features=None,
            target_features=None,
            edge_features=None,
        ),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert res.components["bridge_bonus"] == 1.0
    assert res.components["hub_penalty"] == 1.0


def test_naive_datetimes_treated_as_utc(scorer: StructuralGateScorer) -> None:
    # Naive ts are interpreted as UTC (matches asyncpg behaviour in
    # this repo). A 3-day-old naive ts must land in the freshest band.
    inp = GateInputs(
        edge_type="blocks",
        edge_confidence=0.8,
        edge_updated_at=(_NOW - timedelta(days=3)).replace(tzinfo=None),
        source_features=None,
        target_features=None,
        edge_features=None,
        source_trust_tier="evidenced",
        access_allowed=True,
    )
    res = scorer.score(
        gate_inputs=inp,
        question_primitive="DEPENDENCY",
        now=_NOW.replace(tzinfo=None),
    )
    assert res.components["freshness_weight"] == pytest.approx(1.1)


def test_returns_gatescore_dataclass(scorer: StructuralGateScorer) -> None:
    res = scorer.score(
        gate_inputs=_inputs(),
        question_primitive="DEPENDENCY",
        now=_NOW,
    )
    assert isinstance(res, GateScore)
    assert isinstance(res.score, float)
    assert isinstance(res.components, dict)
    assert isinstance(res.reason, str)
