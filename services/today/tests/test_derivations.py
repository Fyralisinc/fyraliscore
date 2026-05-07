"""Pure-function tests for the substrate→UI mapping in the today
aggregator. No DB or network — covers severity bucketing, kind labels,
tag derivation, action set selection, and suggested-paths shape.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from services.recommendations.repo import (
    RecommendationView,
    TargetEntitySummary,
)
from services.today.aggregator import (
    _derive_actions,
    _derive_category,
    _derive_kind_label,
    _derive_paths,
    _derive_severity,
    _derive_stats,
    _derive_tag,
)


def _view(
    *,
    confidence: float = 0.6,
    expected_impact: float | None = 0.5,
    qualitative_impact: str | None = None,
    operation: str = "transition",
    ref_type: str = "commitment",
    target_state: str | None = "active",
    target_archived: bool = False,
    target_title: str = "Pause the rate limiter",
    created_at: datetime | None = None,
) -> RecommendationView:
    target_id = uuid4()
    return RecommendationView(
        id=uuid4(),
        proposition_text="Pause the rate limiter — three weeks of slipping deliverables.",
        confidence=confidence,
        target_act_ref={"type": ref_type, "id": str(target_id)},
        proposed_change={"operation": operation, "payload": {"new_state": "paused"}},
        expected_impact=expected_impact,
        qualitative_impact=qualitative_impact,
        target_actor_id=uuid4(),
        supporting_event_ids=[],
        supporting_model_ids=[],
        created_at=created_at or datetime.now(timezone.utc),
        scope_entities=[],
        target_entity=TargetEntitySummary(
            type=ref_type,
            id=target_id,
            title=target_title,
            state=target_state,
            archived=target_archived,
        ),
        rank_score=(expected_impact or 0) * confidence,
    )


@pytest.mark.parametrize(
    "impact,conf,expected",
    [
        # Normalized regime — score = impact * confidence. Buckets match
        # services/today/aggregator.py: critical >= 0.80, strategic >= 0.55,
        # high >= 0.30, med >= 0.12, else low.
        (0.95, 0.90, "critical"),    # 0.855
        (0.80, 0.75, "strategic"),   # 0.600
        (0.70, 0.50, "high"),        # 0.350
        (0.30, 0.50, "med"),         # 0.150
        (0.10, 0.50, "low"),         # 0.050
    ],
)
def test_derive_severity_buckets(impact, conf, expected):
    assert _derive_severity(_view(confidence=conf, expected_impact=impact)) == expected


def test_derive_severity_uses_default_impact_when_missing():
    # Falls back to 0.5 when expected_impact is None
    sev = _derive_severity(_view(confidence=0.9, expected_impact=None))
    # 0.5 * 0.9 = 0.45 → high (>= 0.30, < 0.55)
    assert sev == "high"


def test_derive_category_operational_for_critical_transition():
    # Per spec §12 — Decision drift is critical severity but operational
    # category. Kind drives the category, not severity.
    v = _view(confidence=0.95, expected_impact=0.95, operation="transition")
    assert _derive_severity(v) == "critical"
    assert _derive_category(v, _derive_severity(v)) == "operational"


def test_derive_category_strategic_for_create_op():
    v = _view(confidence=0.5, expected_impact=0.4, operation="create")
    assert _derive_category(v, _derive_severity(v)) == "strategic"


def test_derive_category_operational_for_med_transition():
    v = _view(confidence=0.5, expected_impact=0.3, operation="transition")
    assert _derive_category(v, _derive_severity(v)) == "operational"


def test_derive_kind_label_decision_drift():
    v = _view(operation="transition", ref_type="decision")
    assert "Decision drift" in _derive_kind_label(v)


def test_derive_kind_label_strategic_feature():
    v = _view(operation="create", ref_type="goal")
    assert "Strategic" in _derive_kind_label(v)


def test_derive_tag_new_when_recent():
    v = _view(created_at=datetime.now(timezone.utc) - timedelta(hours=4))
    tag = _derive_tag(v, datetime.now(timezone.utc))
    assert tag == {"kind": "new", "label": "new"}


def test_derive_tag_weak_calibration_when_low_confidence():
    v = _view(confidence=0.4, created_at=datetime.now(timezone.utc) - timedelta(days=3))
    tag = _derive_tag(v, datetime.now(timezone.utc))
    assert tag == {"kind": "quiet", "label": "weak calibration"}


def test_derive_tag_routed_otherwise():
    v = _view(confidence=0.8, created_at=datetime.now(timezone.utc) - timedelta(days=3))
    tag = _derive_tag(v, datetime.now(timezone.utc))
    assert tag == {"kind": "quiet", "label": "routed to you"}


def test_derive_actions_includes_act_and_hold_always():
    actions = _derive_actions(_view())
    assert actions[0] == "act"
    assert "hold" in actions


def test_derive_actions_route_for_commitment_transition():
    v = _view(operation="transition", ref_type="commitment")
    actions = _derive_actions(v)
    assert "route" in actions


def test_derive_actions_dismiss_for_create():
    v = _view(operation="create", ref_type="goal")
    actions = _derive_actions(v)
    assert "dismiss" in actions


def test_derive_stats_includes_confidence():
    stats = _derive_stats(_view(confidence=0.82))
    assert stats[0]["label"] == "Confidence"
    assert stats[0]["value"] == "82%"


def test_derive_stats_includes_target_title():
    stats = _derive_stats(_view(target_title="Acme renewal"))
    assert any(s["label"] == "Target" and s["value"].startswith("Acme") for s in stats)


def test_derive_paths_returns_three_paths():
    paths = _derive_paths(_view())
    assert len(paths) == 3
    assert {p["label"] for p in paths} == {"Reaffirm", "Wait", "Reject"}


def test_derive_paths_uses_target_title_in_primary():
    paths = _derive_paths(_view(target_title="Acme renewal"))
    assert "Acme renewal" in paths[0]["body_html"]
