from __future__ import annotations

from services.reasoning.think.lanes import _is_reflex_trigger


def test_pattern_review_is_not_reflex_lane() -> None:
    assert _is_reflex_trigger("T4", "pattern_review") is False


def test_background_maintenance_stays_reflex_lane() -> None:
    assert _is_reflex_trigger("T4", "background_maintenance") is True
