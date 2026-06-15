"""Cost-plan §2.5 — fast planning mode for operator-chosen low-value triggers.

Default is `deep` (no change). `THINK_FAST_PLAN_TRIGGER_KINDS` routes matching
trigger classes to `fast` mode, which the inquiry early-return collapses to a
single planning round.
"""
from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.context_planner import _plan_mode_for_trigger


def _trigger(kind: str, subkind: str | None = None) -> TriggerContext:
    return TriggerContext(kind=kind, subkind=subkind, tenant_id=uuid7())


def test_default_is_deep(monkeypatch):
    monkeypatch.delenv("THINK_FAST_PLAN_TRIGGER_KINDS", raising=False)
    assert _plan_mode_for_trigger(_trigger("T1", "event_arrival")) == "deep"


def test_fast_for_listed_kind(monkeypatch):
    monkeypatch.setenv("THINK_FAST_PLAN_TRIGGER_KINDS", "T4")
    assert _plan_mode_for_trigger(_trigger("T4", "model_reeval")) == "fast"
    assert _plan_mode_for_trigger(_trigger("T1", "event_arrival")) == "deep"


def test_fast_for_listed_kind_subkind(monkeypatch):
    monkeypatch.setenv("THINK_FAST_PLAN_TRIGGER_KINDS", "T1:event_arrival, T3")
    assert _plan_mode_for_trigger(_trigger("T1", "event_arrival")) == "fast"
    assert _plan_mode_for_trigger(_trigger("T3")) == "fast"
    assert _plan_mode_for_trigger(_trigger("T1", "state_change")) == "deep"
