"""Follow-up to the deferred cost-plan items, implemented flag-gated off:
  * 2.4 escalation provider on validation retry (THINK_ESCALATION_MODEL)
  * 2.2 step 3 diff-reuse on tx retry (THINK_REUSE_DIFF_ON_TX_RETRY)
  * 3.2 cascade-depth threading + THINK_MAX_INFERENTIAL_LINEAGE_DEPTH bound
"""
from __future__ import annotations

from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.cascade import propagate_cascade_depth
from services.reasoning.think.reason import (
    _diff_reuse_on_tx_retry_enabled,
    _hash_context_bundle,
)
from services.reasoning.think.tests.conftest import ScriptedProvider
from services.reasoning.think.worker import (
    ThinkWorker,
    _escalation_model_env,
    _max_inferential_lineage_depth,
)


# --- 2.4 escalation -------------------------------------------------------

def _worker_with_provider() -> ThinkWorker:
    provider = ScriptedProvider([])  # cfg model="m", provider="anthropic"
    return ThinkWorker(None, llm_provider=provider, embedder=object())


def test_escalation_model_env(monkeypatch):
    monkeypatch.delenv("THINK_ESCALATION_MODEL", raising=False)
    assert _escalation_model_env() is None
    monkeypatch.setenv("THINK_ESCALATION_MODEL", "  big-model  ")
    assert _escalation_model_env() == "big-model"


def test_escalation_none_without_feedback(monkeypatch):
    monkeypatch.setenv("THINK_ESCALATION_MODEL", "big-model")
    worker = _worker_with_provider()
    assert worker._maybe_escalation_provider({}) is None


def test_escalation_none_when_model_unset(monkeypatch):
    monkeypatch.delenv("THINK_ESCALATION_MODEL", raising=False)
    worker = _worker_with_provider()
    assert worker._maybe_escalation_provider({"validation_feedback": "x"}) is None


def test_escalation_builds_and_caches(monkeypatch):
    monkeypatch.setenv("THINK_ESCALATION_MODEL", "big-model")
    worker = _worker_with_provider()
    p1 = worker._maybe_escalation_provider({"validation_feedback": "x"})
    assert p1 is not None and p1.config.model == "big-model"
    p2 = worker._maybe_escalation_provider({"validation_feedback": "y"})
    assert p2 is p1  # cached


def test_escalation_skips_when_same_model(monkeypatch):
    monkeypatch.setenv("THINK_ESCALATION_MODEL", "m")  # == ScriptedProvider model
    worker = _worker_with_provider()
    assert worker._maybe_escalation_provider({"validation_feedback": "x"}) is None


# --- 2.2 step 3 diff-reuse ------------------------------------------------

def test_diff_reuse_flag(monkeypatch):
    monkeypatch.delenv("THINK_REUSE_DIFF_ON_TX_RETRY", raising=False)
    assert _diff_reuse_on_tx_retry_enabled() is False
    monkeypatch.setenv("THINK_REUSE_DIFF_ON_TX_RETRY", "1")
    assert _diff_reuse_on_tx_retry_enabled() is True


def test_bundle_hash_stable_and_sensitive():
    trigger = TriggerContext(kind="T1", tenant_id=uuid7(), observation_id=uuid7())
    m1, o1 = uuid7(), uuid7()
    bundle_a = SimpleNamespace(
        models=[SimpleNamespace(id=m1, version=1)],
        observations=[SimpleNamespace(id=o1)],
    )
    bundle_a2 = SimpleNamespace(
        models=[SimpleNamespace(id=m1, version=1)],
        observations=[SimpleNamespace(id=o1)],
    )
    bundle_b = SimpleNamespace(
        models=[SimpleNamespace(id=m1, version=2)],  # version changed
        observations=[SimpleNamespace(id=o1)],
    )
    assert _hash_context_bundle(trigger, bundle_a) == _hash_context_bundle(trigger, bundle_a2)
    assert _hash_context_bundle(trigger, bundle_a) != _hash_context_bundle(trigger, bundle_b)


# --- 3.2 cascade depth ----------------------------------------------------

def test_propagate_cascade_depth():
    assert propagate_cascade_depth(None)["cascade_depth"] == 1
    assert propagate_cascade_depth({"cascade_depth": 3})["cascade_depth"] == 4
    # extra fields merge on top.
    out = propagate_cascade_depth({"cascade_depth": 0}, extra={"k": "v"})
    assert out == {"cascade_depth": 1, "k": "v"}


def test_max_inferential_lineage_depth(monkeypatch):
    monkeypatch.delenv("THINK_MAX_INFERENTIAL_LINEAGE_DEPTH", raising=False)
    assert _max_inferential_lineage_depth(50) == 50
    monkeypatch.setenv("THINK_MAX_INFERENTIAL_LINEAGE_DEPTH", "5")
    assert _max_inferential_lineage_depth(50) == 5
    # Never exceeds the hard max.
    monkeypatch.setenv("THINK_MAX_INFERENTIAL_LINEAGE_DEPTH", "999")
    assert _max_inferential_lineage_depth(50) == 50
    monkeypatch.setenv("THINK_MAX_INFERENTIAL_LINEAGE_DEPTH", "garbage")
    assert _max_inferential_lineage_depth(50) == 50
