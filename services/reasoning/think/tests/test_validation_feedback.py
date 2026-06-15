"""Cost-plan §2.4 — validation-failure retry: feedback-append + classification.

Worker-side: validation-class failures get a separate retry cap
(`THINK_VALIDATION_MAX_ATTEMPTS`) and persist the validator's feedback into the
trigger payload. llm_reason then appends that feedback to the next attempt's
prompt so the retry avoids the dropped ops (and the changed prompt bytes make
the §2.2 response cache safe to reuse — C5).
"""
from __future__ import annotations

import json
from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.llm_reason import llm_reason
from services.reasoning.think.tests.conftest import ScriptedProvider
from services.reasoning.think.worker import (
    _classify_failure,
    _validation_max_attempts_env,
)


def _resp() -> str:
    return json.dumps({
        "trigger_ref": str(uuid7()),
        "tenant_id": str(uuid7()),
        "claim_ops": [],
        "reasoning_trace": "ok",
    })


def _t1(seed_signature: dict) -> TriggerContext:
    return TriggerContext(
        kind="T1", subkind="event_arrival", tenant_id=uuid7(),
        observation_id=uuid7(), seed_signature=seed_signature,
    )


async def test_feedback_appended_when_present():
    trigger = _t1({"validation_feedback": "dropped edge_ops[0]: bad endpoint uuid"})
    provider = ScriptedProvider([_resp()])
    await llm_reason(trigger, ContextBundle(), provider, max_attempts=1)
    user = provider.calls[0]["user"]
    assert "<prior_validation_feedback>" in user
    assert "bad endpoint uuid" in user


async def test_no_feedback_section_without_payload():
    provider = ScriptedProvider([_resp()])
    await llm_reason(_t1({}), ContextBundle(), provider, max_attempts=1)
    assert "<prior_validation_feedback>" not in provider.calls[0]["user"]


def test_classify_failure_buckets():
    val_exc = type("ValidationError", (Exception,), {})()
    reason_exc = type("ReasoningFailure", (Exception,), {})()
    assert _classify_failure(SimpleNamespace(exception=val_exc)) == "validation"
    assert _classify_failure(SimpleNamespace(exception=reason_exc)) == "reasoning"
    assert _classify_failure(SimpleNamespace(exception=ValueError("x"))) is None
    assert _classify_failure(SimpleNamespace(exception=None)) is None


def test_validation_max_attempts_env(monkeypatch):
    monkeypatch.delenv("THINK_VALIDATION_MAX_ATTEMPTS", raising=False)
    assert _validation_max_attempts_env() is None
    monkeypatch.setenv("THINK_VALIDATION_MAX_ATTEMPTS", "2")
    assert _validation_max_attempts_env() == 2
    monkeypatch.setenv("THINK_VALIDATION_MAX_ATTEMPTS", "garbage")
    assert _validation_max_attempts_env() is None
