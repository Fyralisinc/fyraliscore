from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.llm.telemetry import (
    CognitionTraceEvent,
    LogicalCallReceipt,
    PhysicalAttemptReceipt,
    UnsafeCognitionTrace,
    sanitize_cognition_payload,
)
from services.reasoning.think.llm_receipts import (
    ReceiptIntegrityError,
    ThinkLLMReceiptCollector,
)
from services.reasoning.think.debug_capture import capture as debug_capture


def _collector() -> ThinkLLMReceiptCollector:
    now = datetime.now(timezone.utc)
    collector = ThinkLLMReceiptCollector(tenant_id=uuid4())
    collector.record_logical_call(LogicalCallReceipt(
        logical_call_id="logical-1", provider="test", model="model",
        purpose="main_reasoning", schema_name="Decision",
        prompt_digest="a" * 64, started_at=now, ended_at=now,
        outcome="success", physical_attempt_count=1,
    ))
    collector.record_attempt(PhysicalAttemptReceipt(
        physical_attempt_id="attempt-1", logical_call_id="logical-1",
        parent_attempt_id=None, provider="test", model="model",
        purpose="main_reasoning", ordinal=1, started_at=now, ended_at=now,
        outcome="success",
    ))
    prompt_payload = {"system_text": "s"}
    collector.record_cognition_event(CognitionTraceEvent(
        schema_version="think-cognition-trace-v1", trace_id="trace-1",
        event_id="event-1",
        logical_call_id="logical-1", stage="prompt",
        cognitive_purpose="main_synthesis", payload=prompt_payload,
        content_digest=canonical_sha256(prompt_payload), occurred_at=now,
    ))
    return collector


def test_provider_free_trace_reconstructs_compile_validate_apply():
    collector = _collector()
    collector.record_pipeline_stage("compiler", {"normalizations": []})
    collector.record_pipeline_stage("validated_command", {"outcome": "accepted"})
    collector.record_pipeline_stage("applied_result", {"outcome": "applied"})

    collector.validate_reconciliation()
    assert [event.stage for event in collector.cognition_events] == [
        "prompt", "compiler", "validated_command", "applied_result",
    ]
    assert len({event.trace_id for event in collector.cognition_events}) == 1
    assert all(len(event.content_digest) == 64 for event in collector.cognition_events)


def test_reconciliation_rejects_attempt_count_and_retry_drift():
    collector = _collector()
    collector.logical_calls[0] = replace(
        collector.logical_calls[0], physical_attempt_count=2
    )
    with pytest.raises(ReceiptIntegrityError, match="attempt count mismatch"):
        collector.validate_reconciliation()


def test_trace_safety_redacts_secrets_and_rejects_gold():
    assert sanitize_cognition_payload({"api_key": "secret"}) == {
        "api_key": "[REDACTED]"
    }
    with pytest.raises(UnsafeCognitionTrace, match="evaluator field forbidden"):
        sanitize_cognition_payload({"expected_thesis": "hidden"})
    assert sanitize_cognition_payload({"confidence_threshold": 0.7}) == {
        "confidence_threshold": 0.7
    }
    with pytest.raises(UnsafeCognitionTrace, match="evaluator field forbidden"):
        sanitize_cognition_payload({"scorer_threshold": 0.9})


@pytest.mark.asyncio
async def test_persistence_rejects_mismatched_cognition_digest():
    collector = _collector()
    collector.cognition_events[0] = replace(
        collector.cognition_events[0], content_digest="0" * 64
    )

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

        async def fetchval(self, _sql, *args):
            return args[1]

    with pytest.raises(ReceiptIntegrityError, match="digest mismatch"):
        await collector.persist(Connection())


def test_terminal_outcomes_bind_to_pipeline_owner_not_later_entity_call():
    collector = _collector()
    collector.record_pipeline_stage("validated_command", {"outcome": "accepted"})
    main_call_id = collector.logical_calls[-1].logical_call_id
    collector.logical_calls.append(replace(
        collector.logical_calls[-1], logical_call_id="entity-call",
        purpose="entity_grounding",
    ))

    collector.set_terminal_outcomes(
        validation_outcome="accepted", apply_outcome="applied"
    )

    assert collector.terminal_outcomes == {
        main_call_id: ("accepted", "applied")
    }


@pytest.mark.asyncio
async def test_unsafe_stage_capture_does_not_abort_and_marks_incomplete(monkeypatch):
    collector = _collector()
    monkeypatch.setenv("DEBUG_ARTIFACT_CAPTURE", "0")

    with collector.capture():
        await debug_capture(
            object(), run_id=uuid4(), tenant_id=collector.tenant_id,
            stage="validation", payload={"expected_thesis": "forbidden"},
        )

    event = collector.cognition_events[-1]
    assert event.stage == "validated_command"
    assert event.payload["capture_complete"] is False
    assert event.payload["failure_class"] == "UnsafeCognitionTrace"
