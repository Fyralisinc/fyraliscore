from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

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
    collector.record_cognition_event(CognitionTraceEvent(
        schema_version="think-cognition-trace-v1", trace_id="trace-1",
        logical_call_id="logical-1", stage="prompt",
        cognitive_purpose="main_synthesis", payload={"system_text": "s"},
        content_digest="b" * 64, occurred_at=now,
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
