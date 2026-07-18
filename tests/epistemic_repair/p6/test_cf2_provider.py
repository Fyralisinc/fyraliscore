from __future__ import annotations

import json
from uuid import uuid4

import pytest
from pydantic import BaseModel

from lib.llm.telemetry import InMemoryLLMReceiptSink
from lib.shared.ids import uuid7
from services.domain.entity_grounding.learned_discovery import LearnedMentionBatch
from services.evaluation.epistemic_repair.cf2_provider import (
    CF2ProviderFreeLLM,
    UnsupportedCF2StructuredCall,
)
from services.platform.execution.question_planning_schemas import (
    LLMCompactQuestionPlan,
)
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import BatchMemoryDecisionSet
from services.reasoning.think.diff_schema import RawDiff, RawDiffClaimsOnly
from services.reasoning.think.prompt import build_prompt


pytestmark = pytest.mark.asyncio


async def test_provider_extracts_mentions_only_from_runtime_signal_text() -> None:
    provider = CF2ProviderFreeLLM()
    signal_id = uuid4()
    text = "Atlas release, update 4: Ownership remains open."

    result = await provider.structured(
        system="Find exact entity mentions.",
        user=json.dumps({"signals": [{
            "signal_id": str(signal_id),
            "source_channel": "slack:message",
            "content_text": text,
        }]}),
        schema=LearnedMentionBatch,
    )

    assert len(result.mentions) == 1
    mention = result.mentions[0]
    assert mention.signal_id == signal_id
    assert mention.surface == "Atlas release"
    assert text[mention.span_start:mention.span_end] == mention.surface
    assert mention.entity_type == "project"


async def test_provider_supports_planner_and_conservative_main_defaults() -> None:
    provider = CF2ProviderFreeLLM()

    plan = await provider.structured(
        system="Plan inquiry.", user="Runtime context only.",
        schema=LLMCompactQuestionPlan,
    )
    decision = await provider.structured(
        system="Decide candidates.",
        user=json.dumps({"candidates": [{"candidate_id": "runtime-candidate"}]}),
        schema=BatchMemoryDecisionSet,
    )

    assert plan.q == [] and plan.d == []
    assert decision.decisions == []
    assert [call.schema_name for call in provider.calls] == [
        "LLMCompactQuestionPlan", "BatchMemoryDecisionSet",
    ]


async def test_runtime_handler_can_decide_from_prompt_without_gold_import() -> None:
    seen = []

    def decide(request):
        payload = json.loads(request.user)
        seen.append(payload)
        candidate_id = payload["candidates"][0]["candidate_id"]
        return {
            "decisions": [{
                "candidate_id": candidate_id,
                "decision": "reject",
                "confidence": 0.9,
                "reason": "Runtime candidate is retained without mutation.",
            }],
            "reasoning_trace": "request-only dynamic handler",
        }

    provider = CF2ProviderFreeLLM(
        handlers={"BatchMemoryDecisionSet": decide}
    )
    result = await provider.structured(
        system="Decide only from this request.",
        user=json.dumps({"candidates": [{"candidate_id": "MDC_RUNTIME_1"}]}),
        schema=BatchMemoryDecisionSet,
    )

    assert seen == [{"candidates": [{"candidate_id": "MDC_RUNTIME_1"}]}]
    assert result.decisions[0].candidate_id == "MDC_RUNTIME_1"
    assert result.decisions[0].decision == "reject"


async def test_raw_diff_noop_uses_runtime_ids_and_emits_receipts() -> None:
    provider = CF2ProviderFreeLLM()
    sink = InMemoryLLMReceiptSink()
    provider.set_receipt_sink(sink)
    tenant_id, trigger_id = uuid4(), uuid4()

    result = await provider.structured(
        system="Return a diff.",
        user=json.dumps({
            "tenant_id": str(tenant_id), "trigger_id": str(trigger_id),
        }),
        schema=RawDiff,
        context_digest="cf2-runtime-context",
    )

    assert result.tenant_id == tenant_id
    assert result.trigger_ref == trigger_id
    assert result.claim_ops == []
    assert len(sink.logical_calls) == 1
    assert len(sink.attempts) == 1
    assert sink.logical_calls[0].schema_name == "RawDiff"
    assert sink.attempts[0].usage_exactness == "estimated"
    telemetry = provider.telemetry()
    assert telemetry["call_count"] == 1
    assert telemetry["input_tokens"] > 0
    assert telemetry["output_tokens"] > 0


async def test_repair_prompt_exposes_required_raw_diff_coordinates() -> None:
    provider = CF2ProviderFreeLLM()
    tenant_id, trigger_id = uuid4(), uuid7()
    pair = build_prompt(
        TriggerContext(
            kind="T4",
            subkind="representation_repair",
            tenant_id=tenant_id,
            seed_signature={"trigger_id": str(trigger_id)},
        ),
        ContextBundle(),
    )

    result = await provider.structured(
        system=pair.system,
        user=pair.user,
        schema=RawDiffClaimsOnly,
    )

    assert result.tenant_id == tenant_id
    assert result.trigger_ref == trigger_id
    assert result.claim_ops == []


async def test_unsupported_schema_fails_closed() -> None:
    class UnsupportedShape(BaseModel):
        mystery: str

    provider = CF2ProviderFreeLLM()
    with pytest.raises(
        UnsupportedCF2StructuredCall,
        match="supported structural fingerprint",
    ):
        await provider.structured(
            system="Unknown call.", user="No implicit fallback.",
            schema=UnsupportedShape,
            max_attempts=1,
        )
