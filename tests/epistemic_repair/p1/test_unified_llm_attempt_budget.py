from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import BaseModel

from lib.llm.provider import LLMConfig, LLMProvider, LLMTransientError
from lib.llm.telemetry import InMemoryLLMReceiptSink
from services.reasoning.think.llm_reason import _structured_with_reasoning_retries


class Result(BaseModel):
    value: str


class ScriptedProvider(LLMProvider):
    def __init__(self, script: list[object]) -> None:
        super().__init__(LLMConfig("openai", "test", "test", max_retries=20))
        self.script = list(script)
        self.calls = 0

    async def _raw_call(self, **_: object) -> str:
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, float):
            await asyncio.sleep(item)
            return json.dumps({"value": "late"})
        return str(item)


@pytest.mark.asyncio
async def test_parse_and_transport_failures_share_three_attempt_budget() -> None:
    provider = ScriptedProvider([
        "not-json",
        LLMTransientError("temporary provider failure"),
        json.dumps({"value": "ok"}),
    ])
    sink = InMemoryLLMReceiptSink()
    provider.set_receipt_sink(sink)

    result = await provider.structured(
        system="s", user="u", schema=Result, max_attempts=3,
    )

    assert result.value == "ok"
    assert provider.calls == 3
    assert [receipt.ordinal for receipt in sink.attempts] == [1, 2, 3]
    assert [receipt.outcome for receipt in sink.attempts] == [
        "parse_failure", "provider_error", "success",
    ]
    assert sink.logical_calls[0].physical_attempt_count == 3


@pytest.mark.asyncio
async def test_caller_cannot_expand_budget_beyond_three_attempts() -> None:
    provider = ScriptedProvider([LLMTransientError("temporary")] * 4)

    with pytest.raises(LLMTransientError):
        await provider.structured(
            system="s", user="u", schema=Result, max_attempts=99,
        )

    assert provider.calls == 3


@pytest.mark.asyncio
async def test_logical_deadline_bounds_the_physical_call() -> None:
    provider = ScriptedProvider([0.05])

    with pytest.raises(TimeoutError):
        await provider.structured(
            system="s", user="u", schema=Result,
            max_attempts=1, deadline_s=0.01,
        )

    assert provider.calls == 1


@pytest.mark.asyncio
async def test_think_wrapper_delegates_once_to_provider_retry_owner() -> None:
    provider = ScriptedProvider([json.dumps({"value": "ok"})])

    result, _ = await _structured_with_reasoning_retries(
        provider=provider,
        system="s",
        user="u",
        schema=Result,
        temperature=0.0,
        max_tokens=10,
        max_attempts=3,
    )

    assert result.value == "ok"
    assert provider.calls == 1
