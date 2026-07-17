"""Deterministic P1 receipts for logical calls and physical provider attempts."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from lib.llm.provider import LLMConfig, LLMParseError, LLMProvider
from lib.llm.telemetry import InMemoryLLMReceiptSink


class Answer(BaseModel):
    answer: str


class ScriptedProvider(LLMProvider):
    def __init__(self, responses: list[str | BaseException], *, retries: int = 0):
        super().__init__(
            LLMConfig(
                provider="test-provider",
                model="test-model",
                api_key="test",
                max_retries=retries,
            )
        )
        self.responses = iter(responses)

    async def _raw_call(self, **_: object) -> str:
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        return response


def _provider(
    responses: list[str | BaseException], *, retries: int = 0
) -> tuple[ScriptedProvider, InMemoryLLMReceiptSink]:
    provider = ScriptedProvider(responses, retries=retries)
    sink = InMemoryLLMReceiptSink()
    provider.set_receipt_sink(sink)
    return provider, sink


@pytest.mark.asyncio
async def test_success_has_one_logical_call_and_one_physical_attempt() -> None:
    provider, sink = _provider(['{"answer":"yes"}'])

    result = await provider.structured(system="s", user="u", schema=Answer)

    assert result.answer == "yes"
    assert len(sink.logical_calls) == 1
    assert len(sink.attempts) == 1
    logical = sink.logical_calls[0]
    attempt = sink.attempts[0]
    assert logical.outcome == "success"
    assert logical.physical_attempt_count == 1
    assert attempt.outcome == "success"
    assert attempt.logical_call_id == logical.logical_call_id
    assert attempt.ordinal == 1
    assert attempt.physical_attempt_id != logical.logical_call_id
    assert attempt.ended_at >= attempt.started_at


@pytest.mark.asyncio
async def test_parse_failure_and_retry_share_stable_logical_call_id() -> None:
    provider, sink = _provider(["not-json", '{"answer":"fixed"}'], retries=1)

    result = await provider.structured(system="s", user="u", schema=Answer)

    assert result.answer == "fixed"
    assert [item.outcome for item in sink.attempts] == ["parse_failure", "success"]
    assert sink.attempts[0].retry_scheduled is True
    assert sink.attempts[1].purpose == "parse_repair"
    assert sink.attempts[1].parent_attempt_id == sink.attempts[0].physical_attempt_id
    ids = {item.logical_call_id for item in sink.attempts}
    assert ids == {sink.logical_calls[0].logical_call_id}
    assert sink.logical_calls[0].physical_attempt_count == 2


@pytest.mark.asyncio
async def test_parse_exhaustion_is_terminal_and_never_schedules_another_retry() -> None:
    provider, sink = _provider(["bad", "still-bad"], retries=1)

    with pytest.raises(LLMParseError):
        await provider.structured(system="s", user="u", schema=Answer)

    assert [item.outcome for item in sink.attempts] == [
        "parse_failure",
        "parse_failure",
    ]
    assert [item.retry_scheduled for item in sink.attempts] == [True, False]
    assert sink.logical_calls[0].outcome == "exhausted"
    assert sink.logical_calls[0].physical_attempt_count == 2
    assert sink.logical_calls[0].error_class == "parse_error"


@pytest.mark.asyncio
async def test_provider_error_is_receipted_without_usage() -> None:
    provider, sink = _provider([RuntimeError("provider unavailable")])

    with pytest.raises(RuntimeError, match="provider unavailable"):
        await provider.structured(system="s", user="u", schema=Answer)

    attempt = sink.attempts[0]
    assert attempt.outcome == "provider_error"
    assert attempt.error_class == "transient"
    assert attempt.usage_exactness == "unavailable"
    assert sink.logical_calls[0].outcome == "provider_error"
    assert sink.logical_calls[0].physical_attempt_count == 1


@pytest.mark.asyncio
async def test_timeout_is_receipted_as_timeout() -> None:
    provider, sink = _provider([asyncio.TimeoutError("deadline")])

    with pytest.raises(asyncio.TimeoutError):
        await provider.structured(system="s", user="u", schema=Answer)

    assert sink.attempts[0].outcome == "timeout"
    assert sink.attempts[0].error_class == "timeout"
    assert sink.logical_calls[0].outcome == "timeout"
    assert sink.logical_calls[0].physical_attempt_count == 1


@pytest.mark.asyncio
async def test_separate_logical_calls_never_reuse_ids() -> None:
    provider, sink = _provider(['{"answer":"a"}', '{"answer":"b"}'])

    await provider.structured(system="s", user="u1", schema=Answer)
    await provider.structured(system="s", user="u2", schema=Answer)

    assert len({item.logical_call_id for item in sink.logical_calls}) == 2
    assert len({item.physical_attempt_id for item in sink.attempts}) == 2


@pytest.mark.asyncio
async def test_caller_can_preserve_logical_id_across_outer_retry_boundary() -> None:
    provider, sink = _provider(['{"answer":"yes"}'])

    await provider.structured(
        system="s",
        user="u",
        schema=Answer,
        logical_call_id="caller-stable-id",
    )

    assert sink.logical_calls[0].logical_call_id == "caller-stable-id"
    assert sink.attempts[0].logical_call_id == "caller-stable-id"
