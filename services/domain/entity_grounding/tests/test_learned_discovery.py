from __future__ import annotations

from uuid import uuid4

import pytest

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from services.domain.entity_grounding.learned_discovery import (
    DISCOVERY_BATCHES,
    DISCOVERY_READINESS,
    DiscoveryProviderPreflightError,
    LearnedMentionBatch,
    PersistedSignalText,
    discover_batch_mentions,
    preflight_structured_discovery,
)


class ScriptedProvider:
    def __init__(self, response: dict | Exception):
        self.response = response
        self.calls = []

    async def structured(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return LearnedMentionBatch.model_validate(self.response)


class PreflightProvider:
    def __init__(self, response: dict | Exception):
        self.response = response

    async def structured(self, **kwargs):
        if isinstance(self.response, Exception):
            raise self.response
        return kwargs["schema"].model_validate(self.response)


@pytest.fixture(autouse=True)
def _reset_discovery_metrics():
    DISCOVERY_BATCHES.reset()
    DISCOVERY_READINESS.reset()
    yield
    DISCOVERY_BATCHES.reset()
    DISCOVERY_READINESS.reset()


@pytest.mark.asyncio
async def test_one_call_discovers_batch_and_verifies_every_source_span() -> None:
    slack_a, slack_b, email = uuid4(), uuid4(), uuid4()
    signals = (
        PersistedSignalText(slack_a, "slack:message", "Northstar is blocked again."),
        PersistedSignalText(slack_b, "slack:message", "It needs Ada Lovelace today."),
        PersistedSignalText(email, "email", "Routine update; ACME-42 is unrelated."),
    )
    response = {"mentions": [
        {"signal_id": str(slack_a), "surface": "Northstar", "span_start": 0,
         "span_end": 9, "entity_type": "project", "confidence": .96, "abstain": False},
        {"signal_id": str(slack_b), "surface": "Ada Lovelace", "span_start": 9,
         "span_end": 21, "entity_type": "person", "confidence": .94, "abstain": False},
        # Hard negative is retained as a governed non-entity fate.
        {"signal_id": str(email), "surface": "Routine update", "span_start": 0,
         "span_end": 14, "entity_type": "other", "confidence": .35, "abstain": True},
        # Hallucinated/out-of-bound coordinates never become a mention.
        {"signal_id": str(email), "surface": "Imaginary Corp", "span_start": 99,
         "span_end": 113, "entity_type": "customer", "confidence": .99, "abstain": False},
        # Duplicate and nested mentions collapse to the maximal exact span.
        {"signal_id": str(slack_b), "surface": "Ada", "span_start": 9,
         "span_end": 12, "entity_type": "person", "confidence": .91, "abstain": False},
        {"signal_id": str(slack_a), "surface": "Northstar", "span_start": 0,
         "span_end": 9, "entity_type": "project", "confidence": .96, "abstain": False},
    ]}
    provider = ScriptedProvider(response)

    result = await discover_batch_mentions(provider=provider, signals=signals)

    assert result.mode == "learned"
    assert len(provider.calls) == 1
    by_surface = {candidate.surface: candidate for candidate in result.candidates}
    assert set(by_surface) == {"Northstar", "Ada Lovelace", "Routine update", "Imaginary Corp"}
    assert by_surface["Northstar"].fate is EntityMentionDetectionFate.DETECTED
    assert by_surface["Ada Lovelace"].fate is EntityMentionDetectionFate.DETECTED
    assert by_surface["Routine update"].fate is EntityMentionDetectionFate.REJECTED_NOT_ENTITY
    assert by_surface["Imaginary Corp"].fate is EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
    prompt = provider.calls[0]["system"].casefold()
    assert "every explicit named company-entity" in prompt
    assert "work signal by signal" in prompt
    assert "return each distinct literal occurrence" in prompt
    assert "never resolve" in prompt and "registry id" in prompt


@pytest.mark.asyncio
async def test_provider_failure_falls_back_without_partial_learned_output() -> None:
    provider = ScriptedProvider(RuntimeError("provider unavailable"))
    signal = PersistedSignalText(uuid4(), "jira:issue", "Nimbus migration blocked")

    result = await discover_batch_mentions(provider=provider, signals=(signal,))

    assert result.mode == "deterministic_fallback"
    assert result.candidates == ()
    assert result.provider_error and "provider unavailable" in result.provider_error
    assert len(provider.calls) == 1
    assert DISCOVERY_BATCHES.get(
        mode="deterministic_fallback", outcome="provider_error"
    ) == 1


@pytest.mark.asyncio
async def test_unique_exact_surface_repairs_bad_model_offsets() -> None:
    signal_id = uuid4()
    provider = ScriptedProvider(
        {"mentions": [{
            "signal_id": str(signal_id),
            "surface": "Project Komorebi",
            "span_start": 4,
            "span_end": 21,
            "entity_type": "project",
            "confidence": 0.93,
            "abstain": False,
        }]}
    )

    result = await discover_batch_mentions(
        provider=provider,
        signals=(PersistedSignalText(
            signal_id,
            "email:message",
            "Aiko leads Project Komorebi.",
        ),),
    )

    candidate = result.candidates[0]
    assert (candidate.span_start, candidate.span_end) == (11, 27)
    assert candidate.fate is EntityMentionDetectionFate.DETECTED
    assert "learned_span_repaired_unique_exact_surface" in candidate.reason_codes


@pytest.mark.asyncio
async def test_repeated_surface_with_bad_offsets_remains_rejected() -> None:
    signal_id = uuid4()
    provider = ScriptedProvider(
        {"mentions": [{
            "signal_id": str(signal_id),
            "surface": "Atlas",
            "span_start": 2,
            "span_end": 7,
            "entity_type": "project",
            "confidence": 0.93,
            "abstain": False,
        }]}
    )

    result = await discover_batch_mentions(
        provider=provider,
        signals=(PersistedSignalText(
            signal_id,
            "slack:message",
            "Atlas depends on Atlas.",
        ),),
    )

    assert result.candidates[0].fate is EntityMentionDetectionFate.REJECTED_NOT_ANCHORED


@pytest.mark.asyncio
async def test_preflight_marks_structured_provider_ready() -> None:
    await preflight_structured_discovery(PreflightProvider({"ready": True}))

    assert DISCOVERY_READINESS.get(state="ready") == 1
    assert DISCOVERY_READINESS.get(state="failed") == 0


@pytest.mark.asyncio
async def test_preflight_classifies_outdated_transport_as_configuration_failure() -> None:
    provider = PreflightProvider(
        RuntimeError("model is not supported by this version; upgrade Codex")
    )

    with pytest.raises(DiscoveryProviderPreflightError) as raised:
        await preflight_structured_discovery(provider)

    assert raised.value.code == "unsupported_or_outdated_model"
    assert raised.value.retryable is False
    assert DISCOVERY_READINESS.get(state="failed") == 1


@pytest.mark.asyncio
async def test_preflight_classifies_transient_startup_outage_separately() -> None:
    with pytest.raises(DiscoveryProviderPreflightError) as raised:
        await preflight_structured_discovery(
            PreflightProvider(RuntimeError("temporary connection reset"))
        )

    assert raised.value.code == "provider_unavailable"
    assert raised.value.retryable is True
