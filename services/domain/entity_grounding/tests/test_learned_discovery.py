from __future__ import annotations

from uuid import uuid4

import pytest

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from services.domain.entity_grounding.learned_discovery import (
    AMBIGUOUS_IDENTIFIER_TYPE_CONFIDENCE_CAP,
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
async def test_type_confidence_caps_only_ambiguous_bare_identifiers() -> None:
    bare_id, typed_id, named = uuid4(), uuid4(), uuid4()
    signals = (
        PersistedSignalText(bare_id, "jira:issue", "RUNE-310 blocked delivery."),
        PersistedSignalText(typed_id, "jira:issue", "Goal ORBIT-52 blocked delivery."),
        PersistedSignalText(named, "email:message", "Selkie Maritime renewed."),
    )
    provider = ScriptedProvider({"mentions": [
        {"signal_id": str(bare_id), "surface": "RUNE-310", "span_start": 0,
         "span_end": 8, "entity_type": "goal", "confidence": .92, "abstain": False},
        {"signal_id": str(typed_id), "surface": "ORBIT-52", "span_start": 5,
         "span_end": 13, "entity_type": "goal", "confidence": .93, "abstain": False},
        {"signal_id": str(named), "surface": "Selkie Maritime", "span_start": 0,
         "span_end": 15, "entity_type": "customer", "confidence": .94, "abstain": False},
    ]})

    result = await discover_batch_mentions(provider=provider, signals=signals)
    by_surface = {item.surface: item for item in result.candidates}

    assert by_surface["RUNE-310"].detection_confidence == .92
    assert by_surface["RUNE-310"].type_confidence == (
        AMBIGUOUS_IDENTIFIER_TYPE_CONFIDENCE_CAP
    )
    assert "learned_type_hypothesis:goal" in by_surface["RUNE-310"].reason_codes
    assert "learned_type_confidence_capped_ambiguous_identifier" in (
        by_surface["RUNE-310"].reason_codes
    )
    assert by_surface["ORBIT-52"].type_confidence == .93
    assert "learned_type_supported_by_nearby_role_cue" in (
        by_surface["ORBIT-52"].reason_codes
    )
    assert by_surface["Selkie Maritime"].type_confidence == .94


@pytest.mark.asyncio
async def test_expands_attached_workstream_suffix_to_complete_designation() -> None:
    signal_id = uuid4()
    provider = ScriptedProvider({"mentions": [{
        "signal_id": str(signal_id), "surface": "Cinder Atlas",
        "span_start": 11, "span_end": 23, "entity_type": "workstream",
        "confidence": .95, "abstain": False,
    }]})

    result = await discover_batch_mentions(
        provider=provider,
        signals=(PersistedSignalText(
            signal_id, "slack:message", "We paused Cinder Atlas workstream yesterday."
        ),),
    )

    candidate = result.candidates[0]
    assert candidate.surface == "Cinder Atlas workstream"
    assert candidate.span_start == 10
    assert candidate.span_end == 33
    assert "learned_span_expanded_attached_type_designator" in candidate.reason_codes


@pytest.mark.asyncio
async def test_unattached_type_word_does_not_validate_bare_code_type() -> None:
    signal_id = uuid4()
    text = "Goal review moved; RUNE-310 blocked delivery."
    start = text.index("RUNE-310")
    provider = ScriptedProvider({"mentions": [{
        "signal_id": str(signal_id), "surface": "RUNE-310",
        "span_start": start, "span_end": start + 8, "entity_type": "goal",
        "confidence": .94, "abstain": False,
    }]})

    result = await discover_batch_mentions(
        provider=provider,
        signals=(PersistedSignalText(signal_id, "jira:issue", text),),
    )

    candidate = result.candidates[0]
    assert candidate.type_confidence == AMBIGUOUS_IDENTIFIER_TYPE_CONFIDENCE_CAP
    assert "learned_type_confidence_capped_ambiguous_identifier" in candidate.reason_codes


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
async def test_type_synonym_does_not_discard_otherwise_valid_batch() -> None:
    signal_id = uuid4()
    provider = ScriptedProvider(
        {"mentions": [{
            "signal_id": str(signal_id),
            "surface": "Mercury API",
            "span_start": 0,
            "span_end": 11,
            "entity_type": "service",
            "confidence": 0.96,
            "abstain": False,
        }]}
    )

    result = await discover_batch_mentions(
        provider=provider,
        signals=(PersistedSignalText(
            signal_id, "jira:issue", "Mercury API is degraded."
        ),),
    )

    assert result.mode == "learned"
    assert result.provider_error is None
    assert result.candidates[0].entity_type == "system"
    assert result.candidates[0].fate is EntityMentionDetectionFate.DETECTED


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
