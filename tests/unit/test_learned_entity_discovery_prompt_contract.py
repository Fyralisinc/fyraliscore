"""Focused contract tests for learned entity-discovery instructions."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.domain.entity_grounding.learned_discovery import (
    LearnedMentionBatch,
    PersistedSignalText,
    discover_batch_mentions,
)


class _PromptCaptureProvider:
    def __init__(self) -> None:
        self.system = ""
        self.user = ""

    async def structured(self, *, system, user, schema, temperature, max_tokens):
        self.system = system
        self.user = user
        return schema(mentions=())


@pytest.mark.asyncio
async def test_prompt_defines_complete_designator_boundary_policy() -> None:
    provider = _PromptCaptureProvider()
    signal = PersistedSignalText(uuid4(), "slack", "ordinary persisted text")

    await discover_batch_mentions(provider=provider, signals=(signal,))

    prompt = provider.system
    assert "smallest *complete written designation*" in prompt
    assert "Do not strip a type-bearing prefix from a code" in prompt
    assert "descriptive trailing nouns" in prompt
    assert "preserve every character inside names and identifiers" in prompt
    assert "repeat the\nper-signal completeness pass" in prompt
    assert "Obsidian Meadow workstream" in prompt


@pytest.mark.asyncio
async def test_prompt_defines_closed_types_by_role_and_transport_negatives() -> None:
    provider = _PromptCaptureProvider()
    signal = PersistedSignalText(uuid4(), "slack", "ordinary persisted text")

    await discover_batch_mentions(provider=provider, signals=(signal,))

    prompt = provider.system
    for expected in (
        "person: a named human",
        "team: an internal organizational group",
        "customer: an explicitly external client",
        "project: a bounded named project",
        "product: a named customer-facing product",
        "system: a technical service",
        "workstream: a named continuing stream of work",
        "goal: a named goal or objective",
        "commitment: a named promise",
        "decision: a named or coded decision",
        "resource: a named ticket",
        "other: only an explicit named business referent",
    ):
        assert expected in prompt
    assert "channel names, thread\nnumbers, timestamps, message IDs" in prompt
    assert "use other or abstain rather than guessing confidently" in prompt
    assert "do not infer a specific type from its prefix" in prompt
    assert "Never transfer entity/non-entity status" in prompt
    assert "explicitly introduces an alias" in prompt


def test_structured_schema_repeats_boundary_and_type_contract() -> None:
    schema = LearnedMentionBatch.model_json_schema()
    mention_schema = schema["$defs"]["LearnedMention"]["properties"]

    assert "smallest complete written designation" in mention_schema["surface"][
        "description"
    ]
    assert "other is a last resort" in mention_schema["entity_type"]["description"]
    assert set(mention_schema["entity_type"]["enum"]) == {
        "person", "team", "customer", "project", "product", "system",
        "workstream", "goal", "commitment", "decision", "resource", "other",
    }


@pytest.mark.asyncio
async def test_prompt_payload_remains_one_exact_focal_batch() -> None:
    provider = _PromptCaptureProvider()
    signals = tuple(
        PersistedSignalText(uuid4(), "slack", f"persisted text {index}")
        for index in range(10)
    )

    await discover_batch_mentions(provider=provider, signals=signals)

    payload = json.loads(provider.user)
    assert len(payload["signals"]) == 10
    assert [row["content_text"] for row in payload["signals"]] == [
        signal.content_text for signal in signals
    ]
