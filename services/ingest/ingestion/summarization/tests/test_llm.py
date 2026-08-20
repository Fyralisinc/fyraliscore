"""Pure-logic tests for document summary parsing/rendering (no DB required).

Covers the Layer-0 changes (docs/plans/document-memory-substrate.md §3):
  - the structured extraction (summary/key_points/decisions/action_items/risks)
    is retained on SummaryResult instead of being discarded after render_summary;
  - action_items carry optional owner/due ({who?, what, due?}) with back-compat
    parsing of bare strings;
  - render_summary output is unchanged for the legacy bare-string shape;
  - map-reduce merge/dedup over partials, and the section splitter.
"""
from __future__ import annotations

import json

import pytest

from services.ingest.ingestion.summarization.llm import (
    ActionItem,
    DocumentSummarySchema,
    LLMSummarizer,
    SummaryResult,
    merge_partials,
    parse_summary_text,
    render_summary,
    split_into_sections,
    summarize_mapreduce,
)


_ACME_PAYLOAD = {
    "summary": "Q3 planning meeting covering the billing revamp and the Acme renewal.",
    "key_points": ["Billing revamp discussed"],
    "decisions": ["Ship billing revamp before the Sept 30 Acme renewal"],
    "action_items": ["Priya to send Acme a revised SOW by June 17"],
    "risks": ["SOC2 audit slip endangers the Acme renewal"],
}

# The brief that render_summary produced for _ACME_PAYLOAD under the legacy
# `action_items: list[str]` schema. The structured action_items change must not
# alter this rendering (bare action items still render as plain `what`).
# Note: render_summary's final truncate_summary_part collapses the internal
# newlines to single spaces, so the brief is one space-joined line.
_ACME_BRIEF = (
    "Q3 planning meeting covering the billing revamp and the Acme renewal. "
    "Key points: Billing revamp discussed "
    "Decisions: Ship billing revamp before the Sept 30 Acme renewal "
    "Actions: Priya to send Acme a revised SOW by June 17 "
    "Risks: SOC2 audit slip endangers the Acme renewal"
)


def _action_dicts(payload: dict) -> list[dict]:
    """Coerce a bare-string action_items list to the structured shape."""
    return [{"who": None, "what": s, "due": None} for s in payload["action_items"]]


def test_parse_summary_text_retains_structured_fields() -> None:
    result = parse_summary_text(json.dumps(_ACME_PAYLOAD), model="test-model", max_chars=1800)

    # Existing behaviour is unchanged: a flattened brief still lands in summary_text.
    assert "Acme" in result.summary_text
    assert result.model == "test-model"

    # New behaviour: the structured extraction is retained verbatim (not discarded),
    # so the document-memory substrate can distill it into durable Models.
    assert result.structured is not None
    assert result.structured["decisions"] == ["Ship billing revamp before the Sept 30 Acme renewal"]
    # action_items now carry optional owner/due; a bare string is coerced to {what}.
    assert result.structured["action_items"] == _action_dicts(_ACME_PAYLOAD)
    assert result.structured["risks"] == ["SOC2 audit slip endangers the Acme renewal"]


def test_render_summary_unchanged_for_short_brief() -> None:
    # render_summary() must keep producing the EXACT same flattened content_text
    # the legacy list[str] schema produced (byte-for-byte).
    parsed = DocumentSummarySchema.model_validate(_ACME_PAYLOAD)
    brief = render_summary(parsed, max_chars=1800)
    assert brief == _ACME_BRIEF


def test_action_item_backcompat_accepts_bare_strings() -> None:
    # A plain list of strings still parses (models that haven't adopted the
    # structured shape yet), coerced to {who:None, what:..., due:None}.
    parsed = DocumentSummarySchema.model_validate(_ACME_PAYLOAD)
    assert isinstance(parsed.action_items[0], ActionItem)
    assert parsed.action_items[0].who is None
    assert parsed.action_items[0].what == "Priya to send Acme a revised SOW by June 17"
    assert parsed.action_items[0].due is None


def test_action_item_structured_owner_due_preserved_and_rendered() -> None:
    payload = dict(_ACME_PAYLOAD)
    payload["action_items"] = [
        {"who": "Priya", "what": "send Acme revised SOW", "due": "2026-06-17"}
    ]
    parsed = DocumentSummarySchema.model_validate(payload)
    item = parsed.action_items[0]
    assert (item.who, item.what, item.due) == ("Priya", "send Acme revised SOW", "2026-06-17")
    # When owner/due are present the brief surfaces them (only then).
    brief = render_summary(parsed, max_chars=1800)
    assert "Actions: Priya: send Acme revised SOW (due 2026-06-17)" in brief


def test_structured_roundtrips_action_item_shape() -> None:
    # A SummaryResult built from structured action items keeps the dict shape.
    payload = dict(_ACME_PAYLOAD)
    payload["action_items"] = [{"who": "Priya", "what": "ship SOW", "due": "2026-06-17"}]
    result = parse_summary_text(json.dumps(payload), model=None, max_chars=1800)
    assert result.structured["action_items"] == [
        {"who": "Priya", "what": "ship SOW", "due": "2026-06-17"}
    ]


# --------------------------------------------------------------------------
# Map-reduce: section splitting + merge/dedup
# --------------------------------------------------------------------------


def test_split_into_sections_short_text_single_section() -> None:
    assert split_into_sections("abcdef", section_chars=100, overlap=10) == ["abcdef"]


def test_split_into_sections_overlapping_windows() -> None:
    # 16 chars, window 6, overlap 2 => step 4 => starts 0,4,8,12.
    sections = split_into_sections("0123456789ABCDEF", section_chars=6, overlap=2)
    assert sections == ["012345", "456789", "89ABCD", "CDEF"]
    # Adjacent sections share the overlap so boundary-straddling facts survive.
    assert sections[0][-2:] == sections[1][:2]


def test_merge_partials_concats_and_dedups() -> None:
    p_a = DocumentSummarySchema.model_validate(
        {
            "summary": "section A",
            "key_points": ["kp1", "kp2"],
            "decisions": ["d1"],
            "action_items": ["Priya: ship SOW"],
            "risks": ["r1"],
        }
    )
    p_b = DocumentSummarySchema.model_validate(
        {
            "summary": "section B",
            "key_points": ["KP1", "kp3"],  # KP1 dup of kp1 (case/space-insensitive)
            "decisions": ["d1", "d2"],  # d1 dup
            "action_items": [
                {"who": None, "what": "Priya: ship SOW", "due": None},  # dup of p_a
                "do X",
            ],
            "risks": ["r1", "r2"],  # r1 dup
        }
    )
    merged = merge_partials([p_a, p_b])
    assert merged["key_points"] == ["kp1", "kp2", "kp3"]
    assert merged["decisions"] == ["d1", "d2"]
    assert merged["risks"] == ["r1", "r2"]
    assert merged["action_items"] == [
        {"who": None, "what": "Priya: ship SOW", "due": None},
        {"who": None, "what": "do X", "due": None},
    ]


def test_merge_partials_dedups_action_items_on_owner_and_due() -> None:
    # Same `what`, different `who` => distinct commitments (not deduped).
    p = DocumentSummarySchema.model_validate(
        {
            "summary": "s",
            "action_items": [
                {"who": "Priya", "what": "ship SOW", "due": "2026-06-17"},
                {"who": "Sam", "what": "ship SOW", "due": "2026-06-17"},
                {"who": "Priya", "what": "ship SOW", "due": "2026-06-17"},  # dup
            ],
        }
    )
    merged = merge_partials([p])
    assert merged["action_items"] == [
        {"who": "Priya", "what": "ship SOW", "due": "2026-06-17"},
        {"who": "Sam", "what": "ship SOW", "due": "2026-06-17"},
    ]


# --------------------------------------------------------------------------
# Map-reduce: the async helper drives map then reduce over a fake provider
# --------------------------------------------------------------------------


class _FakeProvider:
    """Records prompts; returns a per-section schema on map, a fixed one on reduce."""

    class _Cfg:
        model = "fake-model"

    def __init__(self) -> None:
        self.config = _FakeProvider._Cfg()
        self.user_prompts: list[str] = []

    async def structured(self, *, system, user, schema, temperature, max_tokens):
        self.user_prompts.append(user)
        if "Per-section material" in user:  # reduce pass
            return schema.model_validate(
                {
                    "summary": "FINAL reduced summary",
                    "key_points": ["kp1"],
                    "decisions": ["d1"],
                    "action_items": [{"who": "Priya", "what": "ship SOW", "due": "2026-06-17"}],
                    "risks": ["r1"],
                }
            )
        idx = sum(1 for p in self.user_prompts if "Per-section material" not in p)
        return schema.model_validate(
            {
                "summary": f"section {idx}",
                "key_points": [f"kp{idx}"],
                "decisions": [f"d{idx}"],
                "action_items": [f"do {idx}"],
                "risks": [f"r{idx}"],
            }
        )

    @property
    def map_calls(self) -> list[str]:
        return [p for p in self.user_prompts if "Per-section material" not in p]

    @property
    def reduce_calls(self) -> list[str]:
        return [p for p in self.user_prompts if "Per-section material" in p]


@pytest.mark.asyncio
async def test_summarize_mapreduce_runs_map_then_reduce() -> None:
    provider = _FakeProvider()
    result = await summarize_mapreduce(
        "X" * 30,
        {"source_channel": "fireflies:transcript", "title": "Acme"},
        provider=provider,
        max_chars=1800,
        model="fake-model",
        section_chars=10,
        overlap=2,
    )
    # 30 chars, window 10, overlap 2 => step 8 => starts 0,8,16,24 => 4 map calls.
    assert len(provider.map_calls) == 4
    assert len(provider.reduce_calls) == 1
    assert isinstance(result, SummaryResult)
    assert result.model == "fake-model"
    assert result.structured["summary"] == "FINAL reduced summary"
    assert "FINAL reduced summary" in result.summary_text


@pytest.mark.asyncio
async def test_summarize_mapreduce_single_section_skips_reduce() -> None:
    provider = _FakeProvider()
    result = await summarize_mapreduce(
        "short text",
        {"source_channel": "notion:object"},
        provider=provider,
        max_chars=1800,
        section_chars=1000,
        overlap=50,
    )
    assert len(provider.map_calls) == 1
    assert provider.reduce_calls == []
    assert result.structured["summary"] == "section 1"


@pytest.mark.asyncio
async def test_llm_summarizer_threshold_gates_mapreduce() -> None:
    # Below threshold: exactly one single-call (no map-reduce framing).
    provider = _FakeProvider()
    summarizer = LLMSummarizer(provider, max_chars=1800, mapreduce_chars=1000)
    await summarizer.summarize("tiny", metadata={"source_channel": "notion:object"})
    assert len(provider.user_prompts) == 1
    assert "section 1 of" not in provider.user_prompts[0]

    # Above threshold: map-reduce engages (section framing appears).
    big_provider = _FakeProvider()
    big_summarizer = LLMSummarizer(big_provider, max_chars=1800, mapreduce_chars=20)
    await big_summarizer.summarize("Y" * 60, metadata={"source_channel": "notion:object"})
    assert any("section 1 of" in p for p in big_provider.user_prompts)
