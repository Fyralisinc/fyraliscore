"""Pure-logic tests for document summary parsing/rendering (no DB required).

Covers the Layer-0 change: the structured extraction
(summary/key_points/decisions/action_items/risks) is retained on SummaryResult
instead of being discarded after render_summary().
See docs/plans/document-memory-substrate.md.
"""
from __future__ import annotations

import json

from services.ingest.ingestion.summarization.llm import (
    DocumentSummarySchema,
    parse_summary_text,
    render_summary,
)


_ACME_PAYLOAD = {
    "summary": "Q3 planning meeting covering the billing revamp and the Acme renewal.",
    "key_points": ["Billing revamp discussed"],
    "decisions": ["Ship billing revamp before the Sept 30 Acme renewal"],
    "action_items": ["Priya to send Acme a revised SOW by June 17"],
    "risks": ["SOC2 audit slip endangers the Acme renewal"],
}


def test_parse_summary_text_retains_structured_fields() -> None:
    result = parse_summary_text(json.dumps(_ACME_PAYLOAD), model="test-model", max_chars=1800)

    # Existing behaviour is unchanged: a flattened brief still lands in summary_text.
    assert "Acme" in result.summary_text
    assert result.model == "test-model"

    # New behaviour: the structured extraction is retained verbatim (not discarded),
    # so the document-memory substrate can distill it into durable Models.
    assert result.structured is not None
    assert result.structured["decisions"] == ["Ship billing revamp before the Sept 30 Acme renewal"]
    assert result.structured["action_items"] == ["Priya to send Acme a revised SOW by June 17"]
    assert result.structured["risks"] == ["SOC2 audit slip endangers the Acme renewal"]


def test_render_summary_unchanged_for_short_brief() -> None:
    # render_summary() must keep producing the same flattened content_text.
    parsed = DocumentSummarySchema.model_validate(_ACME_PAYLOAD)
    brief = render_summary(parsed, max_chars=1800)
    assert brief.startswith("Q3 planning meeting")
    assert "Decisions:" in brief
    assert "Risks:" in brief


def test_structured_is_none_only_when_unset() -> None:
    # A SummaryResult built without structured data leaves the field None so the
    # writer skips persisting it (back-compat for any non-parsed construction).
    parsed = DocumentSummarySchema.model_validate(_ACME_PAYLOAD)
    result = parse_summary_text(parsed.model_dump_json(), model=None, max_chars=1800)
    assert result.structured == _ACME_PAYLOAD
