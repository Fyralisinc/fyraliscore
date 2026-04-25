"""Tests for services/query/classifier.py.

Covers:
  - heuristic path (keyword-based short-circuits)
  - LLM path (via a ScriptedLLMProvider)
  - cache hit on repeat query
  - fallback to heuristic when LLM raises
  - fallback to 'arbitrary' when LLM raises and no heuristic fires
  - classification of a representative test set
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.query.classifier import (
    QueryClassifier,
    VALID_CATEGORIES,
    heuristic_classify,
)
from services.query.tests._helpers import ScriptedLLMProvider


TENANT = uuid4()


# --------------------------------------------------------------- heuristic


@pytest.mark.parametrize(
    "query, expected",
    [
        ("why is Acme renewal at risk?", "why"),
        ("Why did the sales pipeline slip?", "why"),
        ("how come we missed the deadline?", "why"),
        ("show me the at-risk customers", "show_me"),
        ("show the overdue commitments", "show_me"),
        ("list open PRs this week", "show_me"),
        ("give me the team's calendar", "show_me"),
        ("draft a message to Marcus about Acme", "draft"),
        ("compose a reply to @alice", "draft"),
        ("write an update for the board", "draft"),
        ("what if we cut the billing project?", "what_if"),
        ("what happened yesterday?", "summary"),
        ("summary of last week", "summary"),
        ("summarize the Acme situation", "summary"),
        ("what did we ship this week", "summary"),
        ("recap the Monday meeting", "summary"),
    ],
)
def test_heuristic_hits(query, expected):
    cat, conf = heuristic_classify(query)
    assert cat == expected
    assert conf >= 0.9


def test_heuristic_misses_ambiguous():
    # No leading keyword — heuristic should not fire.
    cat, conf = heuristic_classify("tell me about Acme")
    assert cat is None
    assert conf == 0.0


def test_heuristic_empty_query():
    assert heuristic_classify("") == (None, 0.0)
    assert heuristic_classify("   ") == (None, 0.0)


# --------------------------------------------------------------- classifier path


async def test_classifier_heuristic_skips_llm():
    """High-confidence heuristic short-circuits the LLM call."""
    provider = ScriptedLLMProvider(answer_category="arbitrary")  # would be wrong
    clf = QueryClassifier(provider=provider)
    result = await clf.classify(TENANT, "why is Acme at risk?")
    assert result.category == "why"
    assert result.source == "heuristic"
    assert provider.calls == []


async def test_classifier_falls_through_to_llm():
    """Ambiguous query → LLM gets called."""
    provider = ScriptedLLMProvider(answer_category="arbitrary")
    clf = QueryClassifier(provider=provider)
    result = await clf.classify(TENANT, "tell me about Acme")
    assert result.category == "arbitrary"
    assert result.source == "llm"
    assert len(provider.calls) == 1


async def test_classifier_cache_hit_on_repeat():
    provider = ScriptedLLMProvider(answer_category="arbitrary")
    clf = QueryClassifier(provider=provider)
    r1 = await clf.classify(TENANT, "tell me about the Acme refactor")
    assert r1.source == "llm"
    r2 = await clf.classify(TENANT, "tell me about the Acme refactor")
    assert r2.source == "cache"
    assert r2.category == r1.category
    # Only one LLM call across the two classifications.
    assert len(provider.calls) == 1


async def test_classifier_cache_differentiates_card_context():
    provider = ScriptedLLMProvider(answer_category="arbitrary")
    clf = QueryClassifier(provider=provider)
    await clf.classify(TENANT, "tell me about Acme", has_card_context=False)
    await clf.classify(TENANT, "tell me about Acme", has_card_context=True)
    # Two different cache keys → two LLM calls.
    assert len(provider.calls) == 2


async def test_classifier_falls_back_when_llm_raises():
    class _Broken(ScriptedLLMProvider):
        async def structured(self, **kw):
            raise RuntimeError("deepseek down")
    provider = _Broken()
    clf = QueryClassifier(provider=provider)
    result = await clf.classify(TENANT, "tell me about Acme")
    assert result.category == "arbitrary"
    assert result.source == "fallback"
    assert "llm_error" in result.trace


async def test_classifier_empty_query_is_arbitrary():
    clf = QueryClassifier(provider=ScriptedLLMProvider())
    result = await clf.classify(TENANT, "")
    assert result.category == "arbitrary"
    assert result.source == "fallback"


async def test_llm_provider_invalid_category_survives():
    """Structured response with a category not in VALID_CATEGORIES
    should still return a valid category (pydantic Literal guard).

    Pydantic rejects the invalid category during parsing → LLM path
    raises → we fall back. Exercises the robustness path."""
    class _BadProvider(ScriptedLLMProvider):
        async def structured(self, **kw):
            # Return a wrong literal — pydantic raises.
            from lib.llm.provider import LLMParseError
            raise LLMParseError("bad category", schema="_ClassifierOutput")
    clf = QueryClassifier(provider=_BadProvider())
    result = await clf.classify(TENANT, "tell me about Acme")
    assert result.category in VALID_CATEGORIES


async def test_classifier_representative_set():
    """End-to-end test set covering each category via heuristic paths."""
    test_cases = [
        ("why is Acme at risk?", "why"),
        ("show me at-risk customers", "show_me"),
        ("draft a message to Marcus", "draft"),
        ("what if we defer the billing refactor?", "what_if"),
        ("what happened yesterday", "summary"),
        ("tell me about Nepal hydropower", "arbitrary"),  # needs LLM
    ]
    provider = ScriptedLLMProvider(answer_category="arbitrary")
    clf = QueryClassifier(provider=provider)
    misclassified: list[tuple[str, str, str]] = []
    for query, expected in test_cases:
        result = await clf.classify(TENANT, query)
        if result.category != expected:
            misclassified.append((query, expected, result.category))
    assert not misclassified, f"misclassified: {misclassified}"
