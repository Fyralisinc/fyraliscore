"""Tests for per-category strategies.

Each test exercises the strategy's parse + build_trigger (pure
functions — no DB). Gather() is tested via handler integration so we
can mock retrieval at a single seam; see test_core.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4


from services.query.strategies import get_strategy
from services.query.strategies.base import (
    extract_customer_candidates,
    extract_persons,
    extract_subject_keywords,
    extract_time_window,
    parse_recipient,
)


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
TENANT = uuid4()


# --------------------------------------------------------------- base helpers


def test_extract_persons_dedupes_and_lowercases():
    out = extract_persons("@Alice then @bob and @alice again")
    assert out == ["alice", "bob"]


def test_extract_persons_empty():
    assert extract_persons("") == []
    assert extract_persons("no mentions here") == []


def test_extract_customer_candidates_skips_first_token():
    # "Why" is a capitalized first word — should not be treated as a name.
    out = extract_customer_candidates("Why is Acme slipping?")
    assert "Why" not in out
    assert "Acme" in out


def test_extract_customer_candidates_handles_multi_word():
    out = extract_customer_candidates("How is Acme Corp doing?")
    # "How" skipped as first token; Acme Corp extracted.
    assert any(c.lower() == "acme corp" or c.lower() == "acme" for c in out)


def test_extract_subject_keywords_strips_stopwords():
    out = extract_subject_keywords("why is the billing service slipping?")
    assert "billing" in out
    assert "service" in out
    # Common stopwords we do filter out:
    assert "the" not in out
    assert "is" not in out


def test_extract_time_window_yesterday():
    anchor, window = extract_time_window("what happened yesterday", now=NOW)
    assert window == timedelta(days=1)
    assert anchor is not None


def test_extract_time_window_last_n_days():
    anchor, window = extract_time_window("show me the last 3 days", now=NOW)
    assert window == timedelta(days=3)
    assert anchor == NOW - timedelta(days=3)


def test_extract_time_window_none_when_no_match():
    anchor, window = extract_time_window("tell me about Acme", now=NOW)
    assert anchor is None
    assert window is None


def test_parse_recipient_mention():
    assert parse_recipient("draft a reply to @marcus") == "marcus"


def test_parse_recipient_to_name():
    assert parse_recipient("write a message to Marcus") == "marcus"


def test_parse_recipient_none():
    assert parse_recipient("draft an update") is None


# --------------------------------------------------------------- why strategy


def test_why_strategy_build_trigger_uses_T1():
    strat = get_strategy("why")
    parsed = strat.parse("why is Acme at risk?")
    trigger = strat.build_trigger(parsed, TENANT, now=NOW)
    assert trigger.kind == "T1"
    assert trigger.tenant_id == TENANT
    assert "acme" in (trigger.seed_natural_text or "").lower()
    # 'why' widens the temporal window.
    assert trigger.temporal_window >= timedelta(days=14)


def test_why_strategy_conversation_history_folds_into_keywords():
    strat = get_strategy("why")
    history = [
        {"query": "tell me about the Acme billing refactor"},
    ]
    parsed = strat.parse("why?", conversation_history=history)
    assert any("acme" in k or "billing" in k for k in parsed.subject_keywords)


def test_why_strategy_card_context_folds_in():
    strat = get_strategy("why")
    card = {"subject": "Acme renewal"}
    parsed = strat.parse("why?", card_context=card)
    assert any("acme" in k.lower() for k in parsed.subject_keywords)
    assert parsed.trace.get("card_subject") == "Acme renewal"


# --------------------------------------------------------------- show_me


def test_show_me_build_trigger():
    strat = get_strategy("show_me")
    parsed = strat.parse("show me at-risk customers")
    trigger = strat.build_trigger(parsed, TENANT, now=NOW)
    assert trigger.kind == "T1"
    # Tighter semantic_k for show_me.
    assert trigger.semantic_k == 25


# --------------------------------------------------------------- draft


def test_draft_strategy_parses_recipient():
    strat = get_strategy("draft")
    parsed = strat.parse("draft a reply to Marcus about Acme")
    assert parsed.recipient == "marcus"
    assert "acme" in parsed.subject_keywords


def test_draft_strategy_card_provides_recipient():
    strat = get_strategy("draft")
    card = {"subject": "Acme renewal", "recipient": "monica"}
    parsed = strat.parse("draft an update", card_context=card)
    assert parsed.recipient == "monica"


def test_draft_strategy_notes_voice_hints():
    # Build_trigger + notes is tested in handler tests; here we verify
    # the draft category carries the sender_voice_anchor via parse.
    strat = get_strategy("draft")
    parsed = strat.parse("draft an update for the board")
    # sender defaults to 'ceo'
    assert parsed.sender == "ceo"


# --------------------------------------------------------------- what_if


def test_what_if_extracts_hypothesis():
    strat = get_strategy("what_if")
    parsed = strat.parse("what if we defer the billing refactor?")
    assert parsed.counterfactual_hypothesis
    assert "billing" in parsed.counterfactual_hypothesis.lower()


def test_what_if_trigger_uses_T2_with_signature():
    strat = get_strategy("what_if")
    parsed = strat.parse("what if we cut Acme?")
    trigger = strat.build_trigger(parsed, TENANT, now=NOW)
    assert trigger.kind == "T2"
    assert trigger.seed_signature is not None
    assert trigger.seed_signature.get("kind") == "counterfactual"


# --------------------------------------------------------------- summary


def test_summary_default_window_is_24h():
    strat = get_strategy("summary")
    parsed = strat.parse("what did we ship?")
    assert parsed.time_window == timedelta(days=1)


def test_summary_respects_explicit_window():
    strat = get_strategy("summary")
    parsed = strat.parse("recap the last 7 days")
    assert parsed.time_window == timedelta(days=7)


# --------------------------------------------------------------- arbitrary


def test_arbitrary_falls_back_to_T1():
    strat = get_strategy("arbitrary")
    parsed = strat.parse("tell me something")
    trigger = strat.build_trigger(parsed, TENANT, now=NOW)
    assert trigger.kind == "T1"


def test_get_strategy_unknown_category_returns_arbitrary():
    strat = get_strategy("nonsense")  # type: ignore[arg-type]
    # ArbitraryStrategy is the default.
    assert strat.category == "arbitrary"
