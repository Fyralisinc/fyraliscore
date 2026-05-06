"""Tests for services/rendering/voice_rules.py (Phase 1 exit gate).

Each rule gets a positive test (fires on offending text), a negative
test (stays silent on clean text), and a context-sensitivity test
where the rule only fires on appropriate kinds (RequiresSpecificity).
"""
from __future__ import annotations


from services.rendering.voice_rules import (
    NoEmoji,
    NoExclamationMark,
    NoHedgePadding,
    NoMarketingLanguage,
    RequiresSpecificity,
    RULES,
    RuleContext,
    SentenceLengthLimit,
    Severity,
    check_all,
    format_corrections,
    has_rejections,
    strip_html,
)


# ---------------------------------------------------------------------
# Individual rules
# ---------------------------------------------------------------------


def test_no_exclamation_fires_on_exclaim():
    r = NoExclamationMark()
    v = r.check("Good morning! One thing to note.")
    assert len(v) == 1
    assert v[0].severity is Severity.REJECT
    assert v[0].rule == "no_exclamation_mark"


def test_no_exclamation_silent_on_clean():
    r = NoExclamationMark()
    assert r.check("Good morning. One thing to note.") == []


def test_no_exclamation_inside_html_still_fires():
    r = NoExclamationMark()
    v = r.check('<span class="serif">structurally unsafe</span>!')
    assert len(v) == 1


def test_no_marketing_fires_on_exciting():
    r = NoMarketingLanguage()
    v = r.check("Exciting news: Acme renewal is on track.")
    assert any(x.offending_text and x.offending_text.lower() == "exciting" for x in v)


def test_no_marketing_fires_on_insights():
    r = NoMarketingLanguage()
    v = r.check("Here are some insights on the quarter.")
    assert len(v) >= 1


def test_no_marketing_fires_on_unpack_and_dive_into():
    r = NoMarketingLanguage()
    v = r.check("Let's unpack the data and dive into the detail.")
    # Two phrases present.
    off = [x.offending_text.lower() for x in v if x.offending_text]
    assert "unpack" in off
    assert "dive into" in off


def test_no_marketing_allows_architectural_leverage():
    r = NoMarketingLanguage()
    v = r.check("The retrieval layer leverages the Models spine.")
    # Architectural "leverages" is not the marketing pattern.
    assert v == []


def test_no_marketing_flags_marketing_leverage():
    r = NoMarketingLanguage()
    v = r.check("We leverage our AI to deliver value.")
    assert len(v) >= 1


def test_no_emoji_fires():
    r = NoEmoji()
    v = r.check("Good morning \U0001F44B")
    assert len(v) == 1
    assert v[0].severity is Severity.REJECT


def test_no_emoji_silent_on_dashes_and_arrows():
    r = NoEmoji()
    # The en-dash and arrow are not emoji; do not fire.
    assert r.check("0.81 \u2192 0.54") == []
    assert r.check("Acme renewal \u2014 structurally unsafe") == []


def test_sentence_length_flags_long():
    r = SentenceLengthLimit()
    long_s = " ".join(["word"] * 40) + "."
    v = r.check(long_s)
    assert len(v) == 1
    assert v[0].severity is Severity.FLAG


def test_sentence_length_silent_on_short():
    r = SentenceLengthLimit()
    assert r.check("Short sentence. Another short one.") == []


def test_requires_specificity_rejects_generic_card_body():
    r = RequiresSpecificity()
    text = "engineering is behind and leadership is concerned about the quarter"
    v = r.check(text, RuleContext(kind="card_observation"))
    assert len(v) == 1
    assert v[0].severity is Severity.REJECT


def test_requires_specificity_passes_with_name_and_number():
    r = RequiresSpecificity()
    text = "Acme's renewal confidence dropped 0.81 to 0.54 since Sunday."
    v = r.check(text, RuleContext(kind="card_observation"))
    assert v == []


def test_requires_specificity_passes_with_cite_only():
    r = RequiresSpecificity()
    text = "Model m-2841 carried a falsifier that fired at the weekend."
    v = r.check(text, RuleContext(kind="card_observation"))
    assert v == []


def test_requires_specificity_does_not_fire_on_greeting():
    r = RequiresSpecificity()
    text = "good morning nothing consequential since yesterday"
    v = r.check(text, RuleContext(kind="greeting"))
    assert v == []


def test_no_hedge_fires_on_preamble():
    r = NoHedgePadding()
    v = r.check("I just wanted to flag the Acme situation.")
    assert len(v) >= 1


def test_no_hedge_silent_on_direct():
    r = NoHedgePadding()
    assert r.check("Acme's renewal is structurally unsafe.") == []


def test_check_all_aggregates_violations():
    text = "Exciting news! I just wanted to highlight engineering concerns."
    v = check_all(text, RuleContext(kind="greeting"))
    kinds = {x.rule for x in v}
    assert "no_exclamation_mark" in kinds
    assert "no_marketing_language" in kinds
    assert "no_hedge_padding" in kinds


def test_has_rejections_true_on_reject_severity():
    v = check_all("Exciting!", RuleContext(kind="greeting"))
    assert has_rejections(v) is True


def test_has_rejections_false_on_flag_only():
    long_s = " ".join(["word"] * 40) + "."
    v = check_all(long_s, RuleContext(kind="greeting"))
    # Only SentenceLengthLimit should fire, FLAG severity.
    assert has_rejections(v) is False


def test_strip_html_preserves_text():
    assert strip_html('<span class="n">0.81</span> \u2192 <span class="n">0.54</span>') == (
        "0.81 \u2192 0.54"
    )


def test_format_corrections_empty_on_no_violations():
    assert format_corrections([]) == ""


def test_format_corrections_names_rules():
    v = check_all("Exciting!", RuleContext(kind="greeting"))
    msg = format_corrections(v)
    assert "no_exclamation_mark" in msg or "no_marketing_language" in msg


def test_rules_tuple_is_the_full_set():
    # Sanity: every rule in the spec is in the RULES tuple.
    names = {type(r).__name__ for r in RULES}
    assert names == {
        "NoExclamationMark",
        "NoMarketingLanguage",
        "NoEmoji",
        "SentenceLengthLimit",
        "RequiresSpecificity",
        "NoHedgePadding",
    }
