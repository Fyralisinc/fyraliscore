"""Tests for services/rendering/core.py (Phase 3 exit gate).

Covers:
- Every render method returns a valid response shape.
- Voice-rule retry: a rejected first output triggers one retry with
  the correction prompt appended.
- After-two-reject: response is returned with flagged=True.
- Cost attribution: cost_usd is a Decimal and tracks accumulated usage.
- query_grid label parser robust to fences / prose / too many.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from services.rendering.contracts import (
    RenderCardRequest,
    RenderCloseLineRequest,
    RenderConversationTurnRequest,
    RenderGreetingRequest,
    RenderQueryGridRequest,
)
from services.rendering.core import (
    RenderingService,
    _parse_label_array,
    _unwrap_json_wrapped_html,
)
from services.rendering.tests.fixtures import (
    TENANT_ID,
    acme_card_focus_decision,
    acme_card_focus_observation,
    acme_query_grid_specs,
    acme_tuesday_snapshot,
    founder_rachin,
    nepal_card_focus_question,
    quiet_day_snapshot,
)
from services.rendering.tests.scripted import ScriptedProvider


# ---------------------------------------------------------------------
# Canonical clean outputs per type (voice-compliant; from the design doc)
# ---------------------------------------------------------------------

ACME_GREETING_CLEAN = (
    "Good morning. One thing is worth your attention before the day "
    "starts \u2014 Acme's renewal is <span class=\"serif\">structurally "
    "unsafe</span> as of Sunday, and revenue hasn't caught it yet. "
    "One decision is on you by Thursday. Everything else is handled."
)

ACME_OBS_CLEAN = (
    "Acme's renewal is <span class=\"serif-hot\">structurally unsafe</span>. "
    "Confidence dropped <span class=\"n\">0.81 \u2192 0.54</span> after two "
    "contracted deliverables slipped. Engineering has discussed this "
    "<span class=\"n\">11 times</span> since Friday; the revenue channel "
    "has <span class=\"hl\">zero mentions</span>. Revenue at risk: "
    "<span class=\"n\">$487K</span>."
)

ACME_DEC_CLEAN = (
    "Re-scope the Acme deliverable, or <span class=\"serif\">extend the "
    "renewal window</span>. Decide by <span class=\"n\">Thu 24 Apr</span>; "
    "<span class=\"n\">$487K</span> at stake. Drafts for both paths are ready."
)

NEPAL_Q_CLEAN = (
    "Is the DePIN goal a real bet, or is it there because letting it go "
    "would feel like giving up on Nepal?\n\n"
    "Six weeks, 0.3 FTE on g-42, no commitments, no Model movement. "
    "I can tell you're visiting it; <span class=\"hl\">I can't tell you "
    "what visiting means</span>."
)

TURN_CLEAN = (
    "Model m-2841 carried a falsifier: <em>two or more contracted "
    "deliverables slip past 15 April</em>. It fired Saturday when c-187 "
    "transitioned to Blocked and Alice re-estimated c-203 from two days to ten.\n\n"
    "Neither transition reached the CEO channel. Monica's last customer "
    "touchpoint was 04 April. Engineering Slack has <span class=\"n\">11 "
    "mentions</span> of the slip since Friday; revenue has <span class=\"hl\">"
    "zero</span>.\n\n"
    "Current confidence: <span class=\"n\">0.54</span>. Revenue at risk: "
    "<span class=\"n\">$487,000</span>."
)

CLOSE_CLEAN = "That's the signal. You can go."

GRID_CLEAN_JSON = (
    '[\n'
    '  "Show me why Acme became unsafe",\n'
    '  "What this means for Thursday\'s board update",\n'
    '  "Draft a brief for Monica",\n'
    '  "What did I miss yesterday?",\n'
    '  "Which of my beliefs are least supported?",\n'
    '  "Where is the company silent where it shouldn\'t be?"\n'
    ']'
)


def _now():
    return datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc)


# =====================================================================
# Greeting
# =====================================================================


@pytest.mark.asyncio
async def test_greeting_happy_path():
    provider = ScriptedProvider([ACME_GREETING_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_greeting(req)
    assert resp.body_html.strip() == ACME_GREETING_CLEAN
    assert resp.retried is False
    assert resp.flagged is False
    assert resp.violations == []
    assert isinstance(resp.cost_usd, Decimal)
    assert resp.cost_usd > Decimal("0")
    assert resp.rendering_model_used == "deepseek-chat"
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_greeting_quiet_day():
    quiet = (
        "Good morning. Nothing consequential since yesterday; the company "
        "is running at normal metabolism."
    )
    provider = ScriptedProvider([quiet])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        substrate_state=quiet_day_snapshot(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_greeting(req)
    assert "Nothing consequential" in resp.body_html
    assert resp.retried is False


@pytest.mark.asyncio
async def test_greeting_retry_on_exclamation_then_clean():
    bad = "Good morning! Acme is " + "<span class=\"serif\">unsafe</span>."
    provider = ScriptedProvider([bad, ACME_GREETING_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_greeting(req)
    assert resp.retried is True
    assert resp.flagged is False
    assert resp.body_html.strip() == ACME_GREETING_CLEAN
    # Retry prompt must include the correction guidance.
    assert "no_exclamation_mark" in provider.calls[1]["user"]


@pytest.mark.asyncio
async def test_greeting_flagged_after_two_rejects():
    bad1 = "Exciting news! " + "<span class=\"serif\">unsafe</span>."
    bad2 = "Amazing news! Everything on track!"
    provider = ScriptedProvider([bad1, bad2])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
    )
    resp = await svc.render_greeting(req)
    assert resp.retried is True
    assert resp.flagged is True
    assert len(resp.violations) >= 1
    # Body is still returned — caller may show with flagged indicator.
    assert resp.body_html.strip() == bad2.strip()


# =====================================================================
# Cards
# =====================================================================


@pytest.mark.asyncio
async def test_card_observation_happy_path():
    provider = ScriptedProvider([ACME_OBS_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="observation",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_observation(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_card_observation(req)
    assert "structurally unsafe" in resp.body_html
    assert resp.flagged is False
    assert resp.violations == []


@pytest.mark.asyncio
async def test_card_decision_happy_path():
    provider = ScriptedProvider([ACME_DEC_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="decision",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_decision(),
    )
    resp = await svc.render_card_decision(req)
    assert "Decide by" in resp.body_html or "Thu 24 Apr" in resp.body_html
    assert resp.flagged is False
    # Rev-2 Change 3: decision-kind outputs always carry .card-content +
    # .dec-text + .dec-chips wrappers for Agent-UI styling.
    assert 'class="card-content"' in resp.body_html
    assert 'class="dec-text"' in resp.body_html
    assert 'class="dec-chips"' in resp.body_html


@pytest.mark.asyncio
async def test_card_decision_wrapper_added_when_model_omits_it():
    """Rev-2 Change 3 enforcement: even if the LLM returns bare prose,
    the service wraps it in `.card-content` / `.dec-text` / `.dec-chips`
    using the deadline + at_stake fields from `card_focus`."""
    bare = (
        "Re-scope the Acme deliverable, or <span class=\"serif\">extend "
        "the renewal window</span>. Drafts for both paths are ready."
    )
    provider = ScriptedProvider([bare])
    svc = RenderingService(provider=provider)
    req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="decision",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_decision(),
    )
    resp = await svc.render_card_decision(req)
    assert 'class="card-content"' in resp.body_html
    assert 'class="dec-text"' in resp.body_html
    assert 'class="dec-chips"' in resp.body_html
    # The wrapper must carry the deadline + at_stake chip text.
    assert "Thu 24 Apr" in resp.body_html
    assert "$487K" in resp.body_html


@pytest.mark.asyncio
async def test_card_question_happy_path():
    provider = ScriptedProvider([NEPAL_Q_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="question",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=nepal_card_focus_question(),
    )
    resp = await svc.render_card_question(req)
    assert "DePIN" in resp.body_html or "Nepal" in resp.body_html
    assert resp.violations == []


@pytest.mark.asyncio
async def test_card_rejects_generic_body_then_retries():
    generic = "Things are behind and we should probably talk about it."
    provider = ScriptedProvider([generic, ACME_OBS_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=_now(), kind="observation",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_observation(),
    )
    resp = await svc.render_card_observation(req)
    assert resp.retried is True
    assert resp.flagged is False
    assert "structurally unsafe" in resp.body_html


# =====================================================================
# Query grid
# =====================================================================


@pytest.mark.asyncio
async def test_query_grid_happy_path():
    provider = ScriptedProvider([GRID_CLEAN_JSON])
    svc = RenderingService(provider=provider)
    req = RenderQueryGridRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        specs=acme_query_grid_specs(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_query_grid(req)
    assert len(resp.queries) == 6
    assert resp.queries[0].id == "acme-why"
    assert "Acme" in resp.queries[0].label
    assert resp.queries[0].icon == "why"
    assert resp.queries[0].hot is True
    assert resp.queries[0].tag == "urgent"
    assert resp.flagged is False


@pytest.mark.asyncio
async def test_query_grid_flags_on_short_array():
    # Model returns only 3 labels; service pads from intent and flags.
    short = '["Why Acme unsafe", "Thursday board", "Monica brief"]'
    provider = ScriptedProvider([short])
    svc = RenderingService(provider=provider)
    req = RenderQueryGridRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        specs=acme_query_grid_specs(),
    )
    resp = await svc.render_query_grid(req)
    assert len(resp.queries) == 6
    assert resp.flagged is True


def test_parse_label_array_handles_code_fences():
    raw = "```json\n[\"a\", \"b\", \"c\"]\n```"
    assert _parse_label_array(raw, expected_count=3) == ["a", "b", "c"]


def test_parse_label_array_handles_prefixed_prose():
    raw = "Sure! Here you go:\n[\"a\",\"b\"]\n"
    assert _parse_label_array(raw, expected_count=2) == ["a", "b"]


def test_parse_label_array_trims_overenthusiastic_model():
    raw = '["a","b","c","d"]'
    assert _parse_label_array(raw, expected_count=2) == ["a", "b"]


# =====================================================================
# Conversation turn + close line
# =====================================================================


@pytest.mark.asyncio
async def test_conversation_turn_happy_path():
    provider = ScriptedProvider([TURN_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderConversationTurnRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        query="Show me why Acme became unsafe.",
        retrieval_context={
            "models": [{"id": "m-2841", "conf": "0.81 -> 0.54"}],
            "commitments": [{"id": "c-187", "state": "Blocked"}],
        },
        substrate_state=acme_tuesday_snapshot(),
    )
    resp = await svc.render_conversation_turn(req)
    assert "m-2841" in resp.response_html
    assert resp.violations == []
    assert resp.flagged is False
    # Rev-2 Change 3: conversation-turn outputs always carry .t-body.
    assert 'class="t-body"' in resp.response_html


@pytest.mark.asyncio
async def test_conversation_turn_wraps_t_body_when_model_omits_it():
    """Rev-2 Change 3 enforcement: bare prose from the model is wrapped
    in `<div class="t-body">…</div>` at the service boundary."""
    bare = (
        "Model m-2841 carried a falsifier: <em>two or more contracted "
        "deliverables slip past 15 April</em>. It fired Saturday when "
        "c-187 transitioned to Blocked.\n\nCurrent confidence: "
        "<span class=\"n\">0.54</span>."
    )
    provider = ScriptedProvider([bare])
    svc = RenderingService(provider=provider)
    req = RenderConversationTurnRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        query="Show me why Acme became unsafe.",
        retrieval_context={"models": [{"id": "m-2841"}]},
        substrate_state=acme_tuesday_snapshot(),
    )
    resp = await svc.render_conversation_turn(req)
    assert 'class="t-body"' in resp.response_html
    # The wrapper should be the outermost element.
    assert resp.response_html.strip().startswith('<div class="t-body">')
    assert resp.response_html.strip().endswith('</div>')


@pytest.mark.asyncio
async def test_close_line_happy_path():
    provider = ScriptedProvider([CLOSE_CLEAN])
    svc = RenderingService(provider=provider)
    req = RenderCloseLineRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        signals_watched_count=14206, external_moves=3, calibration_pct=73,
    )
    resp = await svc.render_close_line(req)
    assert resp.body == CLOSE_CLEAN
    assert resp.metadata["signal_count"] == 14206
    assert resp.metadata["external_moves"] == 3
    assert resp.metadata["calibration_pct"] == 73


# =====================================================================
# Cost attribution
# =====================================================================


@pytest.mark.asyncio
async def test_cost_reflects_retry_usage():
    provider = ScriptedProvider([
        "Exciting! bad.",
        ACME_GREETING_CLEAN,
    ])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
    )
    resp = await svc.render_greeting(req)
    # Two calls at default 100 in + 50 out; deepseek-chat pricing.
    # Just verify cost is positive and reasonable.
    assert resp.cost_usd > Decimal("0")
    assert resp.cost_usd < Decimal("1")
    assert resp.retried is True


# =====================================================================
# Week 5 — DeepSeek JSON-wrapped HTML unwrap
# =====================================================================


def test_unwrap_json_wrapped_html_greeting():
    """The observed Week-4 wrapper shape — DeepSeek emits the prose
    under a `greeting_html` key instead of raw HTML. Post-processor
    must unwrap it."""
    wrapped = '{"greeting_html": "Good morning. Acme\'s renewal is <span class=\\"serif\\">structurally unsafe</span>."}'
    out = _unwrap_json_wrapped_html(wrapped)
    assert out == "Good morning. Acme's renewal is <span class=\"serif\">structurally unsafe</span>."
    # Idempotent on the unwrapped value.
    assert _unwrap_json_wrapped_html(out) == out


def test_unwrap_json_wrapped_html_generic_html_key():
    wrapped = '{"html":"<p>hi</p>"}'
    assert _unwrap_json_wrapped_html(wrapped) == "<p>hi</p>"


def test_unwrap_json_wrapped_html_close_line_key():
    wrapped = '{"close_line_html": "That\'s the signal. You can go."}'
    assert _unwrap_json_wrapped_html(wrapped) == "That's the signal. You can go."


def test_unwrap_json_wrapped_html_with_code_fence():
    wrapped = '```json\n{"greeting_html": "Good morning."}\n```'
    assert _unwrap_json_wrapped_html(wrapped) == "Good morning."


def test_unwrap_json_wrapped_html_passthrough_for_raw_html():
    """Correctly-formatted raw HTML should pass through untouched."""
    raw = "Good morning. One thing is <span class=\"serif\">worth</span> your attention."
    assert _unwrap_json_wrapped_html(raw) == raw


def test_unwrap_json_wrapped_html_passthrough_multi_key():
    """Multi-key objects are NOT unwrapped — could be genuine structure."""
    raw = '{"greeting_html": "hi", "extra": "metadata"}'
    assert _unwrap_json_wrapped_html(raw) == raw


def test_unwrap_json_wrapped_html_passthrough_unknown_key():
    """Objects with keys that are neither in the allowlist nor `*_html`
    are left alone. Voice rules will flag if prose is not HTML."""
    raw = '{"some_other_field": "value"}'
    assert _unwrap_json_wrapped_html(raw) == raw


def test_unwrap_json_wrapped_html_passthrough_on_parse_fail():
    raw = 'Not JSON {missing quotes'
    assert _unwrap_json_wrapped_html(raw) == raw


def test_unwrap_json_wrapped_html_handles_empty():
    assert _unwrap_json_wrapped_html("") == ""


@pytest.mark.asyncio
async def test_greeting_unwraps_deepseek_json_wrap_in_pipeline():
    """End-to-end: a provider that returns `{"greeting_html":"..."}`
    must produce clean HTML in the response, not JSON braces."""
    wrapped = (
        '{"greeting_html": "Good morning. One thing is worth your attention '
        "before the day starts \u2014 Acme's renewal is "
        '<span class=\\"serif\\">structurally unsafe</span> as of Sunday, and '
        "revenue hasn't caught it yet. One decision is on you by Thursday. "
        'Everything else is handled."}'
    )
    provider = ScriptedProvider([wrapped])
    svc = RenderingService(provider=provider)
    req = RenderGreetingRequest(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    resp = await svc.render_greeting(req)
    # The response must NOT contain the JSON wrapper braces or key.
    assert not resp.body_html.strip().startswith("{")
    assert "greeting_html" not in resp.body_html
    # The unwrapped prose must be present.
    assert "structurally unsafe" in resp.body_html
    assert resp.retried is False  # No voice-rule rejection on the unwrapped prose.


# =====================================================================
# Week 5 — concurrent cost attribution (ContextVar aggregator)
# =====================================================================


@pytest.mark.asyncio
async def test_concurrent_renders_each_record_their_own_cost():
    """Under concurrent rendering on a shared provider, each caller's
    cost_usd must be non-zero — regression test for the Week-4 leak
    where the instance-wide `_usage_aggregator` was overwritten by
    sibling tasks, producing `$0.00` rows for greeting / query_grid /
    close_line while card_observation kept winning the race.
    """
    from services.rendering.contracts import (
        RenderCardRequest as _RC,
        RenderGreetingRequest as _RG,
        RenderCloseLineRequest as _RCL,
    )

    # One shared provider + service; multiple concurrent render calls.
    provider = ScriptedProvider(
        [ACME_GREETING_CLEAN, ACME_OBS_CLEAN, CLOSE_CLEAN],
    )
    svc = RenderingService(provider=provider)
    greeting_req = _RG(
        tenant_id=TENANT_ID, timestamp=_now(),
        substrate_state=acme_tuesday_snapshot(),
        founder_context=founder_rachin(),
    )
    card_req = _RC(
        tenant_id=TENANT_ID, timestamp=_now(), kind="observation",
        substrate_state=acme_tuesday_snapshot(),
        card_focus=acme_card_focus_observation(),
        founder_context=founder_rachin(),
    )
    close_req = _RCL(
        tenant_id=TENANT_ID, timestamp=_now(),
        signals_watched_count=14206, external_moves=3, calibration_pct=73,
    )

    results = await asyncio.gather(
        svc.render_greeting(greeting_req),
        svc.render_card_observation(card_req),
        svc.render_close_line(close_req),
    )
    # All three must have recorded non-zero cost independently.
    for r in results:
        assert r.cost_usd > Decimal("0"), f"cost was zero for {type(r).__name__}"
