"""Tests for RenderingService.render_card_reasoning + the
POST /rendering/card-reasoning endpoint (Gate 4b fix).

Shape validators:
  - Response carries `reasoning_html` and `evidence[]`.
  - At least one evidence entry's body_html contains `class="cite"`.
  - reasoning_html contains a `.serif` span (the voice hook).
  - cost_usd is a positive Decimal.
  - Retry path: a first output with no `.cite` triggers a retry; the
    follow-up output's evidence supplies the cite span and lands.
  - Parse failure: malformed JSON triggers a retry with the correction
    prompt.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from services.rendering.api import create_app
from services.rendering.contracts import (
    EvidenceRef,
    RenderCardReasoningRequest,
)
from services.rendering.core import (
    RenderingService,
    _has_cite_span_anywhere,
    _parse_reasoning_payload,
)
from services.rendering.tests.fixtures import (
    TENANT_ID,
    acme_tuesday_snapshot,
    founder_rachin,
)
from services.rendering.tests.scripted import ScriptedProvider


_CLEAN_PAYLOAD = {
    "reasoning_html": (
        "Model <b>m-2841</b> carried a falsifier: "
        "<span class=\"note\">two contracted deliverables slip past 15 April</span>. "
        "It fired Saturday when <b>c-187</b> transitioned to "
        "<span class=\"serif\">Blocked</span>; revenue has "
        "<span class=\"hl\">zero mentions</span> of the "
        "<span class=\"n\">$487K</span> at risk."
    ),
    "evidence": [
        {
            "label": "linear webhook \u2014 Sat 19:03",
            "body_html": (
                "c-187 transitioned to <span class=\"cite\">Blocked "
                "\u2014 Sat 19:03</span>; rate-limiter SLA missed."
            ),
        },
        {
            "label": "Alice \u2014 Sat 22:41",
            "body_html": (
                "Alice re-estimated c-203 from <span class=\"n\">2d</span> "
                "to <span class=\"n\">10d</span> "
                "<span class=\"cite\">Alice \u2014 Sat 22:41</span>. "
                "<span class=\"note\">Not escalated.</span>"
            ),
        },
    ],
}


def _now() -> datetime:
    return datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc)


def _acme_evidence() -> list[EvidenceRef]:
    return [
        EvidenceRef(
            actor="linear webhook", channel="linear",
            t=datetime(2026, 4, 18, 19, 3, tzinfo=timezone.utc),
            excerpt="c-187 InProgress \u2192 Blocked",
            cite_id="obs-88412", kind="state_change",
        ),
        EvidenceRef(
            actor="Alice", channel="slack_eng",
            t=datetime(2026, 4, 18, 22, 41, tzinfo=timezone.utc),
            excerpt="re-estimates c-203 from 2d to 10d",
            cite_id="obs-88430", kind="slack",
        ),
        EvidenceRef(
            actor="system", channel="think",
            t=datetime(2026, 4, 19, 3, 12, tzinfo=timezone.utc),
            excerpt="m-2841 0.81 \u2192 0.54; falsifier fired",
            cite_id="m-2841", kind="update",
        ),
    ]


def _request() -> RenderCardReasoningRequest:
    return RenderCardReasoningRequest(
        tenant_id=TENANT_ID,
        timestamp=_now(),
        card_kind="observation",
        card_subject="Acme renewal",
        card_body_context=(
            "Acme\u2019s renewal is <span class=\"serif-hot\">structurally "
            "unsafe</span>. Confidence dropped 0.81 \u2192 0.54."
        ),
        substrate_state=acme_tuesday_snapshot(),
        supporting_evidence=_acme_evidence(),
        founder_context=founder_rachin(),
    )


# ---------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------


def test_parse_reasoning_payload_plain():
    raw = json.dumps(_CLEAN_PAYLOAD)
    parsed = _parse_reasoning_payload(raw)
    assert parsed is not None
    assert "reasoning_html" in parsed
    assert isinstance(parsed["evidence"], list)


def test_parse_reasoning_payload_fenced():
    raw = "```json\n" + json.dumps(_CLEAN_PAYLOAD) + "\n```"
    parsed = _parse_reasoning_payload(raw)
    assert parsed is not None
    assert parsed["reasoning_html"] == _CLEAN_PAYLOAD["reasoning_html"]


def test_parse_reasoning_payload_with_prose_prefix():
    raw = "Here is the output:\n" + json.dumps(_CLEAN_PAYLOAD)
    parsed = _parse_reasoning_payload(raw)
    assert parsed is not None


def test_parse_reasoning_payload_bad_returns_none():
    assert _parse_reasoning_payload("not JSON at all") is None
    assert _parse_reasoning_payload("") is None
    assert _parse_reasoning_payload("{\"foo\":1}") is None  # missing reasoning_html


def test_has_cite_span_detects_both_quote_styles():
    class _E:
        def __init__(self, body):
            self.body_html = body
    assert _has_cite_span_anywhere([_E('<span class="cite">X</span>')]) is True
    assert _has_cite_span_anywhere([_E("<span class='cite'>X</span>")]) is True
    assert _has_cite_span_anywhere([_E('plain text')]) is False
    assert _has_cite_span_anywhere([]) is False


# ---------------------------------------------------------------------
# Service-level
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_render_card_reasoning_happy_path():
    provider = ScriptedProvider([json.dumps(_CLEAN_PAYLOAD)])
    svc = RenderingService(provider=provider)
    resp = await svc.render_card_reasoning(_request())

    assert resp.retried is False
    assert resp.flagged is False
    assert resp.violations == []
    # Reasoning + evidence shape
    assert resp.reasoning_html.strip()
    assert "<span class=\"serif\">" in resp.reasoning_html
    assert len(resp.evidence) == 2
    # At least one cite span in evidence bodies
    assert any('class="cite"' in e.body_html for e in resp.evidence)
    # Cost attribution
    assert isinstance(resp.cost_usd, Decimal)
    assert resp.cost_usd > Decimal("0")
    # Model name
    assert resp.rendering_model_used == "deepseek-chat"
    # One LLM call, no retry.
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_render_card_reasoning_cite_missing_triggers_retry():
    """A first output missing `.cite` should trigger exactly one retry
    with the cite-correction prompt, and the follow-up output lands."""
    no_cite = {
        "reasoning_html": (
            "Model <b>m-2841</b> shifted. "
            "Confidence <span class=\"n\">0.81 \u2192 0.54</span>. "
            "Acme is <span class=\"serif\">at risk</span>."
        ),
        "evidence": [
            {
                "label": "Alice \u2014 Sat 22:41",
                "body_html": (
                    "Alice re-estimated c-203 from 2d to 10d."
                ),  # no cite span
            },
        ],
    }
    good = {
        "reasoning_html": no_cite["reasoning_html"],
        "evidence": [
            {
                "label": "Alice \u2014 Sat 22:41",
                "body_html": (
                    "Alice re-estimated c-203 "
                    "<span class=\"cite\">Alice \u2014 Sat 22:41</span>."
                ),
            },
        ],
    }
    provider = ScriptedProvider([json.dumps(no_cite), json.dumps(good)])
    svc = RenderingService(provider=provider)
    resp = await svc.render_card_reasoning(_request())
    assert resp.retried is True
    assert any('class="cite"' in e.body_html for e in resp.evidence)
    # The correction prompt is appended on the retry user message.
    assert len(provider.calls) == 2
    assert "span class=\"cite\"" in provider.calls[1]["user"]


@pytest.mark.asyncio
async def test_render_card_reasoning_parse_failure_retries():
    """Malformed first output → retry with the JSON-shape correction."""
    provider = ScriptedProvider([
        "this is not JSON; I forgot the format",
        json.dumps(_CLEAN_PAYLOAD),
    ])
    svc = RenderingService(provider=provider)
    resp = await svc.render_card_reasoning(_request())
    assert resp.retried is True
    assert resp.reasoning_html.strip()
    assert len(resp.evidence) == 2
    assert len(provider.calls) == 2


@pytest.mark.asyncio
async def test_render_card_reasoning_parse_failure_twice_flagged():
    provider = ScriptedProvider(["not json", "still not json"])
    svc = RenderingService(provider=provider)
    resp = await svc.render_card_reasoning(_request())
    assert resp.flagged is True
    # Reasoning lands with the raw text stripped, evidence empty.
    assert resp.evidence == []


@pytest.mark.asyncio
async def test_render_card_reasoning_cost_is_positive_decimal():
    provider = ScriptedProvider([json.dumps(_CLEAN_PAYLOAD)])
    svc = RenderingService(provider=provider)
    resp = await svc.render_card_reasoning(_request())
    assert isinstance(resp.cost_usd, Decimal)
    assert resp.cost_usd > Decimal("0")


# ---------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------


def test_card_reasoning_endpoint_ok():
    provider = ScriptedProvider([json.dumps(_CLEAN_PAYLOAD)])
    svc = RenderingService(provider=provider)
    client = TestClient(create_app(service=svc))

    r = client.post(
        "/rendering/card-reasoning",
        json={
            "tenant_id": str(TENANT_ID),
            "timestamp": _now().isoformat(),
            "card_kind": "observation",
            "card_subject": "Acme renewal",
            "card_body_context": "Acme's renewal is at risk.",
            "substrate_state": {
                "tenant_id": str(TENANT_ID),
                "captured_at": _now().isoformat(),
                "top_models": [],
                "active_commitments": [],
                "customer_resources": [],
                "recent_state_changes": [],
                "anomalies": [],
                "conversation_context": {"was_here_recently": False, "last_queries": []},
                "time_of_day_bucket": "morning",
                "signals_watched_count": 14206,
            },
            "supporting_evidence": [
                {
                    "actor": "Alice",
                    "channel": "slack_eng",
                    "t": "2026-04-18T22:41:00+00:00",
                    "excerpt": "re-estimates c-203",
                    "cite_id": "obs-88430",
                    "kind": "slack",
                },
            ],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "reasoning_html" in body
    assert isinstance(body["evidence"], list) and len(body["evidence"]) >= 1
    # Cite span in at least one evidence body.
    assert any('class="cite"' in e["body_html"] for e in body["evidence"])
    # Serif span in reasoning.
    assert "class=\"serif\"" in body["reasoning_html"] or "class='serif'" in body["reasoning_html"]
    # Cost emitted.
    assert float(body["cost_usd"]) > 0.0
