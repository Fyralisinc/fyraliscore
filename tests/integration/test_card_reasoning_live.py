"""tests/integration/test_card_reasoning_live.py

Gate 4b fix — coverage for RND's new `/rendering/card-reasoning`
endpoint and the GRT adapter that consumes it.

Two modes:

* **Ungated (default, always runs)** — drives a scripted provider
  through the in-process rendering FastAPI app + the HTTP GRT adapter
  and asserts the wire shape + fallback path. No external deps.

* **Gated live** — set `RENDER_CARD_REASONING_LIVE=1` to hit the live
  DeepSeek provider via the same gateway stack. Asserts the returned
  `cards[].expanded.reasoning_html` carries voice hooks (`.serif` /
  `.hl`) and at least one `.cite` span inside an evidence entry, with
  length > 80 chars and no voice-rule reject-level violations.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi.testclient import TestClient

from services.rendering.api import create_app
from services.rendering.core import RenderingService
from services.rendering.tests.scripted import ScriptedProvider
from services.rendering.voice_rules import RuleContext, check_all, has_rejections

ROOT = Path(__file__).resolve().parent.parent.parent
CAPTURES_DIR = ROOT / "tests" / "integration" / "captures"
DOGFOOD_TENANT = UUID("00000000-0000-7000-8000-000000000dd1")


_CLEAN_PAYLOAD = {
    "reasoning_html": (
        "Model <b>m-2841</b> carried a falsifier: "
        "<span class=\"note\">two contracted deliverables slip past 15 April</span>. "
        "That fired Saturday when <b>c-187</b> transitioned to "
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


# ---------------------------------------------------------------------
# Ungated — in-process RND app + GRT HTTP adapter
# ---------------------------------------------------------------------


def _snapshot_wire() -> dict[str, Any]:
    now = datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc).isoformat()
    return {
        "tenant_id": str(DOGFOOD_TENANT),
        "captured_at": now,
        "top_models": [
            {
                "id": "m-2841",
                "claim": "Acme renews Q3",
                "confidence": 0.54,
                "prior_confidence": 0.81,
            }
        ],
        "active_commitments": [],
        "customer_resources": [
            {
                "id": "r-cust-acme",
                "kind": "customer",
                "name": "Acme",
                "health": "warning",
                "revenue_at_risk": "$487K",
            }
        ],
        "recent_state_changes": [],
        "anomalies": [
            {
                "id": "an-1",
                "kind": "silence",
                "description": "revenue silent",
                "severity": "high",
            }
        ],
        "conversation_context": {"was_here_recently": False, "last_queries": []},
        "time_of_day_bucket": "morning",
        "signals_watched_count": 14206,
    }


def test_card_reasoning_endpoint_wire_shape() -> None:
    """RND's /rendering/card-reasoning: valid JSON in, scripted LLM out,
    the response carries `reasoning_html` + `evidence[]` with a `.cite`
    span somewhere."""
    provider = ScriptedProvider([json.dumps(_CLEAN_PAYLOAD)])
    svc = RenderingService(provider=provider)
    app = create_app(service=svc)
    client = TestClient(app)

    now = datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc).isoformat()
    r = client.post(
        "/rendering/card-reasoning",
        json={
            "tenant_id": str(DOGFOOD_TENANT),
            "timestamp": now,
            "card_kind": "observation",
            "card_subject": "Acme renewal",
            "card_body_context": (
                "Acme's renewal is structurally unsafe. "
                "Confidence 0.81 \u2192 0.54."
            ),
            "substrate_state": _snapshot_wire(),
            "supporting_evidence": [
                {
                    "actor": "Alice",
                    "channel": "slack_eng",
                    "t": "2026-04-18T22:41:00+00:00",
                    "excerpt": "re-estimates c-203 from 2d to 10d",
                    "cite_id": "obs-88430",
                    "kind": "slack",
                }
            ],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Shape invariants
    assert "reasoning_html" in body and isinstance(body["reasoning_html"], str)
    assert body["reasoning_html"].strip()
    assert isinstance(body["evidence"], list)
    assert len(body["evidence"]) >= 1
    for e in body["evidence"]:
        assert {"label", "body_html"} <= set(e.keys())
    # Cite span on at least one evidence entry.
    assert any('class="cite"' in e["body_html"] for e in body["evidence"])
    # Voice hook: .serif present.
    assert "class=\"serif\"" in body["reasoning_html"]
    # Cost attribution.
    assert float(body["cost_usd"]) > 0.0


@pytest.mark.asyncio
async def test_card_reasoning_http_adapter_fallback_on_rnd_outage() -> None:
    """GRT's HttpRenderingAdapter should fall back to the placeholder
    synthesis when the rendering endpoint errors, never surfacing a
    crash to `GET /view/ceo/home`."""
    from services.greeting.rendering_adapter import HttpRenderingAdapter
    from services.greeting.snapshot import (
        ConversationContext,
        FounderContext,
        SubstrateSnapshot,
    )

    # Point the adapter at an invalid port to guarantee ConnectionError.
    adapter = HttpRenderingAdapter("http://127.0.0.1:1", timeout_s=0.5)

    snap = SubstrateSnapshot(
        tenant_id=DOGFOOD_TENANT,
        captured_at=datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc),
        top_models=[],
        active_commitments=[],
        customer_resources=[],
        recent_state_changes=[],
        anomalies=[],
        conversation_context=ConversationContext(),
        time_of_day_bucket="morning",
    )
    founder = FounderContext(
        tenant_id=DOGFOOD_TENANT,
        role="ceo",
        display_name="Rachin",
        timezone_name="Asia/Kathmandu",
    )

    result = await adapter.render_card_reasoning(
        snap, founder, "observation",
        card_subject="Acme renewal",
        card_body_context="Acme's renewal is structurally unsafe.",
        supporting_evidence=[
            {
                "actor": "Alice",
                "t": datetime(2026, 4, 18, 22, 41, tzinfo=timezone.utc),
                "excerpt": "re-estimates c-203",
                "cite_id": "obs-88430",
                "kind": "slack",
            },
        ],
    )
    assert result.fallback is True
    # Fallback still produces a usable shape with a .cite span.
    assert result.reasoning_html
    assert result.evidence
    assert any('class="cite"' in e["body_html"] for e in result.evidence)


# ---------------------------------------------------------------------
# Gated live — hits the live DeepSeek provider.
# ---------------------------------------------------------------------


pytestmark_live = pytest.mark.skipif(
    os.environ.get("RENDER_CARD_REASONING_LIVE") != "1",
    reason=(
        "set RENDER_CARD_REASONING_LIVE=1 to run the live DeepSeek "
        "card-reasoning integration test"
    ),
)


@pytest.mark.asyncio
@pytestmark_live
async def test_card_reasoning_live_endpoint() -> None:
    """Live exercise of POST /rendering/card-reasoning: hits DeepSeek
    through the rendering service, asserts voice-compliant output with
    a `.cite` span in at least one evidence entry."""
    from services.rendering.core import RenderingService

    svc = RenderingService.from_env()
    app = create_app(service=svc)

    now = datetime.now(timezone.utc)
    payload = {
        "tenant_id": str(DOGFOOD_TENANT),
        "timestamp": now.isoformat(),
        "card_kind": "observation",
        "card_subject": "Acme renewal",
        "card_body_context": (
            "Acme\u2019s renewal is <span class=\"serif-hot\">structurally "
            "unsafe</span>. Confidence dropped <span class=\"n\">0.81 "
            "\u2192 0.54</span>."
        ),
        "substrate_state": _snapshot_wire(),
        "supporting_evidence": [
            {
                "actor": "linear webhook",
                "channel": "linear",
                "t": "2026-04-18T19:03:00+00:00",
                "excerpt": "c-187 InProgress \u2192 Blocked",
                "cite_id": "obs-88412",
                "kind": "state_change",
            },
            {
                "actor": "Alice",
                "channel": "slack_eng",
                "t": "2026-04-18T22:41:00+00:00",
                "excerpt": "re-estimates c-203 from 2d to 10d",
                "cite_id": "obs-88430",
                "kind": "slack",
            },
            {
                "actor": "system",
                "channel": "think",
                "t": "2026-04-19T03:12:00+00:00",
                "excerpt": "m-2841 0.81 \u2192 0.54; falsifier fired",
                "cite_id": "m-2841",
                "kind": "update",
            },
        ],
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://rnd", timeout=120
    ) as client:
        r = await client.post("/rendering/card-reasoning", json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    reasoning = body["reasoning_html"]
    evidence = body["evidence"]

    assert isinstance(reasoning, str) and len(reasoning) > 80, reasoning
    # Voice hook: at least one .serif or .hl span.
    assert ("class=\"serif\"" in reasoning) or ("class=\"hl\"" in reasoning), reasoning
    # Cite on at least one evidence entry.
    assert any('class="cite"' in e["body_html"] for e in evidence), evidence
    # Voice rules: no rejects on the reasoning prose.
    violations = check_all(reasoning, RuleContext(kind="card_reasoning"))
    assert not has_rejections(violations), violations

    # Dump capture for human review / BUILD-LOG citation.
    CAPTURES_DIR.mkdir(parents=True, exist_ok=True)
    (CAPTURES_DIR / "card_reasoning_live_capture.json").write_text(
        json.dumps(body, indent=2, ensure_ascii=False)
    )
