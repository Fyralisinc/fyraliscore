"""Phase-5 stub: run every render method against the Acme Tuesday
snapshot, capture voice-compliant outputs, and verify zero REJECT-
severity violations.

Per the Agent-RND brief:
  > For now, stub a sample SubstrateSnapshot from the design doc's
  > Acme Tuesday state and verify each endpoint produces design-doc-
  > quality output.

This test pins the canonical captures. Real voice calibration against
live LLM output is deferred until Agent-SIM's acme_tuesday scenario
and Agent-GRT's snapshot composer are in place. The output here is
what the BUILD-LOG references.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.rendering.contracts import (
    RenderCardRequest,
    RenderCloseLineRequest,
    RenderConversationTurnRequest,
    RenderGreetingRequest,
    RenderQueryGridRequest,
)
from services.rendering.core import RenderingService
from services.rendering.tests.fixtures import (
    TENANT_ID,
    acme_card_focus_decision,
    acme_card_focus_observation,
    acme_query_grid_specs,
    acme_tuesday_snapshot,
    founder_rachin,
    nepal_card_focus_question,
)
from services.rendering.tests.scripted import ScriptedProvider
from services.rendering.voice_rules import RuleContext, check_all, has_rejections


GREETING_OUT = (
    "Good morning. One thing is worth your attention before the day "
    "starts \u2014 Acme's renewal is <span class=\"serif\">structurally "
    "unsafe</span> as of Sunday, and revenue hasn't caught it yet. "
    "One decision is on you by Thursday. Everything else is handled."
)

OBS_OUT = (
    "Acme's renewal is <span class=\"serif-hot\">structurally unsafe</span>. "
    "Confidence dropped <span class=\"n\">0.81 \u2192 0.54</span> after two "
    "contracted deliverables slipped. Engineering has discussed this "
    "<span class=\"n\">11 times</span> since Friday; the revenue channel "
    "has <span class=\"hl\">zero mentions</span>. Revenue at risk: "
    "<span class=\"n\">$487K</span>."
)

DEC_OUT = (
    "Re-scope the Acme deliverable, or <span class=\"serif\">extend the "
    "renewal window</span>. Decide by <span class=\"n\">Thu 24 Apr</span>; "
    "<span class=\"n\">$487K</span> at stake. Drafts for both paths are ready."
)

QUE_OUT = (
    "Is the DePIN goal a real bet, or is it there because letting it go "
    "would feel like giving up on Nepal?\n\n"
    "Six weeks, 0.3 FTE on g-42, no commitments, no Model movement. "
    "I can tell you're visiting it; <span class=\"hl\">I can't tell you "
    "what visiting means</span>."
)

GRID_OUT = json.dumps([
    "Show me why Acme became unsafe",
    "What this means for Thursday's board update",
    "Draft a brief for Monica",
    "What did I miss yesterday?",
    "Which of my beliefs are least supported?",
    "Where is the company silent where it shouldn't be?",
])

TURN_OUT = (
    "Model m-2841 carried a falsifier: <em>two or more contracted "
    "deliverables slip past 15 April</em>. It fired Saturday when c-187 "
    "transitioned to Blocked and Alice re-estimated c-203 from two days "
    "to ten.\n\n"
    "Neither transition reached the CEO channel. Monica's last customer "
    "touchpoint was 04 April; she has not been contested on her \u201Crock "
    "solid\u201D read. Engineering Slack has <span class=\"n\">11 "
    "mentions</span> of the slip since Friday; revenue has <span class=\"hl\">"
    "zero</span>.\n\nCurrent confidence: <span class=\"n\">0.54</span>. "
    "Revenue at risk, computed structurally through the customer_commitments "
    "spine: <span class=\"n\">$487,000</span>."
)

CLOSE_OUT = "That's the signal. You can go."


def _now():
    return datetime(2026, 4, 21, 6, 42, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_acme_tuesday_captures_are_voice_compliant(capsys):
    """Render all 7 types against the Acme Tuesday snapshot. Verify no
    REJECT-severity voice violations. Capture outputs for BUILD-LOG.
    """
    provider = ScriptedProvider([
        GREETING_OUT,   # greeting
        OBS_OUT,        # observation
        DEC_OUT,        # decision
        QUE_OUT,        # question
        GRID_OUT,       # query grid
        TURN_OUT,       # conversation turn
        CLOSE_OUT,      # close line
    ])
    svc = RenderingService(provider=provider)
    snap = acme_tuesday_snapshot()
    founder = founder_rachin()
    now = _now()

    # Greeting
    g = await svc.render_greeting(RenderGreetingRequest(
        tenant_id=TENANT_ID, timestamp=now, substrate_state=snap,
        founder_context=founder,
    ))
    # Observation
    o = await svc.render_card_observation(RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=now, kind="observation",
        substrate_state=snap, card_focus=acme_card_focus_observation(),
        founder_context=founder,
    ))
    # Decision
    d = await svc.render_card_decision(RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=now, kind="decision",
        substrate_state=snap, card_focus=acme_card_focus_decision(),
        founder_context=founder,
    ))
    # Question
    q = await svc.render_card_question(RenderCardRequest(
        tenant_id=TENANT_ID, timestamp=now, kind="question",
        substrate_state=snap, card_focus=nepal_card_focus_question(),
        founder_context=founder,
    ))
    # Query grid
    grid = await svc.render_query_grid(RenderQueryGridRequest(
        tenant_id=TENANT_ID, timestamp=now, substrate_state=snap,
        specs=acme_query_grid_specs(), founder_context=founder,
    ))
    # Conversation turn
    t = await svc.render_conversation_turn(RenderConversationTurnRequest(
        tenant_id=TENANT_ID, timestamp=now,
        query="Show me why Acme became unsafe.",
        retrieval_context={"models": [{"id": "m-2841"}]},
        substrate_state=snap, founder_context=founder,
    ))
    # Close line
    c = await svc.render_close_line(RenderCloseLineRequest(
        tenant_id=TENANT_ID, timestamp=now,
        signals_watched_count=14206, external_moves=3, calibration_pct=73,
    ))

    # No REJECT-severity violations anywhere.
    for label, resp in [
        ("greeting", g), ("observation", o), ("decision", d), ("question", q),
    ]:
        assert not has_rejections(
            check_all(
                getattr(resp, "body_html"),
                RuleContext(kind=f"card_{label}" if label != "greeting" else "greeting"),
            )
        ), f"{label} has reject-severity violations"
    assert not has_rejections(
        check_all(t.response_html, RuleContext(kind="conversation_turn"))
    )
    assert not has_rejections(check_all(c.body, RuleContext(kind="close_line")))
    # Grid labels are plain text; check each.
    for chip in grid.queries:
        assert not has_rejections(
            check_all(chip.label, RuleContext(kind="query_grid_item"))
        )

    # Capture block for BUILD-LOG — print visible to `-s`.
    lines = [
        "=== AGENT-RND PHASE-5 SAMPLE CAPTURES (acme_tuesday) ===",
        f"GREETING:\n{g.body_html}",
        f"OBSERVATION CARD:\n{o.body_html}",
        f"DECISION CARD:\n{d.body_html}",
        f"QUESTION CARD:\n{q.body_html}",
        "QUERY GRID:",
    ]
    for chip in grid.queries:
        lines.append(
            f"  - [{chip.icon}{' hot' if chip.hot else ''}"
            f"{' ' + chip.tag if chip.tag else ''}] {chip.label}"
        )
    lines.append(f"CONVERSATION TURN:\n{t.response_html}")
    lines.append(f"CLOSE LINE:\n{c.body}")
    lines.append("=== END CAPTURES ===")
    print("\n".join(lines))

    # Write to a deterministic artifact file as well so the BUILD-LOG
    # entry can reference it without re-running the suite.
    artifact = Path(__file__).parent / "phase5_captures.txt"
    artifact.write_text("\n".join(lines), encoding="utf-8")
