"""Integration tests for services/rendering/api.py (Phase 4 exit gate).

Uses FastAPI TestClient + a dependency override to inject a
ScriptedProvider-backed RenderingService. Every endpoint tested.
"""
from __future__ import annotations


from fastapi.testclient import TestClient

from services.rendering.api import create_app
from services.rendering.core import RenderingService
from services.rendering.tests.fixtures import (
    acme_card_focus_decision,
    acme_card_focus_observation,
    nepal_card_focus_question,
)
from services.rendering.tests.scripted import ScriptedProvider


TENANT = "00000000-0000-0000-0000-0000000000a1"
NOW_ISO = "2026-04-21T06:42:00+00:00"


def _snapshot_wire(*, quiet: bool = False) -> dict:
    """Minimal valid substrate wire payload — matches SubstrateSnapshotIn."""
    base = {
        "tenant_id": TENANT,
        "captured_at": NOW_ISO,
        "top_models": [] if quiet else [
            {
                "id": "m-2841",
                "claim": "Acme renews Q3",
                "confidence": 0.54,
                "prior_confidence": 0.81,
                "state_changed_at": "2026-04-19T03:12:00+00:00",
                "falsifier": "two contracted deliverables slip past 15 Apr",
            }
        ],
        "active_commitments": [] if quiet else [
            {"id": "c-187", "label": "rate-limiter", "state": "Blocked", "pressure": "high"}
        ],
        "customer_resources": [
            {"id": "r-cust-acme", "kind": "customer", "name": "Acme",
             "health": "healthy" if quiet else "warning",
             "revenue_at_risk": None if quiet else "$487K"},
        ],
        "recent_state_changes": [],
        "anomalies": [] if quiet else [
            {"id": "an-1", "kind": "silence", "description": "revenue silent", "severity": "high"}
        ],
        "conversation_context": {"was_here_recently": False, "last_queries": []},
        "time_of_day_bucket": "morning",
        "signals_watched_count": 14206 if not quiet else 2011,
    }
    return base


def _client_with_scripted(*responses: str) -> TestClient:
    provider = ScriptedProvider(list(responses))
    svc = RenderingService(provider=provider)
    app = create_app(service=svc)
    return TestClient(app)


def test_greeting_endpoint_ok():
    canned = (
        "Good morning. One thing \u2014 Acme's renewal is "
        "<span class=\"serif\">structurally unsafe</span>. "
        "Decide by <span class=\"n\">Thu 24 Apr</span>. Everything else is handled."
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/greeting",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "substrate_state": _snapshot_wire(),
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "structurally unsafe" in body["body_html"]
    assert body["meta"]["signals_watched_count"] == 14206
    assert body["rendering_model_used"] == "deepseek-chat"
    assert body["flagged"] is False
    assert body["retried"] is False


def test_card_observation_endpoint_ok():
    canned = (
        "Acme's renewal is <span class=\"serif-hot\">structurally unsafe</span>. "
        "Confidence <span class=\"n\">0.81 \u2192 0.54</span>. Revenue at risk: "
        "<span class=\"n\">$487K</span>."
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/card",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "kind": "observation",
            "substrate_state": _snapshot_wire(),
            "card_focus": acme_card_focus_observation(),
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "serif-hot" in body["body_html"]


def test_card_decision_endpoint_ok():
    canned = (
        "Re-scope Acme, or <span class=\"serif\">extend the renewal window</span>. "
        "Decide by <span class=\"n\">Thu 24 Apr</span>; "
        "<span class=\"n\">$487K</span> at stake."
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/card",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "kind": "decision",
            "substrate_state": _snapshot_wire(),
            "card_focus": acme_card_focus_decision(),
        },
    )
    assert r.status_code == 200, r.text


def test_card_question_endpoint_ok():
    canned = (
        "Is the DePIN goal a real bet, or is it there because letting it go "
        "would feel like giving up on Nepal?\n\n"
        "Six weeks, 0.3 FTE on g-42, no Commitments, no Model movement. "
        "<span class=\"hl\">I can't tell you what visiting means</span>."
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/card",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "kind": "question",
            "substrate_state": _snapshot_wire(),
            "card_focus": nepal_card_focus_question(),
        },
    )
    assert r.status_code == 200, r.text


def test_query_grid_endpoint_ok():
    canned = (
        '["Show me why Acme became unsafe",'
        ' "What this means for Thursday\'s board update",'
        ' "Draft a brief for Monica",'
        ' "What did I miss yesterday?",'
        ' "Which of my beliefs are least supported?",'
        ' "Where is the company silent where it shouldn\'t be?"]'
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/query-grid",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "substrate_state": _snapshot_wire(),
            "specs": [
                {"id": "acme-why", "icon": "why", "hot": True, "tag": "urgent", "intent": "why acme unsafe"},
                {"id": "acme-board", "icon": "brief", "hot": True, "tag": "relevant", "intent": "thursday board"},
                {"id": "monica", "icon": "draft", "hot": False, "tag": "2min", "intent": "brief monica"},
                {"id": "miss", "icon": "timeline", "hot": False, "tag": None, "intent": "missed yesterday"},
                {"id": "beliefs", "icon": "calibration", "hot": False, "tag": None, "intent": "least supported beliefs"},
                {"id": "silent", "icon": "observation", "hot": False, "tag": None, "intent": "silence"},
            ],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["queries"]) == 6
    assert body["queries"][0]["id"] == "acme-why"
    assert "Acme" in body["queries"][0]["label"]


def test_conversation_turn_endpoint_ok():
    canned = (
        "Model m-2841 carried a falsifier: <em>two contracted deliverables "
        "slip past 15 April</em>. It fired Saturday.\n\n"
        "Current confidence: <span class=\"n\">0.54</span>. Revenue at risk: "
        "<span class=\"n\">$487,000</span>."
    )
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/conversation-turn",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "query": "Show me why Acme became unsafe.",
            "retrieval_context": {"models": [{"id": "m-2841"}]},
            "substrate_state": _snapshot_wire(),
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "m-2841" in body["response_html"]


def test_close_line_endpoint_ok():
    canned = "That's the signal. You can go."
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/close-line",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "signals_watched_count": 14206,
            "external_moves": 3,
            "calibration_pct": 73,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["body"] == "That's the signal. You can go."
    assert body["metadata"]["calibration_pct"] == 73


def test_unknown_card_kind_rejected_by_pydantic():
    canned = "anything"
    client = _client_with_scripted(canned)
    r = client.post(
        "/rendering/card",
        json={
            "tenant_id": TENANT,
            "timestamp": NOW_ISO,
            "kind": "observation-mistyped",
            "substrate_state": _snapshot_wire(),
        },
    )
    assert r.status_code == 422
