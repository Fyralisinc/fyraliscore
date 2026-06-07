"""Integration tests for the Resolution Tracker backend."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest

from services.decision_deltas.tests.conftest import seed_decision_delta  # type: ignore


pytestmark = pytest.mark.integration


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _thread_payload() -> dict:
    return {
        "id": "rt-acme-deal-reality",
        "title": "Restore Acme Expansion to a supportable late-stage path",
        "status": "active",
        "current_state": "Commit forecast is unsupported.",
        "target_state": "Security review scheduled.",
        "owner": "AE + RevOps",
        "success_criteria": ["Security owner assigned."],
        "steps": [
            {
                "id": "step-security-owner",
                "label": "Assign internal security owner",
                "owner": "VP Sales Ops",
                "status": "waiting",
                "proof_needed": "Named owner appears in Slack.",
            }
        ],
        "watched_signals": [
            {
                "id": "watch-calendar",
                "label": "Buyer alignment meeting",
                "source_type": "Calendar",
                "expected": "CFO + security call appears this week.",
                "status": "watching",
            }
        ],
        "escalation_triggers": ["No buyer alignment meeting scheduled."],
    }


@pytest.mark.asyncio
async def test_accept_delta_creates_persisted_resolution_thread(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    did = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        main_assertion="Move Acme Expansion forecast from Commit to Best Case",
        label="authority_required",
        confidence=0.78,
        falsification_condition="Security review is scheduled.",
        impact={
            "arr_at_risk": 1_200_000,
            "accounts_affected": 1,
            "resolution_thread": _thread_payload(),
        },
        evidence=[
            {
                "source": "calendar",
                "title": "Buyer alignment meeting scheduled",
                "ts": datetime.now(timezone.utc),
                "trust_tier": "verified",
                "excerpt": "CFO + security call appears this week.",
            }
        ],
    )

    resp = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["triggered"]["resolution_thread_created"] is True
    thread = body["updatedDelta"]["resolutionThread"]
    assert thread["title"].startswith("Restore Acme Expansion")
    assert thread["sourceDecisionDeltaId"] == str(did)
    assert thread["steps"][0]["label"] == "Assign internal security owner"

    second = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))
    assert second.status_code == 200, second.text
    assert second.json()["triggered"]["resolution_thread_created"] is False

    listed = await client.get(
        f"/v1/resolution_threads/?source_decision_delta_id={did}",
        headers=_auth(token),
    )
    assert listed.status_code == 200, listed.text
    assert listed.json()["count"] == 1


@pytest.mark.asyncio
async def test_resolution_thread_evaluate_and_complete_step(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    did = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        main_assertion="Move Acme Expansion forecast from Commit to Best Case",
        label="authority_required",
        confidence=0.78,
        falsification_condition="Security review is scheduled.",
        impact={"resolution_thread": _thread_payload()},
        evidence=[
            {
                "source": "calendar",
                "title": "Buyer alignment meeting scheduled",
                "ts": datetime.now(timezone.utc),
                "trust_tier": "verified",
                "excerpt": "CFO + security call appears this week.",
            }
        ],
    )
    applied = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))
    assert applied.status_code == 200, applied.text
    thread = applied.json()["updatedDelta"]["resolutionThread"]
    thread_id = thread["id"]
    step_id = thread["steps"][0]["id"]

    evaluated = await client.post(
        f"/v1/resolution_threads/{thread_id}/evaluate",
        headers=_auth(token),
    )
    assert evaluated.status_code == 200, evaluated.text
    evaluated_body = evaluated.json()
    assert evaluated_body["evaluation"]["signalsSeen"] == 1
    assert evaluated_body["thread"]["watchedSignals"][0]["status"] == "seen"

    completed = await client.patch(
        f"/v1/resolution_threads/{thread_id}/steps/{step_id}",
        headers=_auth(token),
        json={"status": "done"},
    )
    assert completed.status_code == 200, completed.text
    body = completed.json()
    assert body["thread"]["steps"][0]["status"] == "done"
    assert body["thread"]["status"] == "confirmed"
