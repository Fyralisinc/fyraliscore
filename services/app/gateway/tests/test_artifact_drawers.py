from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.app.gateway.artifact_drawers import fetch_commitment_overlay


class _OverlayConn:
    def __init__(self, *, tenant_id: UUID, commitment_id: UUID) -> None:
        self.tenant_id = tenant_id
        self.commitment_id = commitment_id
        self.owner_id = uuid4()
        self.actor_id = uuid4()
        self.goal_id = uuid4()
        self.customer_id = uuid4()
        self.contributor_id = uuid4()
        self.resource_id = uuid4()
        self.decision_id = uuid4()
        self.model_id = uuid4()
        self.observation_id = uuid4()
        self.cause_id = uuid4()
        self.now = datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc)

    async def fetchrow(self, query: str, *args):
        if "FROM commitments" in query:
            if args[0] != self.commitment_id or args[1] != self.tenant_id:
                return None
            return {
                "id": self.commitment_id,
                "title": "Close enterprise rollout",
                "state": "blocked",
                "owner_id": self.owner_id,
                "due_date": self.now,
                "priority": 2,
                "is_maintenance": False,
            }
        if "SELECT id, display_name FROM actors" in query:
            return {"id": self.owner_id, "display_name": "Asha Owner"}
        if "SELECT source_channel, content_text, occurred_at" in query:
            return {
                "source_channel": "slack",
                "content_text": "Customer security review found a blocking gap.",
                "occurred_at": self.now,
                "actor_id": self.actor_id,
            }
        if "SELECT display_name FROM actors" in query:
            return {"display_name": "Riya Reviewer"}
        return None

    async def fetch(self, query: str, *args):
        if "FROM goals g" in query:
            return [
                {
                    "id": self.goal_id,
                    "title": "Enterprise readiness",
                    "altitude": "strategic",
                    "parent_goal_id": None,
                }
            ]
        if "JOIN customer_commitments" in query:
            return [
                {
                    "id": self.customer_id,
                    "identity": "acme",
                    "metadata": {"display_name": "Acme"},
                }
            ]
        if "JOIN commitment_contributors" in query:
            return [
                {"id": self.contributor_id, "display_name": "Dev Partner"}
            ]
        if "JOIN resource_deployments" in query:
            return [
                {
                    "id": self.resource_id,
                    "kind": "human",
                    "identity": "platform-pod",
                    "description": "Platform pod",
                    "current_value": {"label": "Platform pod", "unit": "FTE"},
                    "utilization_state": "allocated",
                    "metadata": {},
                    "deployed_quantity": {"value": 0.4},
                }
            ]
        if "FROM decisions d" in query:
            return [
                {
                    "id": self.decision_id,
                    "title": "Hold launch",
                    "decision_text": "Pause launch until review closes",
                    "rationale": "Avoid enterprise churn",
                    "state": "drifting",
                }
            ]
        if "FROM models" in query:
            return [
                {
                    "id": self.model_id,
                    "natural": "Security reviews delay enterprise rollouts.",
                    "proposition": {},
                    "confidence": 0.8,
                    "falsifier": None,
                    "kind": "pattern",
                    "supporting_event_ids": [self.observation_id],
                    "evidential_weight": 0.7,
                    "created_at": self.now,
                }
            ]
        if "kind = 'state_change'" in query:
            return [
                {
                    "id": uuid4(),
                    "occurred_at": self.now,
                    "cause_id": self.cause_id,
                    "content": {
                        "from_state": "on-track",
                        "to_state": "blocked",
                    },
                }
            ]
        if "SELECT id, occurred_at, content_text FROM observations" in query:
            return [
                {
                    "id": self.observation_id,
                    "occurred_at": self.now,
                    "content_text": "Security review flagged missing SSO evidence.",
                }
            ]
        return []


@pytest.mark.asyncio
async def test_fetch_commitment_overlay_assembles_structure_payload() -> None:
    tenant_id = uuid4()
    commitment_id = uuid4()
    conn = _OverlayConn(tenant_id=tenant_id, commitment_id=commitment_id)

    payload = await fetch_commitment_overlay(commitment_id, tenant_id, conn)  # type: ignore[arg-type]

    assert payload is not None
    assert payload["commitment"]["label"] == "Close enterprise rollout"
    assert payload["commitment"]["status"] == "blocked"
    assert payload["commitment"]["priority"] == "high"
    assert payload["commitment"]["customer_label"] == "Acme"
    assert payload["commitment"]["edges"] == {
        "contributes_to": [str(conn.goal_id)],
        "constrained_by": [str(conn.decision_id)],
        "consumes": [str(conn.resource_id)],
        "contributors": [str(conn.contributor_id)],
    }
    assert payload["goals"][0]["label"] == "Enterprise readiness"
    assert payload["people"] == [
        {"id": str(conn.owner_id), "label": "Asha Owner", "role": "Owner"},
        {"id": str(conn.contributor_id), "label": "Dev Partner", "role": "Contributor"},
    ]
    assert payload["resources"][0]["deployed_quantity"] == 0.4
    assert payload["decisions"][0]["state"] == "drifting"
    assert payload["commitment"]["activity"][0]["desc"] == (
        "transitioned on-track \u2192 blocked"
    )
    assert "Customer security review" in payload["commitment"]["activity"][1]["desc"]
    assert payload["commitment"]["learnings"][0]["strength"] == 0.8


@pytest.mark.asyncio
async def test_fetch_commitment_overlay_returns_none_for_missing_commitment() -> None:
    tenant_id = uuid4()
    conn = _OverlayConn(tenant_id=tenant_id, commitment_id=uuid4())

    payload = await fetch_commitment_overlay(uuid4(), tenant_id, conn)  # type: ignore[arg-type]

    assert payload is None
