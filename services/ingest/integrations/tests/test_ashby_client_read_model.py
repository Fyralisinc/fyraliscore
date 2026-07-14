"""Tests for Ashby's expanded read-model endpoint mapping."""
from __future__ import annotations

from typing import Any

import pytest

from services.ingest.integrations.ashby.client import (
    DEFAULT_ENTITIES,
    INTELLIGENCE_ENTITIES,
    AshbyClient,
)


pytestmark = pytest.mark.asyncio


async def test_default_entities_include_company_intelligence_surfaces() -> None:
    assert "candidate" in DEFAULT_ENTITIES
    assert "application_feedback" in DEFAULT_ENTITIES
    assert "job_posting" in DEFAULT_ENTITIES
    assert "opening" in DEFAULT_ENTITIES
    assert "user" in DEFAULT_ENTITIES
    assert set(INTELLIGENCE_ENTITIES).issubset(DEFAULT_ENTITIES)


async def test_snake_case_entity_maps_to_ashby_rpc_name(monkeypatch) -> None:
    client = AshbyClient(
        base_url="https://api.ashbyhq.com",
        org_id="org-1",
        api_key="test-key",
    )
    seen: dict[str, Any] = {}

    async def fake_rpc(method_path: str, body: dict[str, Any] | None = None):
        seen["method_path"] = method_path
        seen["body"] = body
        return {"success": True, "results": []}

    monkeypatch.setattr(client, "_rpc", fake_rpc)

    await client.list_entities("application_feedback", limit=25)

    assert seen == {
        "method_path": "applicationFeedback.list",
        "body": {"limit": 25},
    }


async def test_endpoint_default_options_are_applied(monkeypatch) -> None:
    client = AshbyClient(
        base_url="https://api.ashbyhq.com",
        org_id="org-1",
        api_key="test-key",
    )
    calls: list[tuple[str, dict[str, Any] | None]] = []

    async def fake_rpc(method_path: str, body: dict[str, Any] | None = None):
        calls.append((method_path, body))
        return {"success": True, "results": []}

    monkeypatch.setattr(client, "_rpc", fake_rpc)

    await client.list_entities("user", cursor="C", sync_token="S", limit=50)
    await client.list_entities("job_posting", cursor="ignored", sync_token="ignored")
    await client.list_entities("survey_submission_questionnaire")

    assert calls[0] == (
        "user.list",
        {
            "includeDeactivated": True,
            "limit": 50,
            "cursor": "C",
            "syncToken": "S",
        },
    )
    assert calls[1] == (
        "jobPosting.list",
        {
            "includeUnpublishedJobPostings": True,
            "listedOnly": False,
        },
    )
    assert calls[2] == (
        "surveySubmission.list",
        {"surveyType": "Questionnaire", "limit": 100},
    )
