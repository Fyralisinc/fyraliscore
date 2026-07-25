"""Tests for services/ingest/ingestion/planners/jira.py (IN-17)."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.jira import (
    SHARD_KIND_PROJECT_ISSUES,
    plan_shards_jira,
)


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, data):
        self._d = data

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _ctx(projects):
    install = _FakeRecord({"id": uuid4(), "projects": json.dumps(projects)})
    return PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )


async def test_dispatch_wired():
    assert resolve_planner("jira") is plan_shards_jira


async def test_one_shard_per_project():
    ctx = _ctx([
        {"project_key": "ENG", "project_id": "100", "updated_cursor": None},
        {"project_key": "OPS", "project_id": "200", "updated_cursor": "2026-05-01T00:00:00.000+0000"},
    ])
    shards = await plan_shards_jira(ctx)
    assert len(shards) == 2
    assert all(s.shard_kind == SHARD_KIND_PROJECT_ISSUES for s in shards)
    by_key = {s.shard_identifier["project_key"]: s for s in shards}
    assert by_key["ENG"].shard_identifier["project_id"] == "100"
    # warm cursor threaded into the shard for incremental.
    assert by_key["OPS"].shard_identifier["updated_cursor"] == "2026-05-01T00:00:00.000+0000"


async def test_empty_projects_yields_no_shards():
    assert await plan_shards_jira(_ctx([])) == []


async def test_projects_as_native_list():
    install = _FakeRecord({
        "id": uuid4(),
        "projects": [{"project_key": "ENG", "project_id": "100"}],
    })
    ctx = PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )
    shards = await plan_shards_jira(ctx)
    assert [s.shard_identifier["project_key"] for s in shards] == ["ENG"]
