"""Tests for services/ingest/ingestion/planners/google_drive.py (IN-16)."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.google_drive import (
    SHARD_KIND_FILES,
    plan_shards_google_drive,
)


pytestmark = pytest.mark.asyncio


def _ctx(targets):
    install = {"id": "11111111-1111-1111-1111-111111111111",
               "targets": json.dumps(targets)}
    return PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )


async def test_dispatch_registered():
    assert resolve_planner("google_drive") is plan_shards_google_drive


async def test_one_shard_per_active_target():
    shards = await plan_shards_google_drive(_ctx([
        {"drive_kind": "my_drive", "drive_id": "my-drive",
         "owner_email": "alice@acme.com", "start_page_token": None},
        {"drive_kind": "shared_drive", "drive_id": "0ABC",
         "owner_email": "admin@acme.com", "start_page_token": "spt-9"},
    ]))
    assert len(shards) == 2
    assert all(s.shard_kind == SHARD_KIND_FILES for s in shards)
    by_owner = {s.shard_identifier["owner_email"]: s.shard_identifier for s in shards}
    assert by_owner["alice@acme.com"]["drive_kind"] == "my_drive"
    assert by_owner["admin@acme.com"]["drive_id"] == "0ABC"
    assert by_owner["admin@acme.com"]["start_page_token"] == "spt-9"


async def test_empty_targets_yields_no_shards():
    shards = await plan_shards_google_drive(_ctx([]))
    assert shards == []


async def test_target_without_owner_skipped():
    shards = await plan_shards_google_drive(_ctx([
        {"drive_kind": "my_drive", "drive_id": "my-drive", "owner_email": ""},
    ]))
    assert shards == []
