"""Figma planning includes a document snapshot even for zero-event files."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.figma import (
    SHARD_KIND_FILE_EVENTS,
    SHARD_KIND_FILE_SNAPSHOT,
    plan_shards_figma,
)


class _Record(dict):
    """Dict provides the small asyncpg.Record surface the planner uses."""


@pytest.mark.asyncio
async def test_each_active_figma_file_has_event_and_snapshot_shards():
    tenant_id = uuid4()
    install_id = uuid4()
    ctx = PlannerContext(
        tenant_id=tenant_id,
        install=_Record({
            "id": install_id,
            "team_id": "team-1",
            "files": [
                {
                    "file_key": "file-a",
                    "file_name": "Checkout",
                    "project_name": "Payments",
                    "event_cursor": None,
                    "snapshot_version": "v-8",
                },
            ],
        }),  # type: ignore[arg-type]
        conn=None,  # type: ignore[arg-type]
    )

    shards = await plan_shards_figma(ctx)

    assert [shard.shard_kind for shard in shards] == [
        SHARD_KIND_FILE_EVENTS,
        SHARD_KIND_FILE_SNAPSHOT,
    ]
    snapshot = shards[1].shard_identifier
    assert snapshot["file_key"] == "file-a"
    assert snapshot["snapshot_version"] == "v-8"
    assert snapshot["installation_id"] == str(install_id)
