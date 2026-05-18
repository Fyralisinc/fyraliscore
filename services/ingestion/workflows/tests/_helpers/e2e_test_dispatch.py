"""Subprocess-loadable test planner + fetcher for the
M6.2a Phase 3 four-subprocess end-to-end test. Installs both into
their respective dispatch tables on import.

Test planner: returns 2 shards for source='slack' (channel C001
and C002 windows).

Test fetcher: returns 5 records on the first call (cursor is
None), then end_of_data=True with empty records on the second
call. One page per shard; 10 records total across 2 shards.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.planners import PLANNER_DISPATCH, Shard


async def _e2e_test_planner(
    tenant_id: UUID, install: asyncpg.Record,
) -> list[Shard]:
    return [
        Shard(
            shard_kind="slack_channel_window",
            shard_identifier={"channel_id": "C001"},
            recency_score=1.0,
        ),
        Shard(
            shard_kind="slack_channel_window",
            shard_identifier={"channel_id": "C002"},
            recency_score=0.9,
        ),
    ]


async def _e2e_test_fetcher(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    if cursor is not None:
        # Second call — end-of-data with no records.
        return FetchResult(records=[], next_cursor=None, end_of_data=True)
    channel = shard_identifier.get("channel_id", "?")
    records = [
        {"channel": channel, "ts": f"1700000000.{i:06d}", "text": f"msg-{i}"}
        for i in range(5)
    ]
    return FetchResult(
        records=records,
        next_cursor={"page": 0},
        end_of_data=False,
    )


# Install both into the dispatch tables at import time.
PLANNER_DISPATCH["slack"] = _e2e_test_planner
FETCHER_DISPATCH["slack"] = _e2e_test_fetcher
