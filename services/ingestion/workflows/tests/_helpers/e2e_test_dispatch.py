"""Subprocess-loadable test planner + fetcher + reconciler
for the M6.2a Phase 3 multi-subprocess end-to-end test. Installs all
three into their respective dispatch tables on import.

Test planner: returns 2 shards for source='slack' (channel C001
and C002 windows).

Test fetcher: returns 5 records on the first call (cursor is
None), then end_of_data=True with empty records on the second
call. One page per shard; 10 records total across 2 shards.

Test reconciler: always returns clean (no gaps) — the test's
clean-path intent. Post-M6.5 the real slack reconciler tries to
open a Slack API client this test doesn't wire up; the override
sidesteps that.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
)


async def _e2e_test_planner(ctx: PlannerContext) -> list[Shard]:
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


async def _e2e_test_reconciler(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    return ReconciliationDecision(has_gaps=False, message="e2e clean")


# Install all three into the dispatch tables at import time.
PLANNER_DISPATCH["slack"] = _e2e_test_planner
FETCHER_DISPATCH["slack"] = _e2e_test_fetcher
RECONCILER_DISPATCH["slack"] = _e2e_test_reconciler
