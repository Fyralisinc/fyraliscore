"""Subprocess-loadable test dispatch overrides for the
M6.2b reshare-path five-subprocess E2E test. Installs:
  - test planner → 2 shards (channels C001 + C002)
  - test fetcher → 5 records per shard then end_of_data
  - test reconciler → returns gappy on pass_count=0, clean on
    pass_count>0. The reshare picks `shards[0].id` as the
    parent_shard_id for the single new reshared shard.

Stateful via reading `run.reconciliation_pass_count` — no in-process
state shared across subprocesses. The pass_count column in
source_onboarding_runs IS the state surface (per M6.2b's
schema-first discipline).
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)


async def _planner(
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


async def _fetcher(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    if cursor is not None:
        return FetchResult(records=[], next_cursor=None, end_of_data=True)
    channel = shard_identifier.get("channel_id", "?")
    records = [
        {"channel": channel, "ts": f"1700000000.{i:06d}", "text": f"msg-{i}"}
        for i in range(5)
    ]
    return FetchResult(
        records=records, next_cursor={"page": 0}, end_of_data=False,
    )


async def _reshare_then_clean_reconciler(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    pass_count = run["reconciliation_pass_count"]
    if pass_count == 0:
        # First pass: declare a gap. Pick the first shard as the
        # parent of the reshared gap-filler.
        parent_id = shards[0]["id"]
        return ReconciliationDecision(
            has_gaps=True,
            message="test reshare: synthetic gap on first pass",
            new_shards=[
                ResharedShard(
                    shard=Shard(
                        shard_kind="slack_channel_window",
                        shard_identifier={
                            "channel_id": "C001",
                            "gap": "synthetic_window",
                        },
                        recency_score=1.5,  # boosted per LLD §3
                    ),
                    parent_shard_id=parent_id,
                ),
            ],
        )
    # Second pass (pass_count >= 1): clean.
    return ReconciliationDecision(
        has_gaps=False,
        message="test reshare: clean on pass 1",
    )


# Install all three dispatch overrides at import time.
PLANNER_DISPATCH["slack"] = _planner
FETCHER_DISPATCH["slack"] = _fetcher
RECONCILER_DISPATCH["slack"] = _reshare_then_clean_reconciler
