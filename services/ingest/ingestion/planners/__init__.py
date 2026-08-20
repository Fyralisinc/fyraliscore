"""Backfill planner compatibility surface for consolidation carry-forwards.

Stable source families use SourceConnector planning capabilities. Instagram
and Facebook Pages keep their completed provider-specific history planners
until those implementations are ported to the stable connector contract.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from services.ingest.ingestion.planners.context import PlannerContext


@dataclass(frozen=True)
class Shard:
    shard_kind: str
    shard_identifier: dict[str, Any]
    recency_score: float = 1.0
    window_start: dt.datetime | None = None
    window_end: dt.datetime | None = None


Planner = Callable[[PlannerContext], Awaitable[list[Shard]]]
PLANNER_DISPATCH: dict[str, Planner] = {}


from services.ingest.ingestion.planners import (
    facebook_pages as _facebook_pages,  # noqa: E402,F401
)
from services.ingest.ingestion.planners import (
    instagram as _instagram,  # noqa: E402,F401
)

__all__ = ["PLANNER_DISPATCH", "Planner", "PlannerContext", "Shard"]
