"""Shared planner types.

Per-source planner callables are declared by ``SourceDefinition`` and resolved
through ``services.ingest.source_contract.runtime``. Importing this package no
longer imports every provider module for registration.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from services.ingest.ingestion.planners.context import PlannerContext


@dataclass(frozen=True)
class Shard:
    """Planner output: one row to insert into ``onboarding_shards``."""

    shard_kind: str
    shard_identifier: dict[str, Any]
    recency_score: float = 1.0
    window_start: dt.datetime | None = None
    window_end: dt.datetime | None = None


Planner = Callable[[PlannerContext], Awaitable[list[Shard]]]


__all__ = [
    "Planner",
    "PlannerContext",
    "Shard",
]
