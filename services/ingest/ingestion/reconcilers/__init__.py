"""Gap reconciliation compatibility surface for supplemental sources."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from services.ingest.ingestion.planners import Shard


class ResharedShard(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    shard: Shard
    parent_shard_id: UUID


class ReconciliationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    has_gaps: bool
    message: str = ""
    new_shards: list[ResharedShard] = Field(default_factory=list)


Reconciler = Callable[
    [list[asyncpg.Record], asyncpg.Record],
    Awaitable[ReconciliationDecision],
]
RECONCILER_DISPATCH: dict[str, Reconciler] = {}


def register_pool_provider(pool: asyncpg.Pool) -> list[str]:
    registered: list[str] = []
    for source, module in (
        ("facebook_pages", _facebook_pages),
        ("instagram", _instagram),
    ):
        module.set_pool_provider(pool)
        registered.append(source)
    return registered


from services.ingest.ingestion.reconcilers import (
    facebook_pages as _facebook_pages,  # noqa: E402
)
from services.ingest.ingestion.reconcilers import instagram as _instagram  # noqa: E402

__all__ = [
    "RECONCILER_DISPATCH",
    "Reconciler",
    "ReconciliationDecision",
    "ResharedShard",
    "register_pool_provider",
]
