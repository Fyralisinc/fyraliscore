"""Shared reconciliation types and pool wiring.

Reconciler callables are declared by ``SourceDefinition`` and resolved lazily.
Pool registration derives from the 26 source definitions that declare history;
WhatsApp is intentionally excluded because ``history=None``.
"""
from __future__ import annotations

import importlib
from typing import Awaitable, Callable
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from services.ingest.ingestion.planners import Shard


class ResharedShard(BaseModel):
    """A recovery shard linked to the original shard that contained a gap."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    shard: Shard
    parent_shard_id: UUID


class ReconciliationDecision(BaseModel):
    """A source reconciler's clean-or-reshare decision."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    has_gaps: bool
    message: str = ""
    new_shards: list[ResharedShard] = Field(default_factory=list)


Reconciler = Callable[
    [list[asyncpg.Record], asyncpg.Record],
    Awaitable[ReconciliationDecision],
]


def register_pool_provider(pool: asyncpg.Pool) -> list[str]:
    """Register ``pool`` with every historical source reconciler module."""

    from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
    from services.ingest.source_contract.runtime import split_callable_reference

    registered: list[str] = []
    historical_sources = (
        source for source in SOURCE_DEFINITIONS if source.history is not None
    )
    for source in historical_sources:
        reference = source.reconciler_binding
        if reference is None:  # guarded by catalog validation
            raise RuntimeError(
                f"historical source {source.source_id!r} has no reconciler binding"
            )
        module_name, _ = split_callable_reference(reference)
        module = importlib.import_module(module_name)
        setter = getattr(module, "set_pool_provider", None)
        if setter is not None:
            setter(pool)
            registered.append(source.source_id)
    return registered


__all__ = [
    "Reconciler",
    "ReconciliationDecision",
    "ResharedShard",
    "register_pool_provider",
]
