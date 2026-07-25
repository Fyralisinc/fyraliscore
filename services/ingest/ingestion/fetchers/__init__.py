"""Shared fetcher types.

Per-source fetchers are declared by ``SourceDefinition`` and resolved lazily
through ``services.ingest.source_contract.runtime``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import asyncpg


@dataclass(frozen=True)
class FetchResult:
    """One fetched page plus its opaque next cursor."""

    records: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: dict[str, Any] | None = None
    end_of_data: bool = False


Fetcher = Callable[
    [asyncpg.Record, dict[str, Any], dict[str, Any] | None],
    Awaitable[FetchResult],
]


__all__ = [
    "Fetcher",
    "FetchResult",
]
