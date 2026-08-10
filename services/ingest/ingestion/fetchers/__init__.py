"""Backfill fetcher compatibility surface for supplemental source adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import asyncpg


@dataclass(frozen=True)
class FetchResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: dict[str, Any] | None = None
    end_of_data: bool = False


Fetcher = Callable[
    [asyncpg.Record, dict[str, Any], dict[str, Any] | None],
    Awaitable[FetchResult],
]
FETCHER_DISPATCH: dict[str, Fetcher] = {}


from services.ingest.ingestion.fetchers import (
    facebook_pages as _facebook_pages,  # noqa: E402,F401
)
from services.ingest.ingestion.fetchers import (
    instagram as _instagram,  # noqa: E402,F401
)

__all__ = ["FETCHER_DISPATCH", "FetchResult", "Fetcher"]
