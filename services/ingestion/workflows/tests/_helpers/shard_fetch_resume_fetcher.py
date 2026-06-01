"""Subprocess-loadable test fetcher for the
shard_fetch resume-from-cursor test. Installs itself into
FETCHER_DISPATCH on import.

Strategy: returns one fake record per page, with an asyncio.sleep
between pages so the test can SIGKILL the subprocess between
advances. Three pages, then end_of_data. The cursor `{"page": N}`
encodes which page was last successfully emitted.
"""
from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


# Per-page artificial delay. Picked to make the resume test
# reliably catchable: the test waits for cursor advance ~1s in,
# then SIGKILLs while the fetcher is sleeping in the NEXT page.
_PAGE_DELAY_SECONDS = 1.5


async def _resume_test_fetcher(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    current_page = 0 if cursor is None else (cursor.get("page", -1) + 1)
    # 3 pages then end_of_data.
    if current_page >= 3:
        return FetchResult(records=[], next_cursor=None, end_of_data=True)
    await asyncio.sleep(_PAGE_DELAY_SECONDS)
    records = [{"page": current_page, "id": current_page * 10}]
    return FetchResult(
        records=records,
        next_cursor={"page": current_page},
        end_of_data=(current_page == 2),
    )


# Install into the dispatch table at import time.
FETCHER_DISPATCH["github"] = _resume_test_fetcher
