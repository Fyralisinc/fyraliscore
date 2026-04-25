"""
services/models/decay.py — hourly activation decay and decay-based
archival per spec §2 + §12.

Spec §2:

    UPDATE models SET activation = activation * exp(-1.0/120.0)
    WHERE status = 'active';

120 = 5 days * 24 h, so each pass is one hour of 5-day-half-life decay.

    UPDATE models
    SET status='archived', archived_at=now(), archive_reason='decay'
    WHERE status='active'
      AND activation < 0.05
      AND (last_retrieved_at IS NULL
           OR last_retrieved_at < now() - interval '30 days');

Both operations accept an optional asyncpg connection so they can run
inside a test fixture or a worker loop without touching the shared
pool. Each returns the number of rows affected — useful for the
maintenance worker's structured log.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from lib.shared.db import get_pool


HOURLY_DECAY_MULTIPLIER = "exp(-1.0/120.0)"
# 5-day half-life ≈ 120 hourly ticks per half. After 120 ticks,
# activation ≈ e^-1 ≈ 0.368.


async def hourly_decay(*, conn: asyncpg.Connection | None = None) -> int:
    """
    Apply one hour's worth of exponential decay to every active Model's
    activation. Returns the number of rows updated.
    """
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        UPDATE models
        SET activation = activation * exp(-1.0/120.0)
        WHERE status = 'active'
        """
    )
    return _rowcount_from_tag(tag)


async def archive_decayed(*, conn: asyncpg.Connection | None = None) -> int:
    """
    Archive active Models whose activation has collapsed below 0.05
    AND have not been retrieved in the last 30 days (or have never
    been retrieved). Sets archive_reason='decay'. Returns the number
    of rows updated.
    """
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        UPDATE models
        SET status = 'archived',
            archived_at = now(),
            archive_reason = 'decay'
        WHERE status = 'active'
          AND activation < 0.05
          AND (last_retrieved_at IS NULL
               OR last_retrieved_at < now() - interval '30 days')
        """
    )
    return _rowcount_from_tag(tag)


def _rowcount_from_tag(tag: str) -> int:
    # asyncpg returns e.g. 'UPDATE 42'. If anything else, we tolerate
    # and return 0 — the row count is informational, not load-bearing.
    try:
        return int(tag.split()[-1])
    except (IndexError, ValueError):
        return 0


__all__ = ["hourly_decay", "archive_decayed", "HOURLY_DECAY_MULTIPLIER"]
