"""Database activity gauges for operational alerting."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import asyncpg

from lib.observability.metrics import counter, gauge


DEFAULT_LONG_TRANSACTION_THRESHOLD_SECONDS = 300.0


@dataclass(frozen=True)
class DbActivitySnapshot:
    longest_transaction_age_seconds: float
    long_transaction_count: int
    threshold_seconds: float


longest_transaction_age_seconds = gauge(
    "db_longest_transaction_age_seconds",
    "Age in seconds of the oldest open transaction visible in pg_stat_activity.",
)
long_transactions_over_threshold = gauge(
    "db_long_transactions_over_threshold",
    "Open transaction count older than the configured long-transaction threshold.",
)
long_transaction_threshold_seconds = gauge(
    "db_long_transaction_threshold_seconds",
    "Configured threshold used for db_long_transactions_over_threshold.",
)
db_activity_last_refresh_timestamp_seconds = gauge(
    "db_activity_last_refresh_timestamp_seconds",
    "Unix timestamp of the latest successful database activity metrics refresh.",
)
db_activity_refresh_failures_total = counter(
    "db_activity_refresh_failures_total",
    "Number of failed database activity metrics refresh attempts.",
)


def _threshold_from_env() -> float:
    raw = os.environ.get("DB_LONG_TRANSACTION_ALERT_THRESHOLD_SECONDS")
    if raw is None or raw == "":
        return DEFAULT_LONG_TRANSACTION_THRESHOLD_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return DEFAULT_LONG_TRANSACTION_THRESHOLD_SECONDS
    return max(1.0, value)


async def refresh_db_activity_metrics(
    conn: asyncpg.Connection,
    *,
    threshold_seconds: float | None = None,
) -> DbActivitySnapshot:
    threshold = float(threshold_seconds or _threshold_from_env())
    try:
        row = await conn.fetchrow(
            """
            SELECT
              COALESCE(
                MAX(EXTRACT(EPOCH FROM clock_timestamp() - xact_start))
                  FILTER (WHERE xact_start IS NOT NULL),
                0
              )::float8 AS longest_transaction_age_seconds,
              COUNT(*) FILTER (
                WHERE xact_start IS NOT NULL
                  AND clock_timestamp() - xact_start >= ($1::double precision * interval '1 second')
              )::bigint AS long_transaction_count
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND state IN (
                'active',
                'idle in transaction',
                'idle in transaction (aborted)'
              )
            """,
            threshold,
        )
    except Exception:
        db_activity_refresh_failures_total.inc()
        raise

    snapshot = _snapshot_from_row(row, threshold_seconds=threshold)
    longest_transaction_age_seconds.set(snapshot.longest_transaction_age_seconds)
    long_transactions_over_threshold.set(float(snapshot.long_transaction_count))
    long_transaction_threshold_seconds.set(snapshot.threshold_seconds)
    db_activity_last_refresh_timestamp_seconds.set(time.time())
    return snapshot


def _snapshot_from_row(
    row: Any,
    *,
    threshold_seconds: float,
) -> DbActivitySnapshot:
    if row is None:
        return DbActivitySnapshot(
            longest_transaction_age_seconds=0.0,
            long_transaction_count=0,
            threshold_seconds=threshold_seconds,
        )
    return DbActivitySnapshot(
        longest_transaction_age_seconds=max(
            0.0,
            float(row["longest_transaction_age_seconds"] or 0.0),
        ),
        long_transaction_count=max(0, int(row["long_transaction_count"] or 0)),
        threshold_seconds=threshold_seconds,
    )


__all__ = [
    "DEFAULT_LONG_TRANSACTION_THRESHOLD_SECONDS",
    "DbActivitySnapshot",
    "refresh_db_activity_metrics",
]
