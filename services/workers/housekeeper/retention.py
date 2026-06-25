"""Retention jobs owned by the housekeeper worker."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
import structlog

from lib.observability.metrics import counter, gauge

log = structlog.get_logger(__name__)

DEFAULT_THINK_RUN_ARTIFACT_RETENTION_DAYS = 30
DEFAULT_THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE = 5000


@dataclass(frozen=True, slots=True)
class RetentionResult:
    table: str
    matched: int
    deleted: int
    cutoff: datetime
    batch_size: int
    dry_run: bool


_RETENTION_ROWS = counter(
    "housekeeper_retention_rows_total",
    "Rows matched or deleted by housekeeper retention jobs.",
    ("table", "mode"),
)
_RETENTION_ELIGIBLE = gauge(
    "housekeeper_retention_eligible_rows",
    "Rows eligible in the latest housekeeper retention batch.",
    ("table",),
)
_RETENTION_LAST_RUN = gauge(
    "housekeeper_retention_last_run_timestamp_seconds",
    "Unix timestamp of the latest housekeeper retention job run.",
    ("table", "status"),
)


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, value)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def think_run_artifact_retention_cutoff(
    *,
    now: datetime | None = None,
    retention_days: int | None = None,
) -> datetime:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    days = retention_days
    if days is None:
        days = _positive_int_env(
            "THINK_RUN_ARTIFACT_RETENTION_DAYS",
            DEFAULT_THINK_RUN_ARTIFACT_RETENTION_DAYS,
        )
    return now - timedelta(days=max(1, int(days)))


async def delete_expired_think_run_artifacts(
    conn: asyncpg.Connection,
    *,
    cutoff: datetime,
    batch_size: int = DEFAULT_THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE,
    dry_run: bool = False,
) -> RetentionResult:
    """Delete one bounded batch of old Think debug artifacts."""

    batch_size = max(1, int(batch_size))
    if dry_run:
        matched = await conn.fetchval(
            """
            SELECT count(*)::int
            FROM (
                SELECT id
                FROM think_run_artifacts
                WHERE captured_at < $1
                ORDER BY captured_at ASC
                LIMIT $2
            ) doomed
            """,
            cutoff,
            batch_size,
        )
        deleted = 0
    else:
        deleted = await conn.fetchval(
            """
            WITH doomed AS (
                SELECT id
                FROM think_run_artifacts
                WHERE captured_at < $1
                ORDER BY captured_at ASC
                LIMIT $2
            ),
            deleted AS (
                DELETE FROM think_run_artifacts a
                USING doomed
                WHERE a.id = doomed.id
                RETURNING 1
            )
            SELECT count(*)::int FROM deleted
            """,
            cutoff,
            batch_size,
        )
        matched = deleted
    result = RetentionResult(
        table="think_run_artifacts",
        matched=int(matched or 0),
        deleted=int(deleted or 0),
        cutoff=cutoff,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    _record_metrics(result, status="ok")
    log.info(
        "retention.think_run_artifacts",
        matched=result.matched,
        deleted=result.deleted,
        cutoff=result.cutoff.isoformat(),
        batch_size=result.batch_size,
        dry_run=result.dry_run,
    )
    return result


async def run_think_run_artifact_retention(pool: asyncpg.Pool) -> RetentionResult:
    cutoff = think_run_artifact_retention_cutoff()
    batch_size = _positive_int_env(
        "THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE",
        DEFAULT_THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE,
    )
    dry_run = _env_bool("THINK_RUN_ARTIFACT_RETENTION_DRY_RUN", False)
    async with pool.acquire() as conn:
        return await delete_expired_think_run_artifacts(
            conn,
            cutoff=cutoff,
            batch_size=batch_size,
            dry_run=dry_run,
        )


def _record_metrics(result: RetentionResult, *, status: str) -> None:
    mode = "dry_run" if result.dry_run else "delete"
    _RETENTION_ROWS.inc(
        result.matched if result.dry_run else result.deleted,
        table=result.table,
        mode=mode,
    )
    _RETENTION_ELIGIBLE.set(result.matched, table=result.table)
    _RETENTION_LAST_RUN.set(
        datetime.now(timezone.utc).timestamp(),
        table=result.table,
        status=status,
    )


__all__ = [
    "DEFAULT_THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE",
    "DEFAULT_THINK_RUN_ARTIFACT_RETENTION_DAYS",
    "RetentionResult",
    "delete_expired_think_run_artifacts",
    "run_think_run_artifact_retention",
    "think_run_artifact_retention_cutoff",
]
