from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from lib.observability.metrics import render_default
from scripts.record_backup_recovery_status import (
    BackupStatusCliError,
    _parse_details,
    build_parser,
    run_command,
)
from services.platform.backup_recovery import (
    classify_backup_health,
    default_freshness_slo_seconds,
    fetch_backup_recovery_statuses,
    record_backup_recovery_status,
    refresh_backup_recovery_metrics,
)


pytestmark = pytest.mark.integration

_TEST_KEYS = (
    ("postgres", "backup"),
    ("object_store", "restore_test"),
    ("application_config", "inventory"),
)


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


@pytest_asyncio.fixture
async def backup_status_clean(fresh_db: asyncpg.Pool):
    async with fresh_db.acquire() as conn:
        await _delete_test_keys(conn)
    try:
        yield
    finally:
        async with fresh_db.acquire() as conn:
            await _delete_test_keys(conn)


async def _delete_test_keys(conn: asyncpg.Connection) -> None:
    for component, check_name in _TEST_KEYS:
        await conn.execute(
            """
            DELETE FROM backup_recovery_status
            WHERE component = $1 AND check_name = $2
            """,
            component,
            check_name,
        )


def test_default_freshness_slo_reads_component_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKUP_POSTGRES_FRESHNESS_SLO_SECONDS", "7200")
    monkeypatch.setenv("BACKUP_OBJECT_STORE_FRESHNESS_SLO_SECONDS", "14400")
    monkeypatch.setenv("RESTORE_TEST_FRESHNESS_SLO_SECONDS", "bad-value")

    assert default_freshness_slo_seconds("backup", component="postgres") == 7200
    assert default_freshness_slo_seconds("backup", component="object_store") == 14400
    assert (
        default_freshness_slo_seconds("restore_test", component="postgres")
        == 35 * 24 * 60 * 60
    )


def test_record_backup_status_cli_rejects_non_object_details() -> None:
    with pytest.raises(BackupStatusCliError, match="JSON object"):
        _parse_details('["not", "an", "object"]')


@pytest.mark.asyncio
async def test_record_backup_status_cli_writes_row_and_metrics(
    fresh_db: asyncpg.Pool,
    backup_status_clean,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    occurred_at = datetime(2026, 6, 24, 1, 2, 3, tzinfo=timezone.utc)
    monkeypatch.setenv("BACKUP_POSTGRES_FRESHNESS_SLO_SECONDS", "7200")

    async with fresh_db.acquire() as conn:
        result = await run_command(
            _parse(
                [
                    "--component",
                    "postgres",
                    "--check",
                    "backup",
                    "--status",
                    "ok",
                    "--occurred-at",
                    occurred_at.isoformat(),
                    "--details-json",
                    '{"provider":"pg_basebackup","job":"daily"}',
                ]
            ),
            conn=conn,
        )

        assert result["ok"] is True
        assert result["component"] == "postgres"
        assert result["check_name"] == "backup"
        assert result["status"] == "ok"
        assert result["freshness_slo_seconds"] == 7200
        assert result["details"] == {"provider": "pg_basebackup", "job": "daily"}

        statuses = await fetch_backup_recovery_statuses(conn)
        status = next(
            row
            for row in statuses
            if row.component == "postgres" and row.check_name == "backup"
        )
        assert status.last_success_at == occurred_at
        assert classify_backup_health(
            status,
            now=occurred_at + timedelta(seconds=60),
        ) == "fresh"

        await refresh_backup_recovery_metrics(
            conn,
            now=occurred_at + timedelta(seconds=60),
        )

    text = render_default()
    assert (
        'backup_recovery_health_status{component="postgres",'
        'check="backup",state="fresh"} 1'
    ) in text
    assert (
        'backup_recovery_last_success_age_seconds{component="postgres",'
        'check="backup"} 60'
    ) in text


@pytest.mark.asyncio
async def test_failed_backup_status_preserves_last_success_but_marks_failed(
    fresh_db: asyncpg.Pool,
    backup_status_clean,
) -> None:
    success_at = datetime(2026, 6, 20, 10, 0, tzinfo=timezone.utc)
    failed_at = datetime(2026, 6, 24, 10, 0, tzinfo=timezone.utc)

    async with fresh_db.acquire() as conn:
        await record_backup_recovery_status(
            conn,
            component="object_store",
            check_name="restore_test",
            status="ok",
            occurred_at=success_at,
            freshness_slo_seconds=10,
            details={"provider": "s3"},
        )
        status = await record_backup_recovery_status(
            conn,
            component="object_store",
            check_name="restore_test",
            status="failed",
            occurred_at=failed_at,
            freshness_slo_seconds=10,
            details={"reason": "quota"},
        )

        assert status.status == "failed"
        assert status.last_success_at == success_at
        assert status.last_attempt_at == failed_at
        assert classify_backup_health(status, now=failed_at) == "failed"

        await refresh_backup_recovery_metrics(conn, now=failed_at)

    text = render_default()
    assert (
        'backup_recovery_health_status{component="object_store",'
        'check="restore_test",state="failed"} 1'
    ) in text


@pytest.mark.asyncio
async def test_backup_status_classifies_missing_and_stale(
    fresh_db: asyncpg.Pool,
    backup_status_clean,
) -> None:
    now = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)

    async with fresh_db.acquire() as conn:
        missing = await record_backup_recovery_status(
            conn,
            component="application_config",
            check_name="inventory",
            status="unknown",
            occurred_at=now,
            freshness_slo_seconds=100,
        )
        stale = await record_backup_recovery_status(
            conn,
            component="postgres",
            check_name="backup",
            status="ok",
            occurred_at=now - timedelta(seconds=101),
            freshness_slo_seconds=100,
        )

    assert classify_backup_health(missing, now=now) == "missing"
    assert classify_backup_health(stale, now=now) == "stale"
