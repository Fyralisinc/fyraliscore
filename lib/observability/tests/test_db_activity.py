from __future__ import annotations

import pytest

from lib.observability import db_activity
from lib.observability.metrics import render_default, reset_default_for_tests


class _FakeConn:
    def __init__(self, row):
        self.row = row
        self.thresholds: list[float] = []

    async def fetchrow(self, _query: str, threshold: float):
        self.thresholds.append(threshold)
        if isinstance(self.row, Exception):
            raise self.row
        return self.row


@pytest.fixture(autouse=True)
def _reset_metrics():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


@pytest.mark.asyncio
async def test_refresh_db_activity_metrics_records_alertable_gauges() -> None:
    conn = _FakeConn(
        {
            "longest_transaction_age_seconds": 42.5,
            "long_transaction_count": 2,
        }
    )

    snapshot = await db_activity.refresh_db_activity_metrics(
        conn,  # type: ignore[arg-type]
        threshold_seconds=30,
    )

    assert snapshot.longest_transaction_age_seconds == 42.5
    assert snapshot.long_transaction_count == 2
    assert conn.thresholds == [30.0]
    text = render_default()
    assert "db_longest_transaction_age_seconds 42.5" in text
    assert "db_long_transactions_over_threshold 2" in text
    assert "db_long_transaction_threshold_seconds 30" in text
    assert "db_activity_last_refresh_timestamp_seconds " in text


@pytest.mark.asyncio
async def test_refresh_db_activity_metrics_counts_failures() -> None:
    conn = _FakeConn(RuntimeError("pg_stat_activity unavailable"))

    with pytest.raises(RuntimeError):
        await db_activity.refresh_db_activity_metrics(conn)  # type: ignore[arg-type]

    assert "db_activity_refresh_failures_total 1" in render_default()
