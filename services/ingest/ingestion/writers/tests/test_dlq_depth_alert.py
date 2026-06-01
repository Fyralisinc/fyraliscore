"""DLQ-depth monitoring tests (gap #5: alert on backlog growth).

Pure DB + function tests — no Kafka needed. Exercises:
  - count_unresolved_failures counts only resolved_at IS NULL rows,
    across tenants (no tenant context → RLS returns all).
  - poll_dlq_depth sets the gauge, fires an alert over threshold,
    stays quiet under threshold, and debounces via the cooldown.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.ingest.ingestion.writers.dlq_writer import dlq_writer as mod
from services.ingest.ingestion.writers.dlq_writer.dlq_writer import (
    DLQWriterConfig,
    count_unresolved_failures,
    get_metrics,
    poll_dlq_depth,
    reset_metrics,
)


pytestmark = [pytest.mark.timeout(60)]


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"dlqd-{tid.hex[:8]}",
    )
    return tid


async def _seed_failure(
    pool: asyncpg.Pool, *, tenant_id: UUID, resolved: bool = False,
) -> None:
    await pool.execute(
        """
        INSERT INTO ingestion_failures
            (id, tenant_id, source, failure_kind, error_summary,
             resolved_at, resolution_kind)
        VALUES ($1, $2, 'slack', 'normalizer_parse_error', 'fixture',
                CASE WHEN $3 THEN now() ELSE NULL END,
                CASE WHEN $3 THEN 'discarded' ELSE NULL END)
        """,
        uuid7(), tenant_id, resolved,
    )


async def test_count_unresolved_counts_only_open_rows(
    fresh_db: asyncpg.Pool,
) -> None:
    tid = await _seed_tenant(fresh_db)
    for _ in range(3):
        await _seed_failure(fresh_db, tenant_id=tid, resolved=False)
    await _seed_failure(fresh_db, tenant_id=tid, resolved=True)

    assert await count_unresolved_failures(fresh_db) == 3


async def test_poll_alerts_over_threshold(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_metrics()
    sent: list[tuple[str, dict]] = []

    async def _capture(event, payload):
        sent.append((event, payload))
        return True

    monkeypatch.setattr(mod, "send_ops_alert", _capture)

    tid = await _seed_tenant(fresh_db)
    for _ in range(5):
        await _seed_failure(fresh_db, tenant_id=tid)

    config = DLQWriterConfig(depth_alert_threshold=3, depth_alert_cooldown_sec=3600.0)
    new_wm = await poll_dlq_depth(
        config, fresh_db,
        last_alert_monotonic=float("-inf"), now_monotonic=1000.0,
    )

    assert get_metrics()["dlq_writer.unresolved_depth"] == 5.0
    assert len(sent) == 1
    assert sent[0][0] == "dlq.depth_threshold_exceeded"
    assert sent[0][1]["unresolved_depth"] == 5
    assert sent[0][1]["threshold"] == 3
    assert get_metrics()["dlq_writer.depth_alerts_sent"] == 1.0
    assert new_wm == 1000.0  # cooldown watermark advanced


async def test_poll_quiet_under_threshold(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_metrics()
    sent: list = []
    monkeypatch.setattr(
        mod, "send_ops_alert",
        lambda event, payload: sent.append(1) or True,
    )

    tid = await _seed_tenant(fresh_db)
    await _seed_failure(fresh_db, tenant_id=tid)

    config = DLQWriterConfig(depth_alert_threshold=10)
    new_wm = await poll_dlq_depth(
        config, fresh_db,
        last_alert_monotonic=float("-inf"), now_monotonic=500.0,
    )

    assert get_metrics()["dlq_writer.unresolved_depth"] == 1.0
    assert sent == []                   # under threshold → no alert
    assert new_wm == float("-inf")      # watermark not advanced


async def test_poll_debounces_within_cooldown(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_metrics()
    sent: list = []

    async def _capture(event, payload):
        sent.append(1)
        return True

    monkeypatch.setattr(mod, "send_ops_alert", _capture)

    tid = await _seed_tenant(fresh_db)
    for _ in range(4):
        await _seed_failure(fresh_db, tenant_id=tid)

    config = DLQWriterConfig(depth_alert_threshold=2, depth_alert_cooldown_sec=3600.0)

    # First poll alerts and sets the watermark at t=1000.
    wm = await poll_dlq_depth(
        config, fresh_db,
        last_alert_monotonic=float("-inf"), now_monotonic=1000.0,
    )
    # Second poll only 100s later — within the 3600s cooldown → quiet.
    wm = await poll_dlq_depth(
        config, fresh_db, last_alert_monotonic=wm, now_monotonic=1100.0,
    )
    # Third poll past the cooldown → alerts again.
    wm = await poll_dlq_depth(
        config, fresh_db, last_alert_monotonic=wm, now_monotonic=5000.0,
    )

    assert len(sent) == 2  # first + third, not the debounced second
