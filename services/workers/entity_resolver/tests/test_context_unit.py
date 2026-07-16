from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.workers.entity_resolver.context import _load_context_candidates


class _CapturingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append((sql, args))
        return []


@pytest.mark.asyncio
async def test_slack_context_is_scoped_to_actual_channel_and_cutoff() -> None:
    conn = _CapturingConnection()
    tenant_id = uuid4()
    observation_id = uuid4()
    cutoff = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)

    await _load_context_candidates(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        observation_id=observation_id,
        source_channel="slack:message",
        source_space="C-finance",
        source_content={"channel": "C-finance", "ts": "100.1", "thread_ts": "99.1"},
        occurred_at=cutoff,
        limit=20,
    )

    assert len(conn.calls) == 2
    temporal_sql, temporal_args = conn.calls[0]
    structural_sql, structural_args = conn.calls[1]
    assert "content ->> 'channel' = $5" in temporal_sql
    assert "occurred_at <= $3" in temporal_sql
    assert temporal_args[:5] == (
        tenant_id,
        "slack:message",
        cutoff,
        observation_id,
        "C-finance",
    )
    assert "content ->> 'thread_ts' = $6" in structural_sql
    assert structural_args[5:7] == ("99.1", "100.1")


@pytest.mark.asyncio
async def test_self_contained_source_does_not_receive_slack_channel_filter() -> None:
    conn = _CapturingConnection()
    await _load_context_candidates(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        observation_id=uuid4(),
        source_channel="jira:issue",
        source_space="PROJECT-X",
        source_content={"project_id": "PROJECT-X"},
        occurred_at=datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc),
        limit=20,
    )

    assert len(conn.calls) == 1
    sql, args = conn.calls[0]
    assert "content ->> 'channel'" not in sql
    assert len(args) == 5
